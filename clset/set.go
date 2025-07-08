package clset

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"strings"
	"sync"

	pb "github.com/ipfs/go-ds-crdt/pb"
	ipld "github.com/ipfs/go-ipld-format"
	logging "github.com/ipfs/go-log/v2"
	multierr "go.uber.org/multierr"

	ds "github.com/ipfs/go-datastore"
	query "github.com/ipfs/go-datastore/query"
)

var (
	elemsNs = "s" // /elements namespace /set/c/<key>
)

// Set implements an Add-Wins Observed-Remove Set using delta-CRDTs
// (https://arxiv.org/abs/1410.2803) and backing all the data in a
// go-datastore. It is fully agnostic to MerkleCRDTs or the delta distribution
// layer.  It chooses the Value with most priority for a Key as the current
// Value. When two values have the same priority, it chooses by alphabetically
// sorting their unique IDs alphabetically.
type Set struct {
	Store      ds.Datastore
	namespace  ds.Key
	putHook    func(key string, v []byte)
	deleteHook func(key string)
	logger     logging.StandardLogger

	// Avoid merging two things at the same time since
	// we read-write value-priorities in a non-atomic way.
	putElemsMux sync.Mutex
}

func New(
	ctx context.Context,
	d ds.Datastore,
	namespace ds.Key,
	dagService ipld.DAGService,
	logger logging.StandardLogger,
	putHook func(key string, v []byte),
	deleteHook func(key string),
) (*Set, error) {

	set := &Set{
		namespace:  namespace,
		Store:      d,
		logger:     logger,
		putHook:    putHook,
		deleteHook: deleteHook,
	}

	return set, nil
}

// Add returns a new delta-set adding the given key/value.
func (s *Set) Add(ctx context.Context, key string, value []byte) (*pb.CLSetDelta, error) {
	elemKey := s.ElemsKey(key)
	existingElement, err := s.Store.Get(ctx, elemKey)
	if err != nil && !errors.Is(err, ds.ErrNotFound) {
		return nil, err
	}
	if errors.Is(err, ds.ErrNotFound) {
		return &pb.CLSetDelta{
			Elements: []*pb.CLSetElement{
				{
					Key:   key,
					Cl:    1,
					Value: value,
				},
			},
		}, nil
	}

	_, cl, _, err := decodeValue(existingElement)
	if err != nil {
		return nil, err
	}

	// Even causal length: element was removed, so reinsert it.

	if cl%2 == 0 {
		return &pb.CLSetDelta{
			Elements: []*pb.CLSetElement{
				{
					Key:   key,
					Cl:    cl + 1,
					Value: value,
				},
			},
		}, nil
	}

	// Odd causal length: element already exists, so update it without changing its causal length.

	return &pb.CLSetDelta{
		Elements: []*pb.CLSetElement{
			{
				Key:   key,
				Cl:    cl,
				Value: value,
			},
		},
	}, nil
}

// Rmv returns a new delta-set removing the given key.
func (s *Set) Rmv(ctx context.Context, key string) (*pb.CLSetDelta, error) {
	elemKey := s.ElemsKey(key)
	existingElement, err := s.Store.Get(ctx, elemKey)
	if errors.Is(err, ds.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	_, cl, _, err := decodeValue(existingElement)
	if err != nil {
		return nil, err
	}
	// Odd causal length: element exists, so remove it.
	if cl%2 == 1 {
		return &pb.CLSetDelta{
			Elements: []*pb.CLSetElement{
				{
					Key:   key,
					Cl:    cl + 1,
					Value: nil,
				},
			},
		}, nil
	}

	// Even causal length: element was already removed, so no need to do anything.
	return nil, nil
}

// Element retrieves the value of an element from the CRDT set.
func (s *Set) Element(ctx context.Context, key string) ([]byte, error) {
	// We can only GET an element if it's part of the Set (in
	// "elements" and not in "tombstones").

	// * If the key has a value in the store it means that it has been
	//   written and is alive. putTombs will delete the value if all elems
	//   are tombstoned, or leave the best one.

	elemK := s.ElemsKey(key)

	value, err := s.Store.Get(ctx, elemK)

	if err != nil { // not found is fine, we just return it
		return value, err
	}

	_, cl, v, err := decodeValue(value)
	if err != nil { // not found is fine, we just return it
		return value, err
	}

	if cl%2 == 0 {
		return nil, ds.ErrNotFound
	}

	return v, nil
}

// Elements returns all the elements in the set.
func (s *Set) Elements(ctx context.Context, q query.Query) (query.Results, error) {
	elemsNamespacePrefix := s.KeyPrefix(elemsNs)
	elemsNamespacePrefixStr := elemsNamespacePrefix.String()

	// We are going to be reading everything in the /set/ namespace which
	// will return items in the form:
	// * /set/<key>/value
	// * /set<key>/priority (a Uvarint)

	// It is clear that KeysOnly=true should be used here when the original
	// query only wants keys.
	//
	// However, there is a question of what is best when the original
	// query wants also values:
	// * KeysOnly: true avoids reading all the priority key values
	//   which are skipped at the cost of doing a separate Get() for the
	//   values (50% of the keys).
	// * KeysOnly: false reads everything from the start. Priorities
	//   and tombstoned values are read for nothing
	//
	// In-mem benchmarking shows no clear winner. Badger docs say that
	// KeysOnly "is several order of magnitudes faster than regular
	// iteration". Contrary to my original feeling, however, live testing
	// with a 50GB badger with millions of keys shows more speed when
	// querying with value. It may be that speed is fully affected by the
	// current state of table compaction as well.
	setQuery := query.Query{
		Prefix:   elemsNamespacePrefixStr,
		KeysOnly: false,
	}

	// send the result and returns false if we must exit.
	sendResult := func(ctx, qctx context.Context, r query.Result, out chan<- query.Result) bool {
		select {
		case out <- r:
		case <-ctx.Done():
			return false
		case <-qctx.Done():
			return false
		}
		return r.Error == nil
	}

	// The code below was very inspired in the Query implementation in
	// flatfs.

	// Originally we were able to set the output channel capacity and it
	// was set to 128 even though not much difference to 1 could be
	// observed on mem-based testing.

	// Using KeysOnly still gives a 128-item channel.
	// See: https://github.com/ipfs/go-datastore/issues/40
	r := query.ResultsWithContext(q, func(qctx context.Context, out chan<- query.Result) {
		// qctx is a Background context for the query. It is not
		// associated to ctx. It is closed when this function finishes
		// along with the output channel, or when the Results are
		// Closed directly.
		results, err := s.Store.Query(ctx, setQuery)
		if err != nil {
			sendResult(ctx, qctx, query.Result{Error: err}, out)
			return
		}
		defer results.Close()

		var entry query.Entry
		for r := range results.Next() {
			if r.Error != nil {
				sendResult(ctx, qctx, query.Result{Error: r.Error}, out)
				return
			}

			key := strings.TrimPrefix(r.Key, elemsNamespacePrefixStr)

			entry.Key = key
			// decode the value
			_, cl, v, err := decodeValue(r.Value)
			if err != nil {
				sendResult(ctx, qctx, query.Result{Error: r.Error}, out)
			}
			if cl%2 == 0 {
				continue
			}
			entry.Value = v
			entry.Size = r.Size
			entry.Expiration = r.Expiration

			// The fact that /v is set means it is not tombstoned,
			// as tombstoning removes /v and /p or sets them to
			// the best value.

			if q.KeysOnly {
				entry.Size = -1
				entry.Value = nil
			}
			if !sendResult(ctx, qctx, query.Result{Entry: entry}, out) {
				return
			}
		}
	})

	return r, nil
}

// InSet returns true if the key belongs to one of the elements in the "elems"
// set, and this element is not tombstoned.
func (s *Set) InSet(ctx context.Context, key string) (bool, error) {
	// If we do not have a value this key was never added or it was fully
	// tombstoned.
	_, err := s.Element(ctx, key)
	if errors.Is(err, ds.ErrNotFound) {
		return false, nil
	}
	if err == nil {
		return true, nil
	}
	return false, err
}

// /namespace/<key>
func (s *Set) KeyPrefix(key string) ds.Key {
	return s.namespace.ChildString(key)
}

// /namespace/elems/<key>
func (s *Set) ElemsKey(key string) ds.Key {
	return s.KeyPrefix(elemsNs).ChildString(key)
}

// sets a value if priority is higher. When equal, it sets if the
// value is lexicographically higher than the current value.
func (s *Set) setValue(ctx context.Context, writeStore ds.Write, key string, value []byte, cl uint64, prio uint64) error {
	// Encode the candidate value.
	newEncoded := encodeValue(prio, cl, value)
	elemK := s.ElemsKey(key)
	curEncoded, err := s.Store.Get(ctx, elemK)
	if err != nil && err != ds.ErrNotFound {
		return err
	}
	if err == nil {
		curPrio, curCL, curVal, err := decodeValue(curEncoded)
		if err != nil {
			return err
		}
		// Only update if the new candidate has higher CL or,
		// when equal, higher priprity, or, when equal, a
		// exicographically greater value.
		if cl < curCL {
			return nil
		}
		if cl == curCL && prio < curPrio {
			return nil
		}
		if cl == curCL && prio == curPrio && bytes.Compare(curVal, value) >= 0 {
			return nil
		}
	}

	// Store the new “best” encoded value.
	if err = writeStore.Put(ctx, elemK, newEncoded); err != nil {
		return err
	}

	if cl%2 == 1 {
		// Trigger the add hook with the original (unencoded) value.
		s.putHook(key, value)
	} else {
		s.deleteHook(key)
	}
	return nil
}

// putElems adds items to the "elems" set. It will also set current
// values and priorities for each element. This needs to run in a lock,
// as otherwise races may occur when reading/writing the priorities, resulting
// in bad behaviours.
//
// Technically the lock should only affect the keys that are being written,
// but with the batching optimization the locks would need to be hold until
// the batch is written), and one lock per key might be way worse than a single
// global lock in the end.
func (s *Set) putElems(ctx context.Context, elems []*pb.CLSetElement, prio uint64) error {
	s.putElemsMux.Lock()
	defer s.putElemsMux.Unlock()

	if len(elems) == 0 {
		return nil
	}

	var store ds.Write = s.Store
	var err error
	if batchingDs, ok := store.(ds.Batching); ok {
		store, err = batchingDs.Batch(ctx)
		if err != nil {
			return err
		}
	}

	for _, e := range elems {
		key := e.GetKey()
		// Update the best value for this key if needed.
		if err := s.setValue(ctx, store, key, e.GetValue(), e.GetCl(), prio); err != nil {
			return err
		}
	}

	if batchingDs, ok := store.(ds.Batch); ok {
		if err := batchingDs.Commit(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (s *Set) Merge(ctx context.Context, d *pb.CLSetDelta, id string) error {
	return s.putElems(ctx, d.GetElements(), d.GetPriority())
}

// currently unused
// // inSet returns if the given cid/block is in elems and not in tombs (and
// // thus, it is an element of the set).
// func (s *set) inSetKeyID(key, id string) (bool, error) {
// 	inTombs, err := s.inTombsKeyID(key, id)
// 	if err != nil {
// 		return false, err
// 	}
// 	if inTombs {
// 		return false, nil
// 	}

// 	return s.inElemsKeyID(key, id)
// }

// perform a sync against all the paths associated with a key prefix
func (s *Set) DatastoreSync(ctx context.Context, prefix ds.Key) error {
	prefixStr := prefix.String()
	toSync := []ds.Key{
		s.ElemsKey(prefixStr),
	}

	errs := make([]error, len(toSync))

	for i, k := range toSync {
		if err := s.Store.Sync(ctx, k); err != nil {
			errs[i] = err
		}
	}

	return multierr.Combine(errs...)
}

func encodeValue(prio uint64, cl uint64, value []byte) []byte {
	buf := make([]byte, 8+8+len(value))
	binary.BigEndian.PutUint64(buf[:8], prio)
	binary.BigEndian.PutUint64(buf[8:16], cl)
	copy(buf[16:], value)
	return buf
}

func decodeValue(encoded []byte) (uint64, uint64, []byte, error) {
	if len(encoded) < 16 {
		return 0, 0, nil, errors.New("encoded value too short")
	}
	prio := binary.BigEndian.Uint64(encoded[:8])
	cl := binary.BigEndian.Uint64(encoded[8:16])
	return prio, cl, encoded[16:], nil
}
