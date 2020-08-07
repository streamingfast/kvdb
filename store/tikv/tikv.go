package tikv

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/dfuse-io/kvdb/store"
	"github.com/dfuse-io/logging"
	"github.com/tikv/client-go/config"
	"github.com/tikv/client-go/key"
	"github.com/tikv/client-go/rawkv"
	"go.uber.org/zap"
)

const emptyValueByte = byte(0x00)

var emptyStartKey = []byte{0x00}

type Store struct {
	dsn          string
	client       *rawkv.Client
	clientConfig config.Config
	keyPrefix    []byte

	batchPut *store.BachOp

	// TIKV does not support empty values, if this flag is set
	// tikv will prepend an empty byte on write and remove the first byte
	// on read to ensure that no empty value is written to the db
	emptyValuePossible bool
}

func (s *Store) String() string {
	return fmt.Sprintf("net kv store with dsn: %q", s.dsn)
}

func init() {
	store.Register(&store.Registration{
		Name:        "tikv",
		FactoryFunc: NewStore,
	})
}

// NewStore supports tikv://pd0,pd1,pd2:2379?prefix=hexkeyprefix
func NewStore(dsnString string) (store.KVStore, error) {
	dsn, err := url.Parse(dsnString)
	if err != nil {
		return nil, err
	}

	chunks := strings.Split(dsn.Host, ":")
	var hosts []string
	for _, h := range strings.Split(chunks[0], ",") {
		hosts = append(hosts, fmt.Sprintf("%s:%s", h, chunks[1]))
	}

	rawConfig := config.Default()
	client, err := rawkv.NewClient(context.Background(), hosts, rawConfig)
	if err != nil {
		return nil, err
	}

	s := &Store{
		dsn:          dsnString,
		client:       client,
		clientConfig: rawConfig,
		batchPut:     store.NewBatchOp(70000000, 0, 0),
	}

	keyPrefix := strings.Trim(dsn.Path, "/") + ";"
	if len(keyPrefix) < 4 {
		return nil, fmt.Errorf("table prefix needs to be more than 3 characters")
	}

	s.keyPrefix = []byte(keyPrefix)

	return s, nil
}

func (s *Store) Close() error {
	return s.client.Close()
}

func (s *Store) Put(ctx context.Context, key, value []byte) (err error) {
	s.batchPut.Op(s.withPrefix(key), s.formatValue(value))
	if s.batchPut.ShouldFlush() {
		return s.FlushPuts(ctx)
	}

	return nil
}

func (s *Store) FlushPuts(ctx context.Context) error {
	kvs := s.batchPut.GetBatch()
	if len(kvs) == 0 {
		return nil
	}

	keys := make([][]byte, len(kvs))
	values := make([][]byte, len(kvs))
	for idx, kv := range kvs {
		// The key & value must not be prefixed/formatted here as they already been processed when added to the batch directly
		keys[idx] = kv.Key
		values[idx] = kv.Value
	}

	err := s.client.BatchPut(ctx, keys, values)
	if err != nil {
		return err
	}

	s.batchPut.Reset()
	return nil
}

func (s *Store) Get(ctx context.Context, key []byte) ([]byte, error) {
	val, err := s.client.Get(ctx, s.withPrefix(key))
	if err != nil {
		return nil, err
	}

	val = s.unformatValue(val)

	// We need to check after unformatting because the value might have changed
	if val == nil {
		return nil, store.ErrNotFound
	}

	return val, nil
}

func (s *Store) BatchGet(ctx context.Context, keys [][]byte) *store.Iterator {
	kr := store.NewIterator(ctx)

	go func() {
		for _, key := range keys {
			val, err := s.client.Get(ctx, s.withPrefix(key))
			if err != nil {
				kr.PushError(err)
				return
			}

			// The key must **not** be unprefixed here because it's the one from the loop which is already unprefixed
			if !kr.PushItem(store.KV{key, s.unformatValue(val)}) {
				break
			}
		}
		kr.PushFinished()
	}()
	return kr
}

func (s *Store) BatchDelete(ctx context.Context, keys [][]byte) error {
	return s.client.BatchDelete(ctx, keys)
}

func (s *Store) Scan(ctx context.Context, start, exclusiveEnd []byte, limit int) *store.Iterator {
	zlogger := logging.Logger(ctx, zlog)
	startKey := s.withPrefix(start)
	endKey := s.withPrefix(exclusiveEnd)

	if store.Limit(limit).Unbounded() {
		limit = s.clientConfig.Raw.MaxScanLimit
	}

	sit := store.NewIterator(ctx)
	zlogger.Debug("scanning", zap.Stringer("start", store.Key(startKey)), zap.Stringer("exclusive_end", store.Key(endKey)), zap.Stringer("limit", store.Limit(limit)))
	go func() {
		keys, values, err := s.client.Scan(ctx, startKey, endKey, limit)
		if err != nil {
			sit.PushError(err)
			return
		}
		for idx, key := range keys {
			if !sit.PushItem(store.KV{s.withoutPrefix(key), s.unformatValue(values[idx])}) {
				break
			}
		}
		sit.PushFinished()
	}()

	return sit
}

func (s *Store) Prefix(ctx context.Context, prefix []byte, limit int) *store.Iterator {
	zlogger := logging.Logger(ctx, zlog)
	sit := store.NewIterator(ctx)
	zlogger.Debug("prefix scanning", zap.Stringer("prefix", store.Key(prefix)), zap.Stringer("limit", store.Limit(limit)))

	startKey := s.withPrefix(prefix)
	exclusiveEnd := key.Key(startKey).PrefixNext()
	sliceSize := 100
	if store.Limit(limit).Bounded() && limit < sliceSize {
		sliceSize = limit
	}

	// Can only happen if the actual prefix is empty (which is not permitted outside package), so no need to prefix the empty start key
	if len(startKey) == 0 {
		startKey = emptyStartKey
		exclusiveEnd = nil
	}

	go func() {
		count := uint64(0)

	outmost:
		for {
			keys, values, err := s.client.Scan(ctx, startKey, exclusiveEnd, sliceSize)
			if err != nil {
				sit.PushError(err)
				return
			}

			for idx, k := range keys {
				count++

				if !sit.PushItem(store.KV{s.withoutPrefix(k), s.unformatValue(values[idx])}) {
					break outmost
				}

				if store.Limit(limit).Reached(count) {
					break outmost
				}
			}

			if len(keys) > 0 {
				startKey = key.Key(keys[len(keys)-1]).Next()
			}

			if len(keys) < sliceSize {
				break
			}
		}
		sit.PushFinished()
	}()

	return sit
}

func (s *Store) BatchPrefix(ctx context.Context, prefixes [][]byte, limit int) *store.Iterator {
	zlogger := logging.Logger(ctx, zlog)
	sit := store.NewIterator(ctx)
	zlogger.Debug("batch prefix scanning", zap.Int("prefix_count", len(prefixes)), zap.Stringer("limit", store.Limit(limit)))

	sliceSize := 100
	if store.Limit(limit).Bounded() && limit < sliceSize {
		sliceSize = limit
	}

	// TODO: The native tikv client does not support batch scanning of multiple ranges of
	//       keys. While it's possible starting multiple goroutines that scans multiple
	//       ranges in parallel, this breaks the expected semantics of `BatchPrefix` that
	//       you receive the set of keys for first prefix, than for second and forward.
	//
	//       Using dhammer here for example could be possible to linearize results respecting
	//       order of received prefixes, but at the expense of storing more data in memory
	//       until the ordered keys are ready.
	//
	//       Another possibility would be to accept an option that would tell us that order
	//       does not matter and that caller is ok receiving keys in any order. It think this
	//       would be the best option for TiKV.
	go func() {
		count := uint64(0)

	outmost:
		for _, prefix := range prefixes {
			startKey := s.withPrefix(prefix)
			exclusiveEnd := key.Key(startKey).PrefixNext()

			// Can only happen if the actual prefix is empty (which is not permitted outside package), so no need to prefix the empty start key
			if len(startKey) == 0 {
				startKey = emptyStartKey
				exclusiveEnd = nil
			}

			for {
				keys, values, err := s.client.Scan(ctx, startKey, exclusiveEnd, sliceSize)
				if err != nil {
					sit.PushError(err)
					return
				}

				for idx, k := range keys {
					count++

					if !sit.PushItem(store.KV{Key: s.withoutPrefix(k), Value: s.unformatValue(values[idx])}) {
						break outmost
					}

					if store.Limit(limit).Reached(count) {
						break outmost
					}
				}

				if len(keys) > 0 {
					startKey = key.Key(keys[len(keys)-1]).Next()
				}

				if len(keys) < sliceSize {
					break
				}
			}
		}

		sit.PushFinished()
	}()

	return sit
}

func (s *Store) withPrefix(key []byte) []byte {
	if len(s.keyPrefix) == 0 {
		return key
	}
	out := make([]byte, len(s.keyPrefix)+len(key))
	copy(out[0:], s.keyPrefix)
	copy(out[len(s.keyPrefix):], key)
	return out
}

func (s *Store) withoutPrefix(key []byte) []byte {
	if len(s.keyPrefix) == 0 {
		return key
	}
	return key[len(s.keyPrefix):]
}

func (s *Store) formatValue(v []byte) []byte {
	if s.emptyValuePossible {
		return append(v, emptyValueByte)
	}

	return v
}

func (s *Store) unformatValue(v []byte) []byte {
	byteCount := len(v)
	if s.emptyValuePossible && byteCount >= 1 {
		return v[0 : byteCount-1]
	}
	return v
}
