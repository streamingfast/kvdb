package tikv

import (
	"context"
	"fmt"
	"net/url"
	"strings"

	"github.com/dfuse-io/kvdb/store"
	"github.com/tikv/client-go/config"
	"github.com/tikv/client-go/key"
	"github.com/tikv/client-go/rawkv"
	"go.uber.org/zap"
)

type Store struct {
	client       *rawkv.Client
	clientConfig config.Config
	keyPrefix    []byte

	batchPut *store.BatchPut
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
		client:       client,
		clientConfig: rawConfig,
		batchPut:     store.NewBatchPut(70000000, 0, 0),
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
	s.batchPut.Put(s.withPrefix(key), value)
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
		k := kv.Key
		keys[idx] = k
		values[idx] = kv.Value
	}
	err := s.client.BatchPut(ctx, keys, values)
	if err != nil {
		return err
	}
	s.batchPut.Reset()
	return nil
}

func (s *Store) Get(ctx context.Context, key []byte) (value []byte, err error) {
	val, err := s.client.Get(ctx, s.withPrefix(key))
	if err != nil {
		return nil, err
	}
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

			if !kr.PushItem(store.KV{key, val}) {
				break
			}
		}
		kr.PushFinished()
	}()
	return kr
}

func (s *Store) Scan(ctx context.Context, start, exclusiveEnd []byte, limit int) *store.Iterator {
	startKey := s.withPrefix(start)
	endKey := s.withPrefix(exclusiveEnd)

	if store.Limit(limit).Unbounded() {
		limit = s.clientConfig.Raw.MaxScanLimit
	}

	sit := store.NewIterator(ctx)
	zlog.Debug("scanning", zap.Stringer("start", store.Key(startKey)), zap.Stringer("exclusive_end", store.Key(endKey)), zap.Stringer("limit", store.Limit(limit)))
	go func() {
		keys, values, err := s.client.Scan(ctx, startKey, endKey, limit)
		if err != nil {
			sit.PushError(err)
			return
		}
		for idx, key := range keys {
			if !sit.PushItem(store.KV{s.withoutPrefix(key), values[idx]}) {
				break
			}
		}
		sit.PushFinished()
	}()

	return sit
}

func (s *Store) Prefix(ctx context.Context, prefix []byte, limit int) *store.Iterator {
	sit := store.NewIterator(ctx)
	zlog.Debug("prefix scanning", zap.Stringer("prefix", store.Key(prefix)), zap.Stringer("limit", store.Limit(limit)))

	startKey := s.withPrefix(prefix)
	exclusiveEnd := key.Key(startKey).PrefixNext()
	sliceSize := 100
	if store.Limit(limit).Bounded() && limit < sliceSize {
		sliceSize = limit
	}

	if len(startKey) == 0 {
		startKey = []byte{0x00}
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

				if !sit.PushItem(store.KV{s.withoutPrefix(k), values[idx]}) {
					break outmost
				}

				if store.Limit(limit).Reached(count) {
					break outmost
				}

				startKey = key.Key(k).Next()
			}

			if len(keys) < sliceSize {
				break
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
