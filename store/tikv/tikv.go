package tikv

import (
	"context"
	"encoding/hex"
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
	compressor   store.Compressor

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

	compressor, err := store.NewCompressor(dsn.Query().Get("compression"))
	if err != nil {
		return nil, err
	}

	s := &Store{
		client:       client,
		clientConfig: rawConfig,
		batchPut:     store.NewBatchPut(70000000, 0, 0),
		compressor:   compressor,
	}

	keyPrefix := dsn.Query().Get("keyPrefix")
	if keyPrefix != "" {
		keyPrefixBytes, err := hex.DecodeString(keyPrefix)
		if err != nil {
			return nil, fmt.Errorf("decoding keyPrefix as hex: %s", err)
		}
		s.keyPrefix = keyPrefixBytes
	}

	return s, nil
}

func (s *Store) Close() error {
	return s.client.Close()
}

func (s *Store) Put(ctx context.Context, key, value []byte) (err error) {
	//zlog.Debug("putting", zap.String("key", hex.EncodeToString(key)))
	s.batchPut.Put(s.withPrefix(key), s.compressor.Compress(value))
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
	val, err = s.compressor.Decompress(val)
	if err != nil {
		return nil, err
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

			val, err = s.compressor.Decompress(val)
			if err != nil {
				kr.PushError(err)
				return
			}

			kr.PushItem(&store.KV{key, val})
		}
		kr.PushFinished()
	}()
	return kr
}

func (s *Store) Scan(ctx context.Context, start, exclusiveEnd []byte, limit int) *store.Iterator {
	startKey := s.withPrefix(start)
	endKey := s.withPrefix(exclusiveEnd)

	if limit == 0 {
		limit = s.clientConfig.Raw.MaxScanLimit
	}

	sit := store.NewIterator(ctx)
	zlog.Debug("scanning", zap.String("start", hex.EncodeToString(startKey)), zap.String("exclusive_end", hex.EncodeToString(endKey)), zap.Int("limit", limit))
	go func() {
		keys, values, err := s.client.Scan(ctx, startKey, endKey, limit)
		if err != nil {
			sit.PushError(err)
			return
		}
		for idx, key := range keys {
			val, err := s.compressor.Decompress(values[idx])
			if err != nil {
				sit.PushError(err)
				return
			}

			sit.PushItem(&store.KV{s.withoutPrefix(key), val})
		}
		sit.PushFinished()
	}()

	return sit
}

func (s *Store) Prefix(ctx context.Context, prefix []byte) *store.Iterator {
	sit := store.NewIterator(ctx)
	zlog.Debug("prefix scanning ", zap.String("prefix", hex.EncodeToString(prefix)))

	startKey := s.withPrefix(prefix)
	exclusiveEnd := key.Key(startKey).PrefixNext()
	sliceSize := 10

	if len(startKey) == 0 {
		startKey = []byte{0x00}
		exclusiveEnd = nil
	}

	go func() {
		for {
			keys, values, err := s.client.Scan(ctx, startKey, exclusiveEnd, sliceSize)
			if err != nil {
				sit.PushError(err)
				return
			}

			for idx, k := range keys {
				val, err := s.compressor.Decompress(values[idx])
				if err != nil {
					sit.PushError(err)
					return
				}

				sit.PushItem(&store.KV{s.withoutPrefix(k), val})

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
