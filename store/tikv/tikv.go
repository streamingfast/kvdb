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
	compressor   store.Compressor

	batchPut *store.BatchOp

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

	keyPrefix := strings.Trim(dsn.Path, "/") + ";"
	if len(keyPrefix) < 4 {
		return nil, fmt.Errorf("table prefix needs to be more than 3 characters")
	}

	dsnQuery := dsn.Query()
	compression := dsnQuery.Get("compression")

	// Use compression size threshold (in bytes) if present, otherwise use ~512KiB
	compressionThreshold, rawValue, err := store.AsIntOption(dsnQuery.Get("compression_size_threshold"), 512*1024)
	if err != nil {
		return nil, fmt.Errorf("compression size threshold option %q is not a valid number: %w", rawValue, err)
	}

	compressor, err := store.NewCompressor(compression, compressionThreshold)
	if err != nil {
		return nil, fmt.Errorf("new compressor: %w", err)
	}

	// Use batch size threshold (in bytes) if present, otherwise use ~7MiB
	batchSizeThreshold, rawValue, err := store.AsIntOption(dsnQuery.Get("batch_size_threshold"), 7*1024*1024)
	if err != nil {
		return nil, fmt.Errorf("batch size threshold option %q is not a valid number: %w", rawValue, err)
	}

	// Use batch ops threshold if present, otherwise use 0 (unlimited)
	batchOpsThreshold, rawValue, err := store.AsIntOption(dsnQuery.Get("batch_ops_threshold"), 0)
	if err != nil {
		return nil, fmt.Errorf("batch ops threshold option %q is not a valid number: %w", rawValue, err)
	}

	// Use batch time threshold if present, otherwise use 0 (unlimited)
	batchTimeThreshold, rawValue, err := store.AsDurationOption(dsnQuery.Get("batch_time_threshold"), 0)
	if err != nil {
		return nil, fmt.Errorf("batch time threshold option %q is not a valid duration: %w", rawValue, err)
	}

	batcher := store.NewBatchOp(batchSizeThreshold, batchOpsThreshold, batchTimeThreshold)

	zlog.Info("creating store instance",
		zap.String("dsn", dsnString),
		zap.String("key_prefix", keyPrefix),
		zap.Object("compressor", compressor),
		zap.Object("batcher", batcher),
	)
	s := &Store{
		dsn:          dsnString,
		client:       client,
		clientConfig: rawConfig,
		batchPut:     batcher,
		compressor:   compressor,
		keyPrefix:    []byte(keyPrefix),
	}

	return s, nil
}

func (s *Store) Close() error {
	return s.client.Close()
}

func (s *Store) Put(ctx context.Context, key, value []byte) (err error) {
	if len(value) == 0 && !s.emptyValuePossible {
		return fmt.Errorf("empty value not supported by this store, if you expect to need to store empty value, please use `store.WithEmptyValue()` when creating the store to enable them")
	}

	formattedKey := s.withPrefix(key)
	formattedValue := s.formatValue(value)

	if s.batchPut.WouldFlushNext(formattedKey, formattedValue) {
		err := s.FlushPuts(ctx)
		if err != nil {
			return err
		}
	}

	s.batchPut.Op(formattedKey, formattedValue)
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

	if traceEnabled {
		zlog.Debug("flush a batch through client", zap.Int("op_count", len(kvs)), zap.Int("size", s.batchPut.Size()))
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

	if traceEnabled {
		zlog.Debug("received raw value for get", zap.Stringer("key", store.Key(key)), zap.Stringer("value", store.Key(val)))
	}

	// Anything that is returned here will have at least one byte because at insertion time, the value either had more than one
	// or we added one ourself because of the `WithEmptyValue` option. So it's safe here to check that the value is `nil`.
	if val == nil {
		return nil, store.ErrNotFound
	}

	val, err = s.unformatValue(val)
	if err != nil {
		return nil, fmt.Errorf("unformat value: %w", err)
	}

	if traceEnabled {
		zlog.Debug("returning value for get", zap.Stringer("key", store.Key(key)), zap.Stringer("value", store.Key(val)))
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

			value, err := s.unformatValue(val)
			if err != nil {
				kr.PushError(fmt.Errorf("unformat value: %w", err))
				return
			}

			// The key must **not** be unprefixed here because it's the one from the loop which is already unprefixed
			if !kr.PushItem(store.KV{Key: key, Value: value}) {
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
			value, err := s.unformatValue(values[idx])
			if err != nil {
				sit.PushError(fmt.Errorf("unformat value: %w", err))
				return
			}

			if !sit.PushItem(store.KV{s.withoutPrefix(key), value}) {
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
				value, err := s.unformatValue(values[idx])
				if err != nil {
					sit.PushError(fmt.Errorf("unformat value: %w", err))
					return
				}

				if !sit.PushItem(store.KV{s.withoutPrefix(k), value}) {
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
					value, err := s.unformatValue(values[idx])
					if err != nil {
						sit.PushError(fmt.Errorf("unformat value: %w", err))
						return
					}

					if !sit.PushItem(store.KV{Key: s.withoutPrefix(k), Value: value}) {
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

func (s *Store) formatValue(v []byte) (out []byte) {
	out = v
	if s.emptyValuePossible {
		out = append(v, emptyValueByte)
	}

	return s.compressor.Compress(out)
}

func (s *Store) unformatValue(v []byte) (out []byte, err error) {
	v, err = s.compressor.Decompress(v)
	if err != nil {
		return v, fmt.Errorf("decompress value: %w", err)
	}

	byteCount := len(v)
	if s.emptyValuePossible && byteCount >= 1 {
		// We have a single byte and about to strip 1 byte, results in a nil output
		if byteCount == 1 {
			return nil, nil
		}

		return v[0 : byteCount-1], nil
	}

	return v, nil
}
