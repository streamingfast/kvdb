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
	dsn        string
	client     *rawkv.Client
	keyPrefix  []byte
	compressor store.Compressor

	batchPut *store.BatchOp

	maxScanSizeLimit uint64

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

	keyPrefix := strings.Trim(dsn.Path, "/") + ";"
	if len(keyPrefix) < 4 {
		return nil, fmt.Errorf("table prefix needs to be more than 3 characters")
	}

	dsnQuery := store.DSNQuery(dsn.Query())
	var rawValue string

	clientConfig := config.Default()
	clientConfig.Raw.MaxScanLimit, rawValue, err = dsnQuery.IntOption("tikv_raw_max_scan_limit", clientConfig.Raw.MaxScanLimit)
	if err != nil {
		return nil, fmt.Errorf("TiKV raw max scan limit option %q is not a valid number: %w", rawValue, err)
	}

	clientConfig.Raw.MaxBatchPutSize, rawValue, err = dsnQuery.IntOption("tikv_raw_max_batch_put_size", clientConfig.Raw.MaxBatchPutSize)
	if err != nil {
		return nil, fmt.Errorf("TiKV raw max batch put size option %q is not a valid number: %w", rawValue, err)
	}

	clientConfig.Raw.BatchPairCount, rawValue, err = dsnQuery.IntOption("tikv_raw_batch_pair_count", clientConfig.Raw.BatchPairCount)
	if err != nil {
		return nil, fmt.Errorf("TiKV raw batch pair count option %q is not a valid number: %w", rawValue, err)
	}

	compression, _ := dsnQuery.StringOption("compression", "")

	// Use compression size threshold (in bytes) if present, otherwise use ~512KiB
	compressionThreshold, rawValue, err := dsnQuery.IntOption("compression_size_threshold", 512*1024)
	if err != nil {
		return nil, fmt.Errorf("compression size threshold option %q is not a valid number: %w", rawValue, err)
	}

	compressor, err := store.NewCompressor(compression, compressionThreshold)
	if err != nil {
		return nil, fmt.Errorf("new compressor: %w", err)
	}

	// Use batch size threshold (in bytes) if present, otherwise use ~7MiB
	batchSizeThreshold, rawValue, err := dsnQuery.IntOption("batch_size_threshold", 7*1024*1024)
	if err != nil {
		return nil, fmt.Errorf("batch size threshold option %q is not a valid number: %w", rawValue, err)
	}

	// Use batch ops threshold if present, otherwise use 0 (unlimited)
	batchOpsThreshold, rawValue, err := dsnQuery.IntOption("batch_ops_threshold", 0)
	if err != nil {
		return nil, fmt.Errorf("batch ops threshold option %q is not a valid number: %w", rawValue, err)
	}

	// Use batch time threshold if present, otherwise use 0 (unlimited)
	batchTimeThreshold, rawValue, err := dsnQuery.DurationOption("batch_time_threshold", 0)
	if err != nil {
		return nil, fmt.Errorf("batch time threshold option %q is not a valid duration: %w", rawValue, err)
	}

	batcher := store.NewBatchOp(batchSizeThreshold, batchOpsThreshold, batchTimeThreshold)

	zlog.Info("creating store instance",
		zap.String("dsn", dsnString),
		zap.String("key_prefix", keyPrefix),
		zap.Object("compressor", compressor),
		zap.Object("tikv_raw_config", tikvConfigRaw(clientConfig.Raw)),
		zap.Object("batcher", batcher),
	)

	client, err := rawkv.NewClient(context.Background(), hosts, clientConfig)
	if err != nil {
		return nil, err
	}

	s := &Store{
		dsn:              dsnString,
		client:           client,
		batchPut:         batcher,
		compressor:       compressor,
		keyPrefix:        []byte(keyPrefix),
		maxScanSizeLimit: uint64(clientConfig.Raw.MaxScanLimit),
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
	zlogger := logging.Logger(ctx, zlog)
	zlogger.Debug("batch get", zap.Int("key_count", len(keys)))

	prefixedKeys := make([][]byte, len(keys))
	for i, key := range keys {
		prefixedKeys[i] = s.withPrefix(key)
	}

	kr := store.NewIterator(ctx)
	go func() {
		rawValues, err := s.client.BatchGet(ctx, prefixedKeys)
		if err != nil {
			kr.PushError(err)
			return
		}

		if len(rawValues) != len(keys) {
			kr.PushError(fmt.Errorf("no enough values received from cluster, have %d keys but got only %d values", len(keys), len(rawValues)))
		}

		for i, rawValue := range rawValues {
			// The key must **not** be unprefixed here because it's the one from the loop which is already unprefixed
			key := keys[i]
			value, err := s.unformatValue(rawValue)
			if err != nil {
				kr.PushError(fmt.Errorf("unformat value of %x: %w", key, err))
				return
			}

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

func (s *Store) Scan(ctx context.Context, start, exclusiveEnd []byte, limit int, options ...store.ReadOption) *store.Iterator {
	zlogger := logging.Logger(ctx, zlog)
	zlogger.Debug("range scan",
		zap.Stringer("start_key", store.Key(start)),
		zap.Stringer("exclusive_end_key", store.Key(exclusiveEnd)),
	)

	return s.scanIterator(ctx, zlogger, s.withPrefix(start), s.withPrefix(exclusiveEnd), store.Limit(limit), options)
}

func (s *Store) Prefix(ctx context.Context, prefix []byte, limit int, options ...store.ReadOption) *store.Iterator {
	zlogger := logging.Logger(ctx, zlog)
	zlogger.Debug("prefix scanning", zap.Stringer("prefix", store.Key(prefix)), zap.Stringer("limit", store.Limit(limit)))

	startKey := s.withPrefix(prefix)
	exclusiveEnd := key.Key(startKey).PrefixNext()

	// This performs a full scan, can only happen if the actual prefix is empty, which is permitted only in package's tests
	if len(startKey) == 0 {
		startKey = emptyStartKey
		exclusiveEnd = nil
	}

	return s.scanIterator(ctx, zlogger, startKey, exclusiveEnd, store.Limit(limit), options)
}

func (s *Store) BatchPrefix(ctx context.Context, prefixes [][]byte, limit int, options ...store.ReadOption) *store.Iterator {
	zlogger := logging.Logger(ctx, zlog)
	zlogger.Debug("batch prefix", zap.Int("prefix_count", len(prefixes)), zap.Stringer("limit", store.Limit(limit)))

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
	it := store.NewIterator(ctx)
	go func() {
		count := uint64(0)
		limit := store.Limit(limit)

		for _, prefix := range prefixes {
			startKey := s.withPrefix(prefix)
			exclusiveEnd := key.Key(startKey).PrefixNext()

			// This performs a full scan, can only happen if the actual prefix is empty, which is permitted only in package's tests
			if len(startKey) == 0 {
				startKey = emptyStartKey
				exclusiveEnd = nil
			}

			scanLimit := limit
			if limit.Bounded() {
				scanLimit = store.Limit(uint64(limit) - count)
			}

			shouldContinue := true
			err := s.scan(ctx, zlogger, startKey, exclusiveEnd, scanLimit, options, func(kv store.KV) bool {
				if !it.PushItem(kv) {
					return false
				}

				count++
				if limit.Bounded() && limit.Reached(count) {
					shouldContinue = false
					return false
				}

				return true
			})

			if err != nil {
				it.PushError(err)
				return
			}

			if !shouldContinue {
				break
			}
		}

		it.PushFinished()
	}()

	return it
}

func (s *Store) scanIterator(ctx context.Context, zlogger *zap.Logger, startKey, exclusiveEnd []byte, limit store.Limit, options []store.ReadOption) *store.Iterator {
	it := store.NewIterator(ctx)
	go func() {
		err := s.scan(ctx, zlogger, startKey, exclusiveEnd, limit, options, func(kv store.KV) bool {
			if !it.PushItem(kv) {
				return false
			}

			return true
		})

		if err != nil {
			it.PushError(err)
			return
		}

		it.PushFinished()
	}()
	return it
}

func (s *Store) scan(ctx context.Context, zlogger *zap.Logger, startKey, exclusiveEnd []byte, limit store.Limit, options []store.ReadOption, onKV func(kv store.KV) bool) (err error) {
	scanOption := tikvScanOption(options)

	zlogger.Debug("scanning",
		zap.Stringer("start_key", store.Key(startKey)),
		zap.Stringer("exclusive_end_key", store.Key(exclusiveEnd)),
		zap.Bool("key_only", scanOption.KeyOnly),
		zap.Stringer("limit", store.Limit(limit)),
	)

	count := uint64(0)

	for {
		sliceSize := uint64(s.maxScanSizeLimit)
		if limit.Bounded() {
			missingCount := uint64(limit) - count
			if missingCount < sliceSize {
				sliceSize = missingCount
			}
		}

		keys, values, err := s.client.Scan(ctx, startKey, exclusiveEnd, int(sliceSize), scanOption)
		if err != nil {
			return err
		}

		for i, key := range keys {
			count++
			value, err := s.unformatValue(values[i])
			if err != nil {
				return fmt.Errorf("unformat value for key %x: %w", key, err)
			}

			shouldContinue := onKV(store.KV{Key: s.withoutPrefix(key), Value: value})
			if !shouldContinue {
				return nil
			}

			if store.Limit(limit).Reached(count) {
				return nil
			}
		}

		if uint64(len(keys)) < sliceSize {
			return nil
		}

		if len(keys) > 0 {
			startKey = key.Key(keys[len(keys)-1]).Next()
		}
	}
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

var defaultScanOption = rawkv.DefaultScanOption()

func tikvScanOption(options []store.ReadOption) rawkv.ScanOption {
	if len(options) == 0 {
		return defaultScanOption
	}

	readOptions := store.ReadOptions{}
	for _, opt := range options {
		opt.Apply(&readOptions)
	}

	return rawkv.ScanOption{
		KeyOnly: readOptions.KeyOnly,
	}
}
