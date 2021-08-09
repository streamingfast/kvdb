package bigkv

import (
	"context"
	"encoding/hex"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/bigtable"
	"github.com/dfuse-io/logging"
	"github.com/streamingfast/kvdb/store"
	"go.uber.org/multierr"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Store struct {
	dsn    string
	client *bigtable.Client
	table  *bigtable.Table

	keyPrefix []byte
	tableName string

	columnName string

	maxBytesBeforeFlush   uint64
	maxRowsBeforeFlush    uint64
	maxSecondsBeforeFlush uint64

	batchPut *store.BatchOp
}

func (s *Store) String() string {
	return fmt.Sprintf("bigtable kv store with dsn: %q", s.dsn)
}

func init() {
	store.Register(&store.Registration{
		Name:        "bigkv",
		FactoryFunc: NewStore,
	})
}

// NewStore supports bigkt://project.instance/tableName?createTable=true
func NewStore(dsnString string) (store.KVStore, error) {
	dsn, err := url.Parse(dsnString)
	if err != nil {
		return nil, err
	}

	ctx := context.Background()

	projInstance := strings.Split(dsn.Host, ".")
	if len(projInstance) != 2 {
		return nil, fmt.Errorf("dsn %q invalid, ensure host component looks like 'project.instance'", dsnString)
	}

	project := projInstance[0]
	instance := projInstance[1]

	optionalTestEnv(project, instance)

	client, err := bigtable.NewClient(ctx, project, instance)
	if err != nil {
		return nil, err
	}

	// FIXME: Sadly, Bigkv and TiKV uses different parameters name for configuring the batch manager.
	//        I tend to see name here better than the ones in TiKV even though I think that starting with
	//        `batch-` would be preferable. We also have a naming convention issue, we use `camelCase` here
	//        but `snake-case` in TiKV. We should decide on our standard convention for parameter naming.
	maxBytesBeforeFlush := uint64(70000000)
	if qMaxBytes := dsn.Query().Get("maxBytesBeforeFlush"); qMaxBytes != "" {
		ms, err := strconv.ParseUint(qMaxBytes, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("dsn: invalid parameter for maxBytesBeforeFlush, %s", err)
		}
		maxBytesBeforeFlush = ms
	}

	maxSecondsBeforeFlush := uint64(10)
	if qMaxSeconds := dsn.Query().Get("maxSecondsBeforeFlush"); qMaxSeconds != "" {
		ms, err := strconv.ParseUint(qMaxSeconds, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("dsn: invalid parameter for maxSecondsBeforeFlush, %s", err)
		}
		maxSecondsBeforeFlush = ms
	}

	maxRowsBeforeFlush := uint64(0)
	if qMaxRows := dsn.Query().Get("maxRowsBeforeFlush"); qMaxRows != "" {
		mb, err := strconv.ParseUint(qMaxRows, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("dsn: invalid parameter for maxRowsBeforeFlush, %s", err)
		}
		maxRowsBeforeFlush = mb
	}

	s := &Store{
		dsn:                   dsnString,
		client:                client,
		batchPut:              store.NewBatchOp(int(maxBytesBeforeFlush), int(maxRowsBeforeFlush), time.Duration(maxSecondsBeforeFlush)*time.Second),
		maxBytesBeforeFlush:   maxBytesBeforeFlush,
		maxRowsBeforeFlush:    maxRowsBeforeFlush,
		maxSecondsBeforeFlush: maxSecondsBeforeFlush,
	}

	if keyPrefix := dsn.Query().Get("keyPrefix"); keyPrefix != "" {
		keyPrefixBytes, err := hex.DecodeString(keyPrefix)
		if err != nil {
			return nil, fmt.Errorf("decoding keyPrefix as hex: %w", err)
		}
		s.keyPrefix = keyPrefixBytes
	}

	s.columnName = "kv"
	if colName := dsn.Query().Get("colName"); colName != "" {
		s.columnName = colName
	}

	createTable := dsn.Query().Get("createTable") == "true"

	tableName := strings.Trim(dsn.Path, "/")
	s.table = client.Open(tableName)

	if createTable {
		adminClient, err := bigtable.NewAdminClient(ctx, project, instance)
		if err != nil {
			return nil, fmt.Errorf("failed setting up admin client: %w", err)
		}

		if err := adminClient.CreateTable(ctx, tableName); err != nil && !isAlreadyExistsError(err) {
			return nil, fmt.Errorf("failed creating table %q: %w", tableName, err)
		}

		if err := adminClient.CreateColumnFamily(ctx, tableName, s.columnName); err != nil && !isAlreadyExistsError(err) {
			return nil, fmt.Errorf("failed creating %q family for table %q: %w", s.columnName, tableName, err)
		}

		if err := adminClient.SetGCPolicy(ctx, tableName, s.columnName, bigtable.MaxVersionsPolicy(1)); err != nil {
			return nil, fmt.Errorf("failed applying gc policy to %q family for table %q: %w", s.columnName, tableName, err)
		}
	}

	return s, nil
}

func isAlreadyExistsError(err error) bool {
	st, ok := status.FromError(err)
	if !ok {
		return false
	}

	return st.Code() == codes.AlreadyExists
}

func (s *Store) Close() error {
	return s.client.Close()
}

func (s *Store) Put(ctx context.Context, key, value []byte) (err error) {
	formattedKey := s.withPrefix(key)
	if s.batchPut.WouldFlushNext(formattedKey, value) {
		err := s.FlushPuts(ctx)
		if err != nil {
			return err
		}
	}

	s.batchPut.Op(formattedKey, value)
	return nil
}

func (s *Store) FlushPuts(ctx context.Context) error {
	if traceEnabled {
		logging.Logger(ctx, zlog).Debug("flushing puts")
	}

	kvs := s.batchPut.GetBatch()
	if len(kvs) == 0 {
		return nil
	}

	keys := make([]string, len(kvs))
	values := make([]*bigtable.Mutation, len(kvs))
	for idx, kv := range kvs {
		keys[idx] = string(kv.Key)
		mut := bigtable.NewMutation()
		mut.Set(s.columnName, "v", bigtable.Now(), kv.Value)
		values[idx] = mut
	}
	errs, err := s.table.ApplyBulk(ctx, keys, values)
	if err != nil {
		return err
	}
	if len(errs) != 0 {
		return fmt.Errorf("apply bulk error: %w", multierr.Combine(errs...))
	}
	s.batchPut.Reset()
	return nil
}

func (s *Store) Get(ctx context.Context, key []byte) (value []byte, err error) {
	btOptions := bigtableReadOptions(store.Limit(store.Unlimited), nil)
	row, err := s.table.ReadRow(ctx, string(s.withPrefix(key)), btOptions...)
	if err != nil {
		return nil, err
	}
	if len(row) == 0 {
		return nil, store.ErrNotFound
	}

	return row[s.columnName][0].Value, nil
}

func (s *Store) BatchGet(ctx context.Context, keys [][]byte) *store.Iterator {
	if traceEnabled {
		logging.Logger(ctx, zlog).Debug("batch get", zap.Int("key_count", len(keys)))
	}

	btKeys := make([]string, len(keys))
	for i, key := range keys {
		btKeys[i] = string(key)
	}

	btOptions := bigtableReadOptions(store.Limit(store.Unlimited), nil)
	kr := store.NewIterator(ctx)
	go func() {
		err := s.table.ReadRows(ctx, bigtable.RowList(btKeys), func(row bigtable.Row) bool {
			return kr.PushItem(store.KV{Key: s.withoutPrefix([]byte(row.Key())), Value: row[s.columnName][0].Value})
		}, btOptions...)

		if err != nil {
			kr.PushError(err)
			return
		}

		kr.PushFinished()
	}()

	return kr
}

func (s *Store) BatchDelete(ctx context.Context, deletionKeys [][]byte) (err error) {
	if traceEnabled {
		logging.Logger(ctx, zlog).Debug("batch delete", zap.Int("key_count", len(deletionKeys)))
	}

	if len(deletionKeys) == 0 {
		return nil
	}

	batch := store.NewBatchOp(int(s.maxBytesBeforeFlush), int(s.maxRowsBeforeFlush), time.Duration(s.maxSecondsBeforeFlush)*time.Second)
	keys := make([]string, len(deletionKeys))
	values := make([]*bigtable.Mutation, len(deletionKeys))
	for idx, deletionKey := range deletionKeys {
		if batch.ShouldFlush() {
			errs, err := s.table.ApplyBulk(ctx, keys, values)
			if err != nil {
				return err
			}
			if len(errs) != 0 {
				return fmt.Errorf("apply bulk error: %w", multierr.Combine(errs...))
			}
			keys = make([]string, len(deletionKeys))
			values = make([]*bigtable.Mutation, len(deletionKeys))
		}
		batch.Op(deletionKey, []byte{0x00})
		keys[idx] = string(deletionKey)
		mut := bigtable.NewMutation()
		mut.DeleteRow()
		values[idx] = mut
	}
	if len(batch.GetBatch()) > 0 {
		errs, err := s.table.ApplyBulk(ctx, keys, values)
		if err != nil {
			return err
		}
		if len(errs) != 0 {
			return fmt.Errorf("apply bulk error: %w", multierr.Combine(errs...))
		}
	}
	return nil
}

func (s *Store) Scan(ctx context.Context, start, exclusiveEnd []byte, limit int, options ...store.ReadOption) *store.Iterator {
	startKey := s.withPrefix(start)
	endKey := s.withPrefix(exclusiveEnd)

	if traceEnabled {
		logging.Logger(ctx, zlog).Debug("scanning", zap.Stringer("start", store.Key(startKey)), zap.Stringer("exclusive_end", store.Key(endKey)), zap.Stringer("limit", store.Limit(limit)))
	}

	sit := store.NewIterator(ctx)
	if len(endKey) == 0 {
		// Act like the other backends
		sit.PushFinished()
		return sit
	}

	btOptions := bigtableReadOptions(store.Limit(limit), options)
	rowRange := bigtable.NewRange(string(startKey), string(endKey))

	go func() {
		err := s.table.ReadRows(ctx, rowRange, func(row bigtable.Row) bool {
			return sit.PushItem(store.KV{s.withoutPrefix([]byte(row.Key())), row[s.columnName][0].Value})
		}, btOptions...)

		if err != nil {
			sit.PushError(err)
			return
		}
		sit.PushFinished()
	}()

	return sit
}

func (s *Store) Prefix(ctx context.Context, prefix []byte, limit int, options ...store.ReadOption) *store.Iterator {
	if traceEnabled {
		logging.Logger(ctx, zlog).Debug("prefix scanning", zap.Stringer("prefix", store.Key(prefix)), zap.Stringer("limit", store.Limit(limit)))
	}

	sit := store.NewIterator(ctx)
	btOptions := bigtableReadOptions(store.Limit(limit), options)
	prefix = s.withPrefix(prefix)

	go func() {
		err := s.table.ReadRows(ctx, bigtable.PrefixRange(string(prefix)), func(row bigtable.Row) bool {
			return sit.PushItem(store.KV{s.withoutPrefix([]byte(row.Key())), row[s.columnName][0].Value})
		}, btOptions...)

		if err != nil {
			sit.PushError(err)
			return
		}

		sit.PushFinished() // there was an error there!
	}()

	return sit
}

func (s *Store) BatchPrefix(ctx context.Context, prefixes [][]byte, limit int, options ...store.ReadOption) *store.Iterator {
	if traceEnabled {
		logging.Logger(ctx, zlog).Debug("batch prefix scanning", zap.Int("prefix_count", len(prefixes)), zap.Stringer("limit", store.Limit(limit)))
	}

	sit := store.NewIterator(ctx)
	btOptions := bigtableReadOptions(store.Limit(limit), options)
	rowRanges := make([]bigtable.RowRange, len(prefixes))
	for i, prefix := range prefixes {
		rowRanges[i] = bigtable.PrefixRange(string(s.withPrefix(prefix)))
	}

	go func() {
		err := s.table.ReadRows(ctx, bigtable.RowRangeList(rowRanges), func(row bigtable.Row) bool {
			return sit.PushItem(store.KV{Key: s.withoutPrefix([]byte(row.Key())), Value: row[s.columnName][0].Value})
		}, btOptions...)

		if err != nil {
			sit.PushError(err)
			return
		}

		sit.PushFinished() // there was an error there!
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

var keyOnlyFilter = bigtable.StripValueFilter()
var latestCellFilter = bigtable.LatestNFilter(1)

func bigtableReadOptions(limit store.Limit, options []store.ReadOption) []bigtable.ReadOption {
	readOptions := store.ReadOptions{}
	for _, opt := range options {
		opt.Apply(&readOptions)
	}

	// We assume here that at most, we will get 2 filters, also, we assume that if key only is specified,
	// it should go first, theory here is that stripping value first (if required) puts less performance hit
	// on BigTable for subsequent filters, so order matters.
	var filters = make([]bigtable.Filter, 0, 2)
	if readOptions.KeyOnly {
		filters = append(filters, keyOnlyFilter)
	}

	filters = append(filters, latestCellFilter)

	var filterOption bigtable.ReadOption
	if len(filters) == 1 {
		filterOption = bigtable.RowFilter(filters[0])
	} else {
		filterOption = bigtable.RowFilter(bigtable.ChainFilters(filters...))
	}

	opts := []bigtable.ReadOption{filterOption}
	if store.Limit(limit).Bounded() {
		opts = append(opts, bigtable.LimitRows(int64(limit)))
	}

	return opts
}

func optionalTestEnv(project, instance string) {
	if isTestEnv(project, instance) && (os.Getenv(emulatorHostDefault) == "") {
		os.Setenv(emulatorHostDefault, emulatorDefaultHostValue)
	}
}

func isTestEnv(project, instance string) bool {
	return (strings.HasPrefix(project, "dev") || strings.HasPrefix(instance, "dev"))
}

const emulatorHostDefault = "BIGTABLE_EMULATOR_HOST"
const emulatorDefaultHostValue = "localhost:8086"
