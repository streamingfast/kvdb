package store

import "context"

type TestPurgeableKVDBDriver struct {
	KVStore
	DSN string
}

func (t *TestPurgeableKVDBDriver) MarkCurrentHeight(height uint64) {
	panic("test purgeable driver, not callable")
}

func (t *TestPurgeableKVDBDriver) PurgeKeys(ctx context.Context) error {
	panic("test purgeable driver, not callable")
}

func NewTestPurgeableKVDBDriver(dsn string) *TestPurgeableKVDBDriver {
	return &TestPurgeableKVDBDriver{
		KVStore: NewTestKVDBDriver(dsn),
		DSN:     dsn,
	}
}

type TestKVDBDriver struct {
	DSN string
}

func NewTestKVDBDriver(dsn string) *TestKVDBDriver {
	return &TestKVDBDriver{
		DSN: dsn,
	}
}
func RegisterTestKVDBDriver() {
	if !isRegistered("test") {
		Register(&Registration{
			Name:        "test",
			Title:       "Test KVDB Driver",
			FactoryFunc: NewTestKVDBDriverFactory,
		})
	}
}

func NewTestKVDBDriverFactory(dsn string) (KVStore, error) {
	return &TestKVDBDriver{
		DSN: dsn,
	}, nil
}

func (t *TestKVDBDriver) Put(ctx context.Context, key, value []byte) (err error) {
	panic("test driver, not callable")
}

func (t *TestKVDBDriver) FlushPuts(ctx context.Context) (err error) {
	panic("test driver, not callable")
}

func (t *TestKVDBDriver) Get(ctx context.Context, key []byte) (value []byte, err error) {
	panic("test driver, not callable")
}

func (t *TestKVDBDriver) BatchGet(ctx context.Context, keys [][]byte) *Iterator {
	panic("test driver, not callable")
}

func (t *TestKVDBDriver) Scan(ctx context.Context, start, exclusiveEnd []byte, limit int, options ...ReadOption) *Iterator {
	panic("test driver, not callable")
}

func (t *TestKVDBDriver) Prefix(ctx context.Context, prefix []byte, limit int, options ...ReadOption) *Iterator {
	panic("test driver, not callable")
}

func (t *TestKVDBDriver) BatchPrefix(ctx context.Context, prefixes [][]byte, limit int, options ...ReadOption) *Iterator {
	panic("test driver, not callable")
}

func (t *TestKVDBDriver) BatchDelete(ctx context.Context, keys [][]byte) (err error) {
	panic("test driver, not callable")
}

func (t *TestKVDBDriver) Close() error {
	return nil
}
