package store

import "context"

type KVStore interface {
	// Put writes to a transaction, which might be flushed from time to time. Call FlushPuts() to ensure all Put entries are properly written to the database.
	Put(ctx context.Context, key, value []byte) (err error)
	// FlushPuts takes any pending writes (calls to Put()), and flushes them.
	FlushPuts(ctx context.Context) (err error)

	// Get a given key.  Returns `kvdb.ErrNotFound` if not found.
	Get(ctx context.Context, key []byte) (value []byte, err error)
	// Get a batch of keys.  Returns `kvdb.ErrNotFound` the first time a key is not found: not finding a key is fatal and interrupts the resultset from being fetched completely.  BatchGet guarantees that Iterator return results in the exact same order as keys
	BatchGet(ctx context.Context, keys [][]byte) *Iterator

	Scan(ctx context.Context, start, exclusiveEnd []byte, limit int) *Iterator
	Prefix(ctx context.Context, prefix []byte, limit int) *Iterator
}

type ReversibleKVStore interface {
	ReverseScan(ctx context.Context, start, exclusiveEnd []byte, limit int) *Iterator
	ReversePrefix(ctx context.Context, prefix []byte, limit int) *Iterator
}
