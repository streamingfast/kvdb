package badger

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"net/url"
	"os"
	"path/filepath"

	"github.com/dfuse-io/kvdb/store"
	"github.com/dfuse-io/logging"
	"github.com/dgraph-io/badger/v2"
	"github.com/dgraph-io/badger/v2/options"
	"go.uber.org/zap"
)

type Store struct {
	dsn        string
	db         *badger.DB
	writeBatch *badger.WriteBatch
	compressor store.Compressor
}

func (s *Store) String() string {
	return fmt.Sprintf("badger kv store with dsn: %q", s.dsn)
}

func init() {
	store.Register(&store.Registration{
		Name:        "badger",
		Title:       "Badger",
		FactoryFunc: NewStore,
	})
}

func NewStore(dsnString string) (store.KVStore, error) {
	dsn, err := url.Parse(dsnString)
	if err != nil {
		return nil, fmt.Errorf("badger new: dsn: %w", err)
	}

	createPath := filepath.Dir(dsn.Path)
	if err := os.MkdirAll(createPath, 0755); err != nil {
		return nil, fmt.Errorf("creating path %q: %w", createPath, err)
	}

	db, err := badger.Open(badger.DefaultOptions(dsn.Path).WithLogger(nil).WithCompression(options.Snappy))
	if err != nil {
		return nil, fmt.Errorf("badger new: open badger db: %w", err)
	}

	// Deprecated: this is only used for backward compatible support as we deprecated this support in Badger
	// It only allows for seamless decompression -- otherwise Snappy kicks in automatically. This is why we
	// use `math.MaxInt64` as the threshold to use for compression, this way, compression never kicks in
	// (because size will always be < than `math.MaxInt64`).
	compressor, err := store.NewCompressor(dsn.Query().Get("compression"), math.MaxInt64)
	if err != nil {
		return nil, err
	}

	s := &Store{
		dsn:        dsnString,
		db:         db,
		compressor: compressor,
	}
	return s, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) Put(ctx context.Context, key, value []byte) (err error) {
	zlogger := logging.Logger(ctx, zlog)
	zlogger.Debug("putting", zap.Stringer("key", store.Key(key)))
	if s.writeBatch == nil {
		s.writeBatch = s.db.NewWriteBatch()
	}

	value = s.compressor.Compress(value)

	err = s.writeBatch.SetEntry(badger.NewEntry(key, value))
	if err == badger.ErrTxnTooBig {
		zlogger.Debug("txn too big pre-emptively pushing")
		if err := s.writeBatch.Flush(); err != nil {
			return err
		}

		s.writeBatch = s.db.NewWriteBatch()
		err := s.writeBatch.SetEntry(badger.NewEntry(key, value))
		if err != nil {
			return fmt.Errorf("after txn too big: %w", err)
		}
	}

	return nil
}

func (s *Store) FlushPuts(ctx context.Context) error {
	if s.writeBatch == nil {
		return nil
	}
	err := s.writeBatch.Flush()
	if err != nil {
		return err
	}
	s.writeBatch = s.db.NewWriteBatch()
	return nil
}

func wrapNotFoundError(err error) error {
	if err == badger.ErrKeyNotFound {
		return store.ErrNotFound
	}
	return err
}

func (s *Store) Get(ctx context.Context, key []byte) (value []byte, err error) {
	err = s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return wrapNotFoundError(err)
		}

		// TODO: optimize: if we're going to decompress, we can use the `item.Value` instead
		// of making a copy
		value, err = item.ValueCopy(nil)
		if err != nil {
			return err
		}

		value, err = s.compressor.Decompress(value)
		if err != nil {
			return err
		}

		return nil
	})
	return
}

func (s *Store) BatchDelete(ctx context.Context, keys [][]byte) (err error) {
	zlogger := logging.Logger(ctx, zlog)

	zlogger.Debug("batch deletion", zap.Int("key_count", len(keys)))

	deletionBatch := s.db.NewWriteBatch()
	for _, key := range keys {
		err = deletionBatch.Delete(key)
		if err == badger.ErrTxnTooBig {
			zlogger.Debug("txn too big pre-emptively pushing")
			if err := deletionBatch.Flush(); err != nil {
				return err
			}

			deletionBatch = s.db.NewWriteBatch()
			err := deletionBatch.Delete(key)
			if err != nil {
				return err
			}
		}
	}

	if deletionBatch == nil {
		return nil
	}
	return deletionBatch.Flush()
}

func (s *Store) BatchGet(ctx context.Context, keys [][]byte) *store.Iterator {
	kr := store.NewIterator(ctx)

	go func() {
		err := s.db.View(func(txn *badger.Txn) error {
			for _, key := range keys {
				item, err := txn.Get(key)
				if err != nil {
					return wrapNotFoundError(err)
				}

				value, err := item.ValueCopy(nil)
				if err != nil {
					return err
				}

				value, err = s.compressor.Decompress(value)
				if err != nil {
					return err
				}

				if !kr.PushItem(store.KV{item.KeyCopy(nil), value}) {
					break
				}

				// TODO: make sure this is conform and takes inspiration from `Scan`.. deals
				// with the `store.Iterator` properly
			}
			return nil
		})
		if err != nil {
			kr.PushError(wrapNotFoundError(err))
			return
		}
		kr.PushFinished()
	}()
	return kr
}

func (s *Store) Scan(ctx context.Context, start, exclusiveEnd []byte, limit int, options ...store.ReadOption) *store.Iterator {
	zlogger := logging.Logger(ctx, zlog)
	sit := store.NewIterator(ctx)
	zlogger.Debug("scanning", zap.Stringer("start", store.Key(start)), zap.Stringer("exclusive_end", store.Key(exclusiveEnd)), zap.Stringer("limit", store.Limit(limit)))
	go func() {
		err := s.db.View(func(txn *badger.Txn) error {
			badgerOptions := badgerIteratorOptions(store.Limit(limit), options)
			bit := txn.NewIterator(badgerOptions)
			defer bit.Close()

			var err error
			count := uint64(0)
			for bit.Seek(start); bit.Valid() && bytes.Compare(bit.Item().Key(), exclusiveEnd) == -1; bit.Next() {
				count++

				// We require value only when `PrefetchValues` is true, otherwise, we are performing a key-only iteration and as such,
				// we should not fetch nor decompress actual value
				var value []byte
				if badgerOptions.PrefetchValues {
					value, err = bit.Item().ValueCopy(nil)
					if err != nil {
						return err
					}

					value, err = s.compressor.Decompress(value)
					if err != nil {
						return err
					}
				}

				if !sit.PushItem(store.KV{Key: bit.Item().KeyCopy(nil), Value: value}) {
					break
				}

				if store.Limit(limit).Reached(count) {
					break
				}
			}
			return nil
		})
		if err != nil {
			sit.PushError(err)
			return
		}

		sit.PushFinished()
	}()

	return sit
}

func (s *Store) Prefix(ctx context.Context, prefix []byte, limit int, options ...store.ReadOption) *store.Iterator {
	zlogger := logging.Logger(ctx, zlog)
	kr := store.NewIterator(ctx)
	zlogger.Debug("prefix scanning", zap.Stringer("prefix", store.Key(prefix)), zap.Stringer("limit", store.Limit(limit)))
	go func() {
		err := s.db.View(func(txn *badger.Txn) error {
			badgerOptions := badgerIteratorOptions(store.Limit(limit), options)
			badgerOptions.Prefix = prefix

			it := txn.NewIterator(badgerOptions)
			defer it.Close()

			var err error
			count := uint64(0)
			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				count++

				// We require value only when `PrefetchValues` is true, otherwise, we are performing a key-only iteration and as such,
				// we should not fetch nor decompress actual value
				var value []byte
				if badgerOptions.PrefetchValues {
					value, err = it.Item().ValueCopy(nil)
					if err != nil {
						return err
					}

					value, err = s.compressor.Decompress(value)
					if err != nil {
						return err
					}
				}

				if !kr.PushItem(store.KV{Key: it.Item().KeyCopy(nil), Value: value}) {
					break
				}

				if store.Limit(limit).Reached(count) {
					break
				}
			}
			return nil
		})
		if err != nil {
			kr.PushError(err)
			return
		}

		kr.PushFinished()
	}()

	return kr
}

func (s *Store) BatchPrefix(ctx context.Context, prefixes [][]byte, limit int, options ...store.ReadOption) *store.Iterator {
	zlogger := logging.Logger(ctx, zlog)
	kr := store.NewIterator(ctx)
	zlogger.Debug("batch prefix scanning", zap.Int("prefix_count", len(prefixes)), zap.Stringer("limit", store.Limit(limit)))

	go func() {
		err := s.db.View(func(txn *badger.Txn) error {
			badgerOptions := badgerIteratorOptions(store.Limit(limit), options)
			it := txn.NewIterator(badgerOptions)
			defer it.Close()

			var err error
			count := uint64(0)
		terminateLoop:
			for _, prefix := range prefixes {
				for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
					count++

					// We require value only when `PrefetchValues` is true, otherwise, we are performing a key-only iteration and as such,
					// we should not fetch nor decompress actual value
					var value []byte
					if badgerOptions.PrefetchValues {
						value, err = it.Item().ValueCopy(nil)
						if err != nil {
							return err
						}

						value, err = s.compressor.Decompress(value)
						if err != nil {
							return err
						}
					}

					if !kr.PushItem(store.KV{Key: it.Item().KeyCopy(nil), Value: value}) {
						break terminateLoop
					}

					if store.Limit(limit).Reached(count) {
						break terminateLoop
					}
				}
			}

			return nil
		})

		if err != nil {
			kr.PushError(err)
			return
		}

		kr.PushFinished()
	}()

	return kr
}

func badgerIteratorOptions(limit store.Limit, options []store.ReadOption) badger.IteratorOptions {
	if limit.Unbounded() && len(options) == 0 {
		return badger.DefaultIteratorOptions
	}

	readOptions := store.ReadOptions{}
	for _, opt := range options {
		opt.Apply(&readOptions)
	}

	opts := badger.DefaultIteratorOptions
	if readOptions.KeyOnly {
		opts.PrefetchValues = false
	} else if limit.Bounded() && int(limit) < opts.PrefetchSize {
		opts.PrefetchSize = int(limit)
	}

	return opts
}
