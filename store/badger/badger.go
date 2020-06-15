package badger

import (
	"bytes"
	"context"
	"fmt"
	"net/url"
	"os"
	"path/filepath"

	"github.com/dfuse-io/kvdb/store"
	"github.com/dgraph-io/badger/v2"
	"github.com/dgraph-io/badger/v2/options"
	"go.uber.org/zap"
)

type Store struct {
	db         *badger.DB
	writeBatch *badger.WriteBatch
	compressor store.Compressor
}

func init() {
	store.Register(&store.Registration{
		Name:        "badger",
		Title:       "Badger",
		FactoryFunc: NewStore,
	})
}

func NewStore(dsnString string, opts ...store.Option) (store.KVStore, error) {
	dsn, err := url.Parse(dsnString)
	if err != nil {
		return nil, fmt.Errorf("badger new: dsn: %w", err)
	}

	zlog.Debug("setting up badger db",
		zap.String("dsn.path", dsnString),
	)

	createPath := filepath.Dir(dsn.Path)
	if err := os.MkdirAll(createPath, 0755); err != nil {
		return nil, fmt.Errorf("creating path %q: %s", createPath, err)
	}

	db, err := badger.Open(badger.DefaultOptions(dsn.Path).WithLogger(nil).WithCompression(options.Snappy))
	if err != nil {
		return nil, fmt.Errorf("badger new: open badger db: %w", err)
	}

	//Deprecated: this is only used for backward compatible support as we deprecated this support in Badger
	// It only allow for seamless decompression -- otherwise Snappy kicks in automatically
	compressor, err := store.NewCompressor(dsn.Query().Get("compression"))
	if err != nil {
		return nil, err
	}

	s := &Store{
		db:         db,
		compressor: compressor,
	}

	return s, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) Put(ctx context.Context, key, value []byte) (err error) {
	zlog.Debug("putting", zap.Stringer("key", store.Key(key)))
	if s.writeBatch == nil {
		s.writeBatch = s.db.NewWriteBatch()
	}

	value = s.compressor.Compress(value)

	err = s.writeBatch.SetEntry(badger.NewEntry(key, value))
	if err == badger.ErrTxnTooBig {
		zlog.Debug("txn too big pre-emptively pushing")
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

func (s *Store) Get(ctx context.Context, key []byte) (value []byte, err error) {
	err = s.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return store.ErrNotFound
			}
			return err
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

func (s *Store) BatchGet(ctx context.Context, keys [][]byte) *store.Iterator {
	kr := store.NewIterator(ctx)

	go func() {
		err := s.db.View(func(txn *badger.Txn) error {
			for _, key := range keys {
				item, err := txn.Get(key)
				if err != nil {
					return err
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
			kr.PushError(err)
			return
		}
		kr.PushFinished()
	}()
	return kr
}

func (s *Store) Scan(ctx context.Context, start, exclusiveEnd []byte, limit int) *store.Iterator {
	sit := store.NewIterator(ctx)
	zlog.Debug("scanning", zap.Stringer("start", store.Key(start)), zap.Stringer("exclusive_end", store.Key(exclusiveEnd)), zap.Stringer("limit", store.Limit(limit)))
	go func() {
		err := s.db.View(func(txn *badger.Txn) error {
			options := badger.DefaultIteratorOptions
			if store.Limit(limit).Bounded() && limit < options.PrefetchSize {
				options.PrefetchSize = limit
			}

			bit := txn.NewIterator(options)
			defer bit.Close()

			count := uint64(0)
			for bit.Seek(start); bit.Valid() && bytes.Compare(bit.Item().Key(), exclusiveEnd) == -1; bit.Next() {
				count++
				value, err := bit.Item().ValueCopy(nil)
				if err != nil {
					return err
				}

				value, err = s.compressor.Decompress(value)
				if err != nil {
					return err
				}

				if !sit.PushItem(store.KV{bit.Item().KeyCopy(nil), value}) {
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

func (s *Store) Prefix(ctx context.Context, prefix []byte, limit int) *store.Iterator {
	kr := store.NewIterator(ctx)
	zlog.Debug("prefix scanning", zap.Stringer("prefix", store.Key(prefix)), zap.Stringer("limit", store.Limit(limit)))
	go func() {
		err := s.db.View(func(txn *badger.Txn) error {
			options := badger.DefaultIteratorOptions
			if store.Limit(limit).Bounded() && limit < options.PrefetchSize {
				options.PrefetchSize = limit
			}

			it := txn.NewIterator(options)
			defer it.Close()

			count := uint64(0)
			for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
				count++

				key := it.Item().KeyCopy(nil)
				value, err := it.Item().ValueCopy(nil)
				if err != nil {
					return err
				}

				value, err = s.compressor.Decompress(value)
				if err != nil {
					return err
				}

				if !kr.PushItem(store.KV{key, value}) {
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

func (s *Store) BatchPrefix(ctx context.Context, prefixes [][]byte, limit int) *store.Iterator {
	kr := store.NewIterator(ctx)
	zlog.Debug("batch prefix scanning", zap.Int("prefix_count", len(prefixes)), zap.Stringer("limit", store.Limit(limit)))

	go func() {
		err := s.db.View(func(txn *badger.Txn) error {
			options := badger.DefaultIteratorOptions
			if store.Limit(limit).Bounded() && limit < options.PrefetchSize {
				options.PrefetchSize = limit
			}

			it := txn.NewIterator(options)
			defer it.Close()

			count := uint64(0)
		terminateLoop:
			for _, prefix := range prefixes {
				for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
					count++

					key := it.Item().KeyCopy(nil)
					value, err := it.Item().ValueCopy(nil)
					if err != nil {
						return err
					}

					value, err = s.compressor.Decompress(value)
					if err != nil {
						return err
					}

					if !kr.PushItem(store.KV{Key: key, Value: value}) {
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
