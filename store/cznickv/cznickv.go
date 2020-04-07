package cznickv

import (
	"bytes"
	"context"
	"encoding/hex"
	"io"
	"net/url"

	"github.com/dgraph-io/badger/v2"
	"github.com/dfuse-io/kvdb/store"
	"go.uber.org/zap"
	"modernc.org/kv"
)

type Store struct {
	db         *kv.DB
	writeBatch *badger.WriteBatch
}

func init() {
	store.Register(&store.Registration{
		Name:        "cznickv",
		Title:       "https://gitlab.com/cznic/kv and https://godoc.org/modernc.org/kv",
		FactoryFunc: NewStore,
	})
}

func NewStore(dsnString string) (store.KVStore, error) {
	u, err := url.Parse(dsnString)
	if err != nil {
		return nil, err
	}

	// TODO: kv.Open if `u.Path` exists already

	db, err := kv.Create(u.Path, &kv.Options{})
	if err != nil {
		return nil, err
	}

	return &Store{db: db}, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) Put(ctx context.Context, key, value []byte) (err error) {
	zlog.Debug("putting", zap.String("key", hex.EncodeToString(key)))
	return s.db.Set(key, value)
}

func (s *Store) FlushPuts(ctx context.Context) error {
	return nil
}

func (s *Store) Get(ctx context.Context, key []byte) (value []byte, err error) {
	val, err := s.db.Get(nil, key)
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
			val, err := s.db.Get(nil, key)
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
	sit := store.NewIterator(ctx)
	zlog.Debug("scanning", zap.String("start", hex.EncodeToString(start)), zap.String("exclusive_end", hex.EncodeToString(exclusiveEnd)), zap.Int("limit", limit))
	go func() {
		count := 0
		kit, hit, err := s.db.Seek(start)
		_ = hit
		if err != nil {
			sit.PushError(err)
			return
		}
		for {
			key, value, err := kit.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				sit.PushError(err)
				return
			}

			if bytes.Compare(key, exclusiveEnd) != -1 {
				break
			}

			sit.PushItem(&store.KV{key, value})

			if count == limit && limit > 0 {
				break
			}
		}

		sit.PushFinished()
	}()

	return sit
}

func (s *Store) Prefix(ctx context.Context, prefix []byte) *store.Iterator {
	sit := store.NewIterator(ctx)
	zlog.Debug("prefix scanning ", zap.String("prefix", hex.EncodeToString(prefix)))

	go func() {
		kit, hit, err := s.db.Seek(prefix)
		_ = hit
		if err != nil {
			sit.PushError(err)
			return
		}
		for {
			key, value, err := kit.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				sit.PushError(err)
				return
			}

			if !bytes.HasPrefix(key, prefix) {
				break
			}

			sit.PushItem(&store.KV{key, value})
		}

		sit.PushFinished()
	}()

	return sit
}
