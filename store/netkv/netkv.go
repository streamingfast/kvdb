package netkv

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"strings"

	"github.com/dfuse-io/kvdb/store"
	pbnetkv "github.com/dfuse-io/kvdb/store/netkv/pb"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type Store struct {
	conn     *grpc.ClientConn
	client   pbnetkv.NetKVClient
	putBatch []*pbnetkv.KeyValue
	zlogger  *zap.Logger
}

func init() {
	store.Register(&store.Registration{
		Name:        "netkv",
		Title:       "netkv",
		FactoryFunc: NewStore,
	})
}

func NewStore(dsnString string, opts ...store.Option) (store.ConfigurableKVStore, error) {
	dsn, err := url.Parse(dsnString)
	if err != nil {
		return nil, fmt.Errorf("badger new: dsn: %w", err)
	}

	var grpcOpts []grpc.DialOption
	if dsn.Query().Get("insecure") == "true" {
		grpcOpts = append(grpcOpts, grpc.WithInsecure())
	}

	// TODO: init gRPC connection to the `dsn.Host`
	conn, err := grpc.Dial(dsn.Host, grpcOpts...)
	if err != nil {
		return nil, err
	}

	client := pbnetkv.NewNetKVClient(conn)

	s := &Store{
		conn:    conn,
		client:  client,
		zlogger: zap.NewNop(),
	}

	return s, nil
}

func (s *Store) Close() error {
	return s.conn.Close()
}

func (s *Store) Put(ctx context.Context, key, value []byte) (err error) {
	s.zlogger.Debug("putting", zap.Stringer("key", store.Key(key)))
	s.putBatch = append(s.putBatch, &pbnetkv.KeyValue{Key: key, Value: value})
	return nil
}

func (s *Store) FlushPuts(ctx context.Context) error {
	if s.putBatch == nil {
		return nil
	}
	_, err := s.client.BatchPut(ctx, &pbnetkv.KeyValues{Kvs: s.putBatch})
	if err != nil {
		return err
	}
	s.putBatch = nil
	return nil
}

func wrapNotFoundError(err error) error {
	// TODO: unwrap the `gRPC Status` object, and check with the `Code`
	if strings.Contains(err.Error(), "not found") {
		return store.ErrNotFound
	}
	return err
}

func (s *Store) Get(ctx context.Context, key []byte) (value []byte, err error) {
	resp, err := s.client.BatchGet(ctx, &pbnetkv.Keys{Keys: [][]byte{key}})
	if err != nil {
		return nil, err
	}
	for {
		kv, err := resp.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, wrapNotFoundError(err)
		}

		if value != nil {
			return nil, fmt.Errorf("duplicate response when we expected a single return value")
		}

		// TODO: we'll check `NotFound` in the `BatchGet` eventually?
		value = kv.Value
	}
	return
}

func (s *Store) BatchGet(ctx context.Context, keys [][]byte) *store.Iterator {
	it := store.NewIterator(ctx)

	go func() {
		resp, err := s.client.BatchGet(ctx, &pbnetkv.Keys{Keys: keys})
		if err != nil {
			it.PushError(err)
			return
		}
		for {
			kv, err := resp.Recv()
			if !pushToIterator(it, kv, err) {
				break
			}
		}
	}()
	return it
}

func (s *Store) Scan(ctx context.Context, start, exclusiveEnd []byte, limit int) *store.Iterator {
	it := store.NewIterator(ctx)

	go func() {
		resp, err := s.client.Scan(ctx, &pbnetkv.ScanRequest{Start: start, ExclusiveEnd: exclusiveEnd, Limit: uint64(limit)})
		if err != nil {
			it.PushError(err)
			return
		}
		for {
			kv, err := resp.Recv()
			if !pushToIterator(it, kv, err) {
				break
			}
		}
	}()
	return it
}

func pushToIterator(it *store.Iterator, kv *pbnetkv.KeyValue, err error) bool {
	if err == io.EOF {
		it.PushFinished()
		return false
	}
	if err != nil {
		it.PushError(wrapNotFoundError(err))
		return false
	}

	// TODO: we'll check `NotFound` in the `BatchGet` eventually?
	return it.PushItem(store.KV{kv.Key, kv.Value})
}

func (s *Store) Prefix(ctx context.Context, prefix []byte, limit int) *store.Iterator {
	it := store.NewIterator(ctx)

	go func() {
		resp, err := s.client.Prefix(ctx, &pbnetkv.PrefixRequest{Prefix: prefix, Limit: uint64(limit)})
		if err != nil {
			it.PushError(err)
			return
		}
		for {
			kv, err := resp.Recv()
			if !pushToIterator(it, kv, err) {
				break
			}
		}
	}()
	return it
}

func (s *Store) BatchPrefix(ctx context.Context, prefixes [][]byte, limitPerPrefix int) *store.Iterator {
	it := store.NewIterator(ctx)

	go func() {
		resp, err := s.client.BatchPrefix(ctx, &pbnetkv.BatchPrefixRequest{Prefixes: prefixes, LimitPerPrefix: uint64(limitPerPrefix)})
		if err != nil {
			it.PushError(err)
			return
		}
		for {
			kv, err := resp.Recv()
			if !pushToIterator(it, kv, err) {
				break
			}
		}
	}()
	return it
}
