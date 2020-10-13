package netkvserver

import (
	"context"
	"fmt"
	"io"
	"net"

	"github.com/dfuse-io/kvdb/store"
	pbnetkv "github.com/dfuse-io/kvdb/store/netkv/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/reflection"
	"google.golang.org/grpc/status"
)

// Implement the SERVER aspect of netbadger, which defers to the `badger` KV store
// implementation.

type Server struct {
	store      store.KVStore
	grpcServer *grpc.Server
	listener   net.Listener
}

func Launch(listenAddr string, dsn string) (*Server, error) {
	str, err := store.New(dsn)
	if err != nil {
		return nil, fmt.Errorf("setting up kvdb store: %w", err)
	}

	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return nil, fmt.Errorf("failed listening: %w", err)
	}

	gsrv := grpc.NewServer()

	s := &Server{
		store:      str,
		grpcServer: gsrv,
		listener:   lis,
	}

	reflection.Register(gsrv)
	pbnetkv.RegisterNetKVServer(gsrv, s)

	go gsrv.Serve(lis)

	return s, nil
}

func (s *Server) Close() error {
	if closer, ok := s.store.(io.Closer); ok {
		if err := closer.Close(); err != nil {
			return err
		}
	}
	s.grpcServer.GracefulStop()
	if err := s.listener.Close(); err != nil {
		return err
	}
	return nil
}

func (s *Server) BatchPut(ctx context.Context, kvs *pbnetkv.KeyValues) (*pbnetkv.EmptyResponse, error) {
	for _, kv := range kvs.Kvs {
		err := s.store.Put(ctx, kv.Key, kv.Value)
		if err != nil {
			return nil, err
		}
	}
	if err := s.store.FlushPuts(ctx); err != nil {
		return nil, err
	}
	return &pbnetkv.EmptyResponse{}, nil
}

// BatchGet returns only values, and assumes the same order in values as the order of the input keys.
func (s *Server) BatchGet(keys *pbnetkv.Keys, stream pbnetkv.NetKV_BatchGetServer) error {
	if len(keys.Keys) == 0 {
		return status.Newf(codes.InvalidArgument, "at least one key required for BatchGet").Err()
	}
	if len(keys.Keys) == 1 {
		val, err := s.store.Get(stream.Context(), keys.Keys[0])
		if err != nil {
			return wrapNotFoundError(err)
		}
		if err := stream.Send(&pbnetkv.KeyValue{Value: val}); err != nil {
			return err
		}
		return nil
	}

	it := s.store.BatchGet(stream.Context(), keys.Keys)

	for it.Next() {
		if err := stream.Send(&pbnetkv.KeyValue{Value: it.Item().Value}); err != nil {
			return err
		}
	}
	if it.Err() != nil {
		return wrapNotFoundError(it.Err())
	}
	return nil
}

func (s *Server) BatchDelete(ctx context.Context, keys *pbnetkv.Keys) (*pbnetkv.EmptyResponse, error) {
	if len(keys.Keys) == 0 {
		return &pbnetkv.EmptyResponse{}, nil
	}

	err := s.store.BatchDelete(ctx, keys.Keys)
	if err != nil {
		return nil, err
	}

	return &pbnetkv.EmptyResponse{}, nil
}

func wrapNotFoundError(err error) error {
	// TODO: unwrap the `gRPC Status` object, and check with the `Code`
	if err == store.ErrNotFound {
		return status.Newf(codes.NotFound, err.Error()).Err()

	}
	return err
}

func (s *Server) Scan(req *pbnetkv.ScanRequest, stream pbnetkv.NetKV_ScanServer) error {
	it := s.store.Scan(stream.Context(), req.Start, req.ExclusiveEnd, int(req.Limit), storeReadOptions(req.Options)...)
	for it.Next() {
		item := it.Item()
		if err := stream.Send(&pbnetkv.KeyValue{Key: item.Key, Value: item.Value}); err != nil {
			return err
		}
	}
	if it.Err() != nil {
		return it.Err()
	}
	return nil
}

func (s *Server) BatchScan(req *pbnetkv.BatchScanRequest, stream pbnetkv.NetKV_BatchScanServer) error {
	return fmt.Errorf("unimplemented yet, until it's in the KVStore interface and our `badger` backend supports it.")
}

func (s *Server) Prefix(req *pbnetkv.PrefixRequest, stream pbnetkv.NetKV_PrefixServer) error {
	it := s.store.Prefix(stream.Context(), req.Prefix, int(req.Limit), storeReadOptions(req.Options)...)
	for it.Next() {
		item := it.Item()
		if err := stream.Send(&pbnetkv.KeyValue{Key: item.Key, Value: item.Value}); err != nil {
			return err
		}
	}
	if it.Err() != nil {
		return it.Err()
	}
	return nil
}

func (s *Server) BatchPrefix(req *pbnetkv.BatchPrefixRequest, stream pbnetkv.NetKV_BatchPrefixServer) error {
	it := s.store.BatchPrefix(stream.Context(), req.Prefixes, int(req.LimitPerPrefix), storeReadOptions(req.Options)...)
	for it.Next() {
		item := it.Item()
		if err := stream.Send(&pbnetkv.KeyValue{Key: item.Key, Value: item.Value}); err != nil {
			return err
		}
	}
	if it.Err() != nil {
		return it.Err()
	}
	return nil
}

func storeReadOptions(options *pbnetkv.ReadOptions) (out []store.ReadOption) {
	if options != nil && options.KeyOnly {
		return []store.ReadOption{store.KeyOnly()}
	}

	// There is not options for now, so we can always return nothing from that point
	return nil
}
