package store

import (
	"fmt"
	"strings"

	"go.uber.org/zap"
)

type Option func(s *KVStore)

func WithLogger(logger *zap.Logger) Option {
	return func(s *KVStore) {
		str := *s
		if store, ok := str.(Configurable); ok {
			store.SetLogger(logger)
		}
	}
}

func WithPurgeableStore(tablePrefix []byte, ttlInBlocks uint64) Option {
	return func(s *KVStore) {
		*s = NewPurgeableStore(tablePrefix, *s, ttlInBlocks)
	}
}

func WithEmptyValue() Option {
	return func(s *KVStore) {
		str := *s
		if store, ok := str.(Configurable); ok {
			store.EnableEmpty()
		}
	}
}

// NewStoreFunc is a function for opening a databse.
type NewStoreFunc func(path string, opts ...Option) (KVStore, error)

type Registration struct {
	Name        string // unique name
	Title       string // human-readable name
	FactoryFunc NewStoreFunc
}

var registry = make(map[string]*Registration)

func Register(reg *Registration) {
	if reg.Name == "" {
		zlog.Fatal("name cannot be blank")
	} else if _, ok := registry[reg.Name]; ok {
		zlog.Fatal("already registered", zap.String("name", reg.Name))
	}
	registry[reg.Name] = reg
}

func New(dsn string, opts ...Option) (KVStore, error) {
	chunks := strings.Split(dsn, ":")
	reg, found := registry[chunks[0]]
	if !found {
		return nil, fmt.Errorf("no such kv store registered %q", chunks[0])
	}
	return reg.FactoryFunc(dsn, opts...)
}

// ByName returns a registered store driver
func ByName(name string) *Registration {
	r, ok := registry[name]
	if !ok {
		return nil
	}
	return r
}
