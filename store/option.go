package store

import "go.uber.org/zap"

type Option interface {
	apply(s ConfigurableKVStore)
}

type loggerOpt struct {
	logger *zap.Logger
}

func WithLogger(logger *zap.Logger) Option {
	return &loggerOpt{logger: logger}
}

func (l loggerOpt) apply(s ConfigurableKVStore) {
	s.SetLogger(l.logger)
}

type emptyValueOpt struct {
}

func WithEmptyValue() Option {
	return emptyValueOpt{}
}

func (e emptyValueOpt) apply(s ConfigurableKVStore) {
	s.EnableEmpty()
}

type purgeableOpt struct {
	tablePrefix []byte
	ttlInBlocks uint64
}

func WithPurgeableStore(tablePrefix []byte, ttlInBlocks uint64) Option {
	return purgeableOpt{
		tablePrefix: tablePrefix,
		ttlInBlocks: ttlInBlocks,
	}
}

func (p purgeableOpt) apply(s ConfigurableKVStore) {
}
