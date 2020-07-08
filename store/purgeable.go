package store

import (
	"context"
	"encoding/binary"
	"fmt"

	"go.uber.org/zap"
)

var PurgeableMaxBatchSize = 500

type PurgeableKVStore struct {
	ConfigurableKVStore
	tablePrefix []byte // 0x09
	height      uint64 // ecah time you're writing a block, startby  marking this block so all keys wehre we call "Put()"
	heightSet   bool
	ttlInBlocks uint64
}

func NewPurgeableStore(tablePrefix []byte, store ConfigurableKVStore, ttlInBlocks uint64) *PurgeableKVStore {
	if _, ok := store.(Deletable); !ok {
		panic("Purgeable KVStores requires a Deletable KVStore (implements `BatchDelete`)")
	}
	return &PurgeableKVStore{
		ConfigurableKVStore: store,
		tablePrefix:         tablePrefix,
		ttlInBlocks:         ttlInBlocks,
	}
}

func (s *PurgeableKVStore) Put(ctx context.Context, key, value []byte) error {
	if !s.heightSet {
		panic("ephemeral kv store height not set")
	}
	if err := s.ConfigurableKVStore.Put(ctx, key, value); err != nil {
		return err
	}

	deletionKey := s.deletionKey(s.height, key)

	if err := s.ConfigurableKVStore.Put(ctx, deletionKey, []byte{0x00}); err != nil {
		return err
	}
	return nil
}

func (s *PurgeableKVStore) MarkCurrentHeight(height uint64) {
	s.height = height
	s.heightSet = true
}

func (s *PurgeableKVStore) PurgeKeys(ctx context.Context) error {
	if s.height < s.ttlInBlocks {
		return nil
	}
	lowBlockNum := uint64(0)
	highBlockNum := uint64(s.height - s.ttlInBlocks)
	zlog.Debug("purging below",
		zap.Uint64("high_block_num", highBlockNum),
		zap.Uint64("low_block_num", lowBlockNum),
	)
	startKey := s.deletionKey(lowBlockNum, []byte{})
	endKey := s.deletionKey(highBlockNum, []byte{})

	itr := s.Scan(ctx, startKey, endKey, Unlimited)
	deletionKey := [][]byte{}
	for itr.Next() {
		if len(deletionKey) >= PurgeableMaxBatchSize {
			err := s.ConfigurableKVStore.(Deletable).BatchDelete(ctx, deletionKey)
			if err != nil {
				return fmt.Errorf("unable to delete batch: %w", err)
			}
			deletionKey = [][]byte{}
		}
		deletionKey = append(deletionKey, itr.Item().Key)
		deletionKey = append(deletionKey, s.originalKey(itr.Item().Key))
	}
	if len(deletionKey) >= 0 {
		err := s.ConfigurableKVStore.(Deletable).BatchDelete(ctx, deletionKey)
		if err != nil {
			return fmt.Errorf("unable to delete batch: %w", err)
		}
	}
	return nil
}

func (s *PurgeableKVStore) deletionKey(height uint64, key []byte) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, height)
	deletionKey := append(s.tablePrefix, buf...)
	return append(deletionKey, key...)
}

func (s *PurgeableKVStore) originalKey(deletionKey []byte) []byte {
	offset := len(s.tablePrefix) + 8
	return deletionKey[offset:]
}
