package store

import (
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestPurgeableKVStore_MarkCurrentHeight(t *testing.T) {
	store := &PurgeableKVStore{
		tablePrefix: []byte{0x09},
	}
	require.Equal(t, store.heightSet, false)
	store.MarkCurrentHeight(100)
	assert.Equal(t, store.height, uint64(100))
	assert.Equal(t, store.heightSet, true)
}

func TestPurgeableKVStore_deletionKey(t *testing.T) {
	store := &PurgeableKVStore{
		tablePrefix: []byte{0x09},
	}
	key := []byte{0xaa,0xbb,0xcc}

	deletionKey := store.deletionKey(100, key)
	assert.Equal(t, []byte{0x09,0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x64, 0xaa,0xbb,0xcc}, deletionKey)
}


func TestPurgeableKVStore_explodeDeletionKey(t *testing.T) {
	store := &PurgeableKVStore{
		tablePrefix: []byte{0x09},
	}
	deletionKey := []byte{0x09,0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x64, 0xaa,0xbb,0xcc}
	assert.Equal(t, []byte{0xaa,0xbb,0xcc}, store.originalKey(deletionKey))
}
