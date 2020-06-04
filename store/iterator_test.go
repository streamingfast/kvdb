package store

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIteratorPushStuck(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	it := NewIterator(ctx)
	kv := KV{}

	// fill buffer
	for i := 0; i < 100; i++ {
		it.PushItem(kv)
	}

	cancel()
	ok := it.PushItem(kv)
	assert.False(t, ok)
}

func TestRaceFinished(t *testing.T) {
	ctx := context.Background()

	for i := 0; i < 100; i++ {
		it := NewIterator(ctx)
		kv := KV{}
		require.True(t, it.PushItem(kv))
		it.PushFinished()
		assert.True(t, it.Next())
		assert.Equal(t, kv, it.Item())

	}
}

func TestErrorExit(t *testing.T) {
	ctx := context.Background()
	it := NewIterator(ctx)
	deadErr := errors.New("dead")
	it.PushError(deadErr)
	assert.False(t, it.Next())
	assert.Equal(t, deadErr, it.Err())
}
