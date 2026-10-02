package bigkv

import (
	"context"
	"errors"
	"testing"
	"time"

	"cloud.google.com/go/bigtable"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPutDoesNotWaitForFlushInProgress(t *testing.T) {
	ctx := context.Background()
	kvStore := newEmulatorStore(t, "?createTable=true").(*Store)

	applyBulk := kvStore.applyBulk
	flushing := make(chan struct{})
	release := make(chan struct{})
	kvStore.applyBulk = func(ctx context.Context, rowKeys []string, muts []*bigtable.Mutation) ([]error, error) {
		close(flushing)
		<-release

		return applyBulk(ctx, rowKeys, muts)
	}

	require.NoError(t, kvStore.Put(ctx, []byte("first"), []byte("value")))

	flushed := make(chan error, 1)
	go func() { flushed <- kvStore.FlushPuts(ctx) }()
	<-flushing

	put := make(chan error, 1)
	go func() { put <- kvStore.Put(ctx, []byte("second"), []byte("value")) }()

	select {
	case err := <-put:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("Put waited for the flush in progress")
	}

	close(release)
	require.NoError(t, <-flushed)

	_, err := kvStore.Get(ctx, []byte("first"))
	require.NoError(t, err)

	kvStore.applyBulk = applyBulk
	require.NoError(t, kvStore.FlushPuts(ctx))

	_, err = kvStore.Get(ctx, []byte("second"))
	require.NoError(t, err, "an entry put during a flush must be written by the next one")
}

func TestFailedFlushKeepsEntriesForNextFlush(t *testing.T) {
	ctx := context.Background()
	kvStore := newEmulatorStore(t, "?createTable=true").(*Store)

	applyBulk := kvStore.applyBulk
	kvStore.applyBulk = func(context.Context, []string, []*bigtable.Mutation) ([]error, error) {
		return nil, errors.New("bigtable unavailable")
	}

	require.NoError(t, kvStore.Put(ctx, []byte("kept"), []byte("value")))
	require.NoError(t, kvStore.Put(ctx, []byte("rewritten"), []byte("old")))
	require.Error(t, kvStore.FlushPuts(ctx))

	kvStore.applyBulk = applyBulk

	require.NoError(t, kvStore.Put(ctx, []byte("rewritten"), []byte("new")))
	require.NoError(t, kvStore.FlushPuts(ctx))

	value, err := kvStore.Get(ctx, []byte("kept"))
	require.NoError(t, err)
	assert.Equal(t, []byte("value"), value)

	value, err = kvStore.Get(ctx, []byte("rewritten"))
	require.NoError(t, err)
	assert.Equal(t, []byte("new"), value, "an entry kept from a failed flush must not overwrite a newer one")
}
