package bigkv

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
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

func TestWritersFindingBatchFullFlushOnce(t *testing.T) {
	const writerCount = 8

	ctx := context.Background()
	kvStore := newEmulatorStore(t, "?createTable=true&maxRowsBeforeFlush=100").(*Store)

	var flushes atomic.Int64
	applyBulk := kvStore.applyBulk
	kvStore.applyBulk = func(ctx context.Context, rowKeys []string, muts []*bigtable.Mutation) ([]error, error) {
		flushes.Add(1)
		return applyBulk(ctx, rowKeys, muts)
	}

	// One entry short of the threshold, so that every writer below finds the batch full.
	for i := 0; i < 99; i++ {
		require.NoError(t, kvStore.Put(ctx, []byte(fmt.Sprintf("filler-%02d", i)), []byte("value")))
	}

	// Flushes are held back until every writer has found the batch full and is waiting for
	// its turn to flush.
	kvStore.flushLock.Lock()

	var wg sync.WaitGroup
	for writer := 0; writer < writerCount; writer++ {
		wg.Add(1)

		go func(writer int) {
			defer wg.Done()
			assert.NoError(t, kvStore.Put(ctx, testKey(writer, 0), []byte("value")))
		}(writer)
	}

	time.Sleep(200 * time.Millisecond)
	kvStore.flushLock.Unlock()
	wg.Wait()

	assert.EqualValues(t, 1, flushes.Load(), "only the first writer has anything left to flush")

	require.NoError(t, kvStore.FlushPuts(ctx))
	for writer := 0; writer < writerCount; writer++ {
		_, err := kvStore.Get(ctx, testKey(writer, 0))
		require.NoError(t, err)
	}
}
