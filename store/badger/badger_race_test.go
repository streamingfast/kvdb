package badger

import (
	"context"
	"fmt"
	"io"
	"path"
	"sync"
	"testing"

	"github.com/streamingfast/kvdb/store"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConcurrentPut ensures that concurrent `Put` calls on a single store do not lose entries.
// It is meant to be run with `-race` but even without it, two goroutines that both see a nil
// `writeBatch` create one each and only one of them survives the assignment, everything written
// in the other one is silently dropped.
func TestConcurrentPut(t *testing.T) {
	const (
		writerCount   = 16
		putsPerWriter = 64
	)

	ctx := context.Background()
	kvStore := newTempStore(t)

	var wg sync.WaitGroup
	for writer := 0; writer < writerCount; writer++ {
		wg.Add(1)

		go func(writer int) {
			defer wg.Done()

			for i := 0; i < putsPerWriter; i++ {
				// Assertions are used here instead of requirements because `require` calls `t.FailNow`
				// which is invalid from a goroutine that is not the test's one.
				assert.NoError(t, kvStore.Put(ctx, testKey(writer, i), []byte("value")))
			}
		}(writer)
	}
	wg.Wait()

	require.NoError(t, kvStore.FlushPuts(ctx))

	seen := map[string]bool{}
	it := kvStore.Scan(ctx, []byte("0"), []byte("z"), store.Unlimited)
	for it.Next() {
		seen[string(it.Item().Key)] = true
	}
	require.NoError(t, it.Err())

	missingCount := 0
	firstMissing := ""
	for writer := 0; writer < writerCount; writer++ {
		for i := 0; i < putsPerWriter; i++ {
			if key := string(testKey(writer, i)); !seen[key] {
				missingCount++
				if firstMissing == "" {
					firstMissing = key
				}
			}
		}
	}

	assert.Zero(t, missingCount, "%d entries out of %d were lost by the batch, first missing one is %q", missingCount, writerCount*putsPerWriter, firstMissing)
}

func testKey(writer, index int) []byte {
	return []byte(fmt.Sprintf("%02d-%04d", writer, index))
}

// newTempStore returns a store backed by a Badger database living in a temporary directory
// cleaned up when the test ends, no external dependency is required to run against it.
func newTempStore(t *testing.T) store.KVStore {
	t.Helper()

	kvStore, err := store.New("badger://" + path.Join(t.TempDir(), "race.db"))
	require.NoError(t, err)
	t.Cleanup(func() {
		kvStore.(io.Closer).Close()
	})

	return kvStore
}
