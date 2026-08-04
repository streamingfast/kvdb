package netkv

import (
	"context"
	"fmt"
	"io"
	"path"
	"sync"
	"testing"

	"github.com/streamingfast/kvdb/store"
	_ "github.com/streamingfast/kvdb/store/badger"
	netkvserver "github.com/streamingfast/kvdb/store/netkv/server"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConcurrentPut ensures that concurrent `Put` calls on a single store do not corrupt the
// pending write batch. It is meant to be run with `-race` but even without it, a lost `append`
// on the shared batch slice makes the final entry count differ from what was written.
func TestConcurrentPut(t *testing.T) {
	const (
		writerCount   = 16
		putsPerWriter = 64
	)

	ctx := context.Background()
	kvStore := newServedStore(t, ":65113")

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

// newServedStore starts an in-process netkv server backed by a Badger database living in a
// temporary directory and returns a netkv store connected to it, no external dependency is
// required to run against it.
func newServedStore(t *testing.T, listenAddr string) store.KVStore {
	t.Helper()

	server, err := netkvserver.Launch(listenAddr, "badger://"+path.Join(t.TempDir(), "race.db"))
	require.NoError(t, err)
	t.Cleanup(func() {
		server.Close()
	})

	kvStore, err := store.New(fmt.Sprintf("netkv://localhost%s?insecure=true", listenAddr))
	require.NoError(t, err)
	t.Cleanup(func() {
		kvStore.(io.Closer).Close()
	})

	return kvStore
}
