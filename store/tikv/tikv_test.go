package tikv

import (
	"fmt"
	"io"
	"os"
	"testing"

	"github.com/dfuse-io/kvdb/store"
	"github.com/dfuse-io/kvdb/store/storetest"
	"github.com/stretchr/testify/require"
)

func TestAll(t *testing.T) {
	if os.Getenv("TEST_TIKV") == "" {
		t.Skip("To run those tests, you need to have TEST_TIKV environment variable set")
		return
	}

	storetest.TestAll(t, "tikv", newTestFactory(t), false)
}

func newTestFactory(t *testing.T) storetest.DriverFactory {
	return func(opts ...store.Option) (store.KVStore, storetest.DriverCleanupFunc) {
		kvStore, err := store.New("tikv://localhost:2379/data", opts...)
		if err != nil {
			t.Skip(fmt.Errorf("pd0 unreachable, cannot run tests: %w", err)) // FIXME: this just times out
			return nil, nil
		}
		require.NoError(t, err)
		return kvStore, func() {
			kvStore.(io.Closer).Close()
		}
	}
}
