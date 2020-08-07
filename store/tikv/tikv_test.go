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

var tikvDSN = os.Getenv("TEST_TIKV")

func TestAll(t *testing.T) {
	if tikvDSN == "" {
		t.Skip("To run those tests, you need to have TEST_TIKV environment variable set pointing to a TiKV cluster (like `TEST_TIKV=tikv://127.0.0.1:2379/data`)")
		return
	}

	storetest.TestAll(t, "tikv", newTestFactory(t, tikvDSN))
}

func newTestFactory(t *testing.T, dsn string) storetest.DriverFactory {
	return func(opts ...store.Option) (store.KVStore, storetest.DriverCleanupFunc) {
		kvStore, err := store.New(tikvDSN, opts...)
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
