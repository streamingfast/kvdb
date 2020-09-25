package bigkv

import (
	"io"
	"os"
	"testing"

	"github.com/dfuse-io/logging"

	"github.com/dfuse-io/kvdb/store"
	"github.com/dfuse-io/kvdb/store/storetest"
	"github.com/stretchr/testify/require"
)

func init() {
	logging.TestingOverride()
}

func TestAll(t *testing.T) {
	if os.Getenv("TEST_BIGKV") == "" {
		t.Skip("To run those tests, you need to have TEST_BIGKV environment variable set and bigtable emulator `gcloud beta emulators bigtable start`")
		return
	}

	storetest.TestAll(t, "bigkv", newTestFactory(t))
}

func newTestFactory(t *testing.T) storetest.DriverFactory {
	return func(opts ...store.Option) (store.KVStore, storetest.DriverCleanupFunc) {
		kvStore, err := store.New("bigkv://dev.dev/dev?createTable=true", opts...)
		if err != nil {
			t.Skip("bigtable unreachable, cannot run tests") // FIXME: this just times out
			return nil, nil
		}
		require.NoError(t, err)
		return kvStore, func() {
			kvStore.(io.Closer).Close()
		}
	}
}
