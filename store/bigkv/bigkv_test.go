package bigkv

import (
	"fmt"
	"io"
	"math/rand"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/dfuse-io/kvdb/store"
	"github.com/dfuse-io/kvdb/store/storetest"
	"github.com/dfuse-io/logging"
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
	return func(opts ...store.Option) (store.KVStore, *storetest.DriverCapabilities, storetest.DriverCleanupFunc) {
		dsn := "bigkv://dev.dev/dev-{prefix}?createTable=true"

		generator := rand.New(rand.NewSource(time.Now().UnixNano()))
		dsn = strings.ReplaceAll(dsn, "{prefix}", fmt.Sprintf("%x", generator.Int()))

		kvStore, err := store.New(dsn, opts...)
		if err != nil {
			t.Skip("bigtable unreachable, cannot run tests") // FIXME: this just times out
			return nil, nil, nil
		}
		require.NoError(t, err)
		return kvStore, storetest.NewDriverCapabilities(), func() {
			kvStore.(io.Closer).Close()
		}
	}
}
