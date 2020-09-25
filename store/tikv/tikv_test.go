package tikv

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

var tikvDSN = os.Getenv("TEST_TIKV")

func init() {
	logging.TestingOverride()
}

func TestAll(t *testing.T) {
	if tikvDSN == "" {
		t.Skip("To run those tests, you need to have TEST_TIKV environment variable set pointing to a TiKV cluster (like `TEST_TIKV=tikv://127.0.0.1:2379/data`)")
		return
	}

	storetest.TestAll(t, "tikv", newTestFactory(t, tikvDSN))
}

func newTestFactory(t *testing.T, dsn string) storetest.DriverFactory {
	return func(opts ...store.Option) (store.KVStore, storetest.DriverCleanupFunc) {
		// Auto add a prefix to the path if asking for it
		generator := rand.New(rand.NewSource(time.Now().UnixNano()))
		dsn = strings.ReplaceAll(dsn, "{prefix}", fmt.Sprintf("%x", generator.Int()))

		kvStore, err := store.New(dsn, opts...)
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
