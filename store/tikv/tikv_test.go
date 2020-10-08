package tikv

import (
	"fmt"
	"io"
	"math/rand"
	"net/url"
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

	parsedDSN, err := url.Parse(tikvDSN)
	require.NoError(t, err)

	rawDSN, err := url.PathUnescape(parsedDSN.String())
	require.NoError(t, err)

	// We use a low scan limit so we ensure that it passes through multiple loops in tikv scanning routine
	storetest.TestAll(t, "tikv", newTestFactory(t, 1, rawDSN+"?tikv_raw_max_scan_limit=2"))

	// FIXME: This causes too much coupling with actual tests implementations and knownledge about what it requires.
	//        Ideally, the storetest package would be able to create the store with compression (and its config) by
	//        itself.
	storetest.TestAll(t, "tikv/compression", newTestFactory(t, 2, rawDSN+"?compression=zstd&compression_size_threshold=25"))
}

func newTestFactory(t *testing.T, seed int64, dsn string) storetest.DriverFactory {
	return func(opts ...store.Option) (store.KVStore, *storetest.DriverCapabilities, storetest.DriverCleanupFunc) {
		// Auto add a prefix to the path if asking for it
		generator := rand.New(rand.NewSource(time.Now().UnixNano() + seed))
		dsn = strings.ReplaceAll(dsn, "{prefix}", fmt.Sprintf("%x", generator.Int()))

		kvStore, err := store.New(dsn, opts...)
		if err != nil {
			t.Skip(fmt.Errorf("pd0 unreachable, cannot run tests: %w", err)) // FIXME: this just times out
			return nil, nil, nil
		}
		require.NoError(t, err)

		capabilities := storetest.NewDriverCapabilities()
		capabilities.SupportsEmptyValue = false

		return kvStore, capabilities, func() {
			kvStore.(io.Closer).Close()
		}
	}
}
