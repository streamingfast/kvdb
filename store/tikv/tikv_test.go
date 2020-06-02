package tikv

import (
	"io"
	"os"
	"testing"

	"github.com/dfuse-io/kvdb/store"
	"github.com/dfuse-io/kvdb/store/storetest"
	"github.com/stretchr/testify/require"
)

func init() {
	//	if os.Getenv("DEBUG") != "" {
	//logging.Override(logging.MustCreateLoggerWithLevel("test", zap.NewAtomicLevelAt(zap.DebugLevel)))
	//	}
}

func TestAll(t *testing.T) {
	if os.Getenv("TEST_TIKV") != "" {
		storetest.TestAll(t, "tikv", newTestFactory(t))
	}
}

func newTestFactory(t *testing.T) storetest.DriverFactory {
	return func() (store.KVStore, storetest.DriverCleanupFunc) {
		kvStore, err := NewStore("tikv://pd0:2379/helloworld")
		if err != nil {
			t.Skip("pd0 unreachable, cannot run tests") // FIXME: this just times out
			return nil, nil
		}
		require.NoError(t, err)
		return kvStore, func() {
			kvStore.(io.Closer).Close()
		}
	}
}
