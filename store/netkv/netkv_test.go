package netkv

import (
	"fmt"
	"io/ioutil"
	"path"
	"testing"
	"time"

	"github.com/streamingfast/logging"
	"github.com/streamingfast/kvdb/store"
	_ "github.com/streamingfast/kvdb/store/badger"
	netkvserver "github.com/streamingfast/kvdb/store/netkv/server"
	"github.com/streamingfast/kvdb/store/storetest"
	"github.com/stretchr/testify/require"
)

func init() {
	logging.TestingOverride()
}

func TestAll(t *testing.T) {
	storetest.TestAll(t, "NetKV", newTestNetKVFactory(t))
}

func newTestNetKVFactory(t *testing.T) storetest.DriverFactory {
	return func(opts ...store.Option) (store.KVStore, *storetest.DriverCapabilities, storetest.DriverCleanupFunc) {
		// Start a server
		dir, err := ioutil.TempDir("", "kvdb-netkv-server")
		require.NoError(t, err)
		dsn1 := fmt.Sprintf("badger://%s", path.Join(dir, "netkv"))
		server, err := netkvserver.Launch(":65112", dsn1)
		require.NoError(t, err)
		time.Sleep(100 * time.Millisecond)

		// Setup the `netkv` client, and test it.
		dsn2 := fmt.Sprintf("netkv://localhost:65112?insecure=true")
		kvStore, err := store.New(dsn2, opts...)
		require.NoError(t, err)

		return kvStore, storetest.NewDriverCapabilities(), func() {
			server.Close()
			time.Sleep(100 * time.Millisecond)

		}
	}
}
