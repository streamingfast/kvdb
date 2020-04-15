package badger

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/dfuse-io/kvdb/store"
	"github.com/dfuse-io/kvdb/store/storetest"
	"github.com/stretchr/testify/require"
)

func TestAll(t *testing.T) {
	storetest.TestAll(t, "Badger", newTestBadgerFactory(t, "badger-test.db"))
}

func newTestBadgerFactory(t *testing.T, testDBFilename string) storetest.DriverFactory {
	return func() (store.KVStore, storetest.DriverCleanupFunc) {
		dir, err := ioutil.TempDir("", "kvdb-badger")
		require.NoError(t, err)
		dsn := fmt.Sprintf("badger://%s", path.Join(dir, "flux.db"))
		kvStore, err := NewStore(dsn)
		require.NoError(t, err)
		return kvStore, func() {
			err := os.RemoveAll(dir)
			require.NoError(t, err)
		}
	}
}