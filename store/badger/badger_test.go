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
	storetest.TestAll(t, "Badger", NewTestBadgerFactory(t, "badger-test.db"), true)
}

func NewTestBadgerFactory(t *testing.T, testDBFilename string) storetest.DriverFactory {
	return func(opts ...store.Option) (store.KVStore, storetest.DriverCleanupFunc) {
		dir, err := ioutil.TempDir("", "kvdb-badger")
		require.NoError(t, err)
		dsn := fmt.Sprintf("badger://%s", path.Join(dir, testDBFilename))
		kvStore, err := store.New(dsn, opts...)
		require.NoError(t, err)
		return kvStore, func() {
			err := os.RemoveAll(dir)
			require.NoError(t, err)
		}
	}
}
