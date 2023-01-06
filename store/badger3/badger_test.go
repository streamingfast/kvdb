package badger3

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/streamingfast/kvdb/store"
	"github.com/streamingfast/kvdb/store/storetest"
	"github.com/streamingfast/logging"
	"github.com/stretchr/testify/require"
)

func init() {
	logging.TestingOverride()
}

func TestAll(t *testing.T) {
	storetest.TestAll(t, "Badger", NewTestBadgerFactory(t, "badger-test.db"))
}

func NewTestBadgerFactory(t *testing.T, testDBFilename string) storetest.DriverFactory {
	return func(opts ...store.Option) (store.KVStore, *storetest.DriverCapabilities, storetest.DriverCleanupFunc) {
		dir, err := ioutil.TempDir("", "kvdb-badger")
		require.NoError(t, err)
		dsn := fmt.Sprintf("badger3://%s", path.Join(dir, testDBFilename))
		kvStore, err := store.New(dsn, opts...)
		require.NoError(t, err)

		return kvStore, storetest.NewDriverCapabilities(), func() {
			err := os.RemoveAll(dir)
			require.NoError(t, err)
		}
	}
}
