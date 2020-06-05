package storetest

import (
	"os"
	"testing"

	"github.com/dfuse-io/kvdb/store"
)

var debug = false

func init() {
	debug = os.Getenv("DEBUG") != ""
}

type DriverCleanupFunc func()
type DriverFactory func() (store.KVStore, DriverCleanupFunc)

func TestAll(t *testing.T, driverName string, driverFactory DriverFactory) {
	TestAllKVStore(t, driverName, driverFactory)
}
