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
type DriverFactory func(opts ...store.Option) (store.KVStore, DriverCleanupFunc)

func TestAll(t *testing.T, driverName string, driverFactory DriverFactory,testPurgeableStore bool) {
	TestAllKVStore(t, driverName, driverFactory, testPurgeableStore)
}
