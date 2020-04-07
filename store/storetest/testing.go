package storetest

import (
	"testing"

	"github.com/dfuse-io/kvdb/store"
)

type DriverCleanupFunc func()
type DriverFactory func() (store.KVStore, DriverCleanupFunc)

func TestAll(t *testing.T, driverName string, driverFactory DriverFactory) {
	TestAllKVStore(t, driverName, driverFactory)
}
