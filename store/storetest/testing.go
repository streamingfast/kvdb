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

type DriverCapabilities struct {
	SupportsEmptyValue bool
}

func NewDriverCapabilities() *DriverCapabilities {
	return &DriverCapabilities{SupportsEmptyValue: true}
}

type DriverCleanupFunc func()
type DriverFactory func(opts ...store.Option) (store.KVStore, *DriverCapabilities, DriverCleanupFunc)

func TestAll(t *testing.T, driverName string, driverFactory DriverFactory) {
	testAllKVStore(t, driverName, driverFactory)
}
