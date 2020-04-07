package storetest

import (
	"context"
	"fmt"
	"testing"

	"github.com/dfuse-io/kvdb/store"
	"github.com/stretchr/testify/require"
)

var kvstoreTests = []struct {
	name string
	test func(t *testing.T, driver store.KVStore)
}{
	{"basic", TestBasic},
}

func TestAllKVStore(t *testing.T, driverName string, driverFactory DriverFactory) {
	for _, rt := range kvstoreTests {
		t.Run(driverName+"/"+rt.name, func(t *testing.T) {
			driver, closer := driverFactory()
			defer closer()
			rt.test(t, driver)
		})
	}
}

func TestBasic(t *testing.T, driver store.KVStore) {
	all := []store.KV{
		{Key: []byte("a"), Value: []byte("1")},
		{Key: []byte("ba"), Value: []byte("2")},
		{Key: []byte("ba1"), Value: []byte("3")},
		{Key: []byte("ba2"), Value: []byte("4")},
		{Key: []byte("bb"), Value: []byte("5")},
		{Key: []byte("c"), Value: []byte("6")},
	}

	// testing PUT function
	for _, kv := range all {
		err := driver.Put(context.Background(), kv.Key, kv.Value)
		require.NoError(t, err)
	}

	// testing GET without a flush  //// SKIP: Some backends still flush.
	// for _, kv := range all {
	// 	// testing GET function
	// 	_, err := driver.Get(context.Background(), kv.Key)
	// 	require.Equal(t, kvdb.ErrNotFound, err)
	// }

	// testing Flush Put
	err := driver.FlushPuts(context.Background())
	require.NoError(t, err)

	// testing GET with a flush
	for _, kv := range all {
		// testing GET function
		v, err := driver.Get(context.Background(), kv.Key)
		require.NoError(t, err)
		require.Equal(t, kv.Value, v)
	}

	// None existant key
	_, err = driver.Get(context.Background(), []byte("keydoesnotexists"))
	require.Equal(t, store.ErrNotFound, err)

	// testing Prefix
	testPrefix(t, driver, nil, all)
	testPrefix(t, driver, []byte("a"), all[:1])
	testPrefix(t, driver, []byte("c"), all[5:])
	testPrefix(t, driver, []byte("b"), all[1:5])
	testPrefix(t, driver, []byte("ba"), all[1:4])

	// testing Scan without limit
	//testScan(t, driver, nil, nil, 0, all)
	testScan(t, driver, []byte("a"), []byte("a"), 0, nil)
	testScan(t, driver, []byte("a"), []byte("b"), 0, all[:1])
	testScan(t, driver, []byte("b"), []byte("a"), 0, nil)
	testScan(t, driver, []byte("b"), []byte("bb"), 0, all[1:4])
	testScan(t, driver, []byte("b"), []byte("c"), 0, all[1:5])
	testScan(t, driver, []byte("a"), []byte("c"), 0, all[:5])
	testScan(t, driver, []byte("ba"), []byte("bb"), 0, all[1:4])
	testScan(t, driver, nil, nil, 0, nil)
	testScan(t, driver, testStringsToKey(""), testStringsToKey(""), 0, nil)

	testScan(t, driver, nil, testStringsToKey("c"), 0, all[:5])
	testScan(t, driver, []byte(""), testStringsToKey("c"), 0, all[:5])
	testScan(t, driver, []byte("b"), nil, 0, nil)
	testScan(t, driver, []byte("b"), testStringsToKey(""), 0, nil)
}

func testPrefix(t *testing.T, driver store.KVStore, prefix []byte, exp []store.KV) {
	var got []store.KV
	itr := driver.Prefix(context.Background(), prefix)
	for itr.Next() {
		got = append(got, *itr.Item())
	}
	testPrintKVs(fmt.Sprintf("test prefix with prefix %q", string(prefix)), got)
	require.NoError(t, itr.Err())
	require.Equal(t, exp, got)
}

func testScan(t *testing.T, driver store.KVStore, start, end []byte, limit int, exp []store.KV) {
	var got []store.KV
	itr := driver.Scan(context.Background(), start, end, limit)
	for itr.Next() {
		got = append(got, *itr.Item())
	}

	testPrintKVs(fmt.Sprintf("test scan with start %q and end %q", string(start), string(end)), got)
	require.NoError(t, itr.Err())
	require.Equal(t, exp, got)
}

func testStringsToKey(parts ...string) (out []byte) {
	for _, s := range parts {
		out = append(out, []byte(s)...)
	}
	return out
}

func testPrintKVs(title string, out []store.KV) {
	//fmt.Printf("%s\n", title)
	//for _, kv := range out {
	//	fmt.Printf("- %s => %s\n", string(kv.Key), string(kv.Value))
	//}
}
