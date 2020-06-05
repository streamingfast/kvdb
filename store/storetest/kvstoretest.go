package storetest

import (
	"context"
	"fmt"
	"strings"
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

	// testing Prefix without limit
	testPrefix(t, driver, nil, store.Unlimited, all)
	testPrefix(t, driver, []byte("a"), store.Unlimited, all[:1])
	testPrefix(t, driver, []byte("c"), store.Unlimited, all[5:])
	testPrefix(t, driver, []byte("b"), store.Unlimited, all[1:5])
	testPrefix(t, driver, []byte("ba"), store.Unlimited, all[1:4])

	// testing Prefix with limit
	testPrefix(t, driver, nil, 2, all[:2])
	testPrefix(t, driver, nil, 5, all[:5])
	testPrefix(t, driver, nil, 10, all)

	testPrefix(t, driver, []byte("a"), 2, all[:1])
	testPrefix(t, driver, []byte("c"), 1, all[5:6])
	testPrefix(t, driver, []byte("b"), 3, all[1:4])
	testPrefix(t, driver, []byte("ba"), 10, all[1:4])

	// testing BatchPrefix without limit
	testBatchPrefix(t, driver, [][]byte{[]byte("ba")}, store.Unlimited, all[1:4]...)
	testBatchPrefix(t, driver, [][]byte{[]byte("ba"), []byte("c")}, store.Unlimited, all[1], all[2], all[3], all[5])
	testBatchPrefix(t, driver, [][]byte{[]byte("a"), []byte("c")}, store.Unlimited, all[0], all[5])
	testBatchPrefix(t, driver, [][]byte{[]byte("d"), []byte("f")}, store.Unlimited)

	// testing BatchPrefix with limit
	testBatchPrefix(t, driver, [][]byte{[]byte("ba")}, 1, all[1])
	testBatchPrefix(t, driver, [][]byte{[]byte("ba"), []byte("c")}, 2, all[1], all[2])
	testBatchPrefix(t, driver, [][]byte{[]byte("a"), []byte("c")}, 1, all[0])
	testBatchPrefix(t, driver, [][]byte{[]byte("d"), []byte("f")}, 10)

	// testing Scan without limit
	testScan(t, driver, []byte("a"), []byte("a"), store.Unlimited, nil)
	testScan(t, driver, []byte("a"), []byte("b"), store.Unlimited, all[:1])
	testScan(t, driver, []byte("b"), []byte("a"), store.Unlimited, nil)
	testScan(t, driver, []byte("b"), []byte("bb"), store.Unlimited, all[1:4])
	testScan(t, driver, []byte("b"), []byte("c"), store.Unlimited, all[1:5])
	testScan(t, driver, []byte("a"), []byte("c"), store.Unlimited, all[:5])
	testScan(t, driver, []byte("ba"), []byte("bb"), store.Unlimited, all[1:4])
	testScan(t, driver, nil, nil, store.Unlimited, nil)
	testScan(t, driver, testStringsToKey(""), testStringsToKey(""), store.Unlimited, nil)

	testScan(t, driver, nil, testStringsToKey("c"), store.Unlimited, all[:5])
	testScan(t, driver, []byte(""), testStringsToKey("c"), store.Unlimited, all[:5])
	testScan(t, driver, []byte("b"), nil, store.Unlimited, nil)
	testScan(t, driver, []byte("b"), testStringsToKey(""), store.Unlimited, nil)

	// testing scan with limit
	testScan(t, driver, []byte("a"), []byte("a"), 100, nil)
	testScan(t, driver, []byte("a"), []byte("b"), 1, all[:1])
	testScan(t, driver, []byte("b"), []byte("a"), 10, nil)
	testScan(t, driver, []byte("b"), []byte("bb"), 1, all[1:2])
	testScan(t, driver, []byte("b"), []byte("bb"), 2, all[1:3])
	testScan(t, driver, []byte("b"), []byte("bb"), 3, all[1:4])
	testScan(t, driver, []byte("b"), []byte("bb"), 4, all[1:4])
	testScan(t, driver, nil, nil, 100, nil)
	testScan(t, driver, testStringsToKey(""), testStringsToKey(""), 10, nil)

	testScan(t, driver, nil, testStringsToKey("c"), 1, all[:1])
	testScan(t, driver, []byte(""), testStringsToKey("c"), 3, all[:3])
	testScan(t, driver, []byte("b"), nil, 1, nil)
	testScan(t, driver, []byte("b"), testStringsToKey(""), 1, nil)
}

func testPrefix(t *testing.T, driver store.KVStore, prefix []byte, limit int, exp []store.KV) {
	var got []store.KV
	itr := driver.Prefix(context.Background(), prefix, limit)
	for itr.Next() {
		got = append(got, itr.Item())
	}
	testPrintKVs(fmt.Sprintf("test prefix with prefix %q", string(prefix)), got)
	require.NoError(t, itr.Err())
	require.Equal(t, exp, got)
}

func testBatchPrefix(t *testing.T, driver store.KVStore, prefixes [][]byte, limit int, exp ...store.KV) {
	var got []store.KV
	itr := driver.BatchPrefix(context.Background(), prefixes, limit)
	for itr.Next() {
		got = append(got, itr.Item())
	}

	stringPrefixes := make([]string, len(prefixes))
	for i, prefix := range prefixes {
		stringPrefixes[i] = string(prefix)
	}

	testPrintKVs(fmt.Sprintf("test prefixes with prefix %q", strings.Join(stringPrefixes, ", ")), got)
	require.NoError(t, itr.Err())
	require.Equal(t, exp, got)
}

func testScan(t *testing.T, driver store.KVStore, start, end []byte, limit int, exp []store.KV) {
	var got []store.KV
	itr := driver.Scan(context.Background(), start, end, limit)
	for itr.Next() {
		got = append(got, itr.Item())
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
	if debug {
		fmt.Printf("%s\n", title)
		for _, kv := range out {
			fmt.Printf("- %s => %s\n", string(kv.Key), string(kv.Value))
		}
	}
}
