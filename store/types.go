package store

import (
	"encoding/hex"
	"strconv"
)

const Unlimited = 0

type KV struct {
	Key, Value []byte
}

func (kv *KV) Size() int {
	if kv == nil {
		return 0
	}

	return len(kv.Key) + len(kv.Value)
}

type Key []byte

func (k Key) String() string {
	return hex.EncodeToString(k)
}

// Next returns the next key in byte-order.
//
// Copied from https://github.com/tikv/client-go/blob/master/key/key.go
func (k Key) Next() Key {
	// add 0x0 to the end of key
	buf := make([]byte, len([]byte(k))+1)
	copy(buf, []byte(k))
	return buf
}

// PrefixNext returns the next prefix key.
//
// Assume there are keys like:
//
//   rowkey1
//   rowkey1_column1
//   rowkey1_column2
//   rowKey2
//
// If we seek 'rowkey1' Next, we will get 'rowkey1_column1'.
// If we seek 'rowkey1' PrefixNext, we will get 'rowkey2'.
//
// Copied from https://github.com/tikv/client-go/blob/master/key/key.go
func (k Key) PrefixNext() Key {
	buf := make([]byte, len([]byte(k)))
	copy(buf, []byte(k))
	var i int
	for i = len(k) - 1; i >= 0; i-- {
		buf[i]++
		if buf[i] != 0 {
			break
		}
	}
	if i == -1 {
		copy(buf, k)
		buf = append(buf, 0)
	}
	return buf
}

type Limit int

func (l Limit) Reached(count uint64) bool {
	return l.Bounded() && count >= uint64(l)
}

func (l Limit) Bounded() bool {
	return int(l) > 0
}

func (l Limit) Unbounded() bool {
	return int(l) <= 0
}

func (l Limit) String() string {
	if l.Unbounded() {
		return "unlimited"
	}

	return strconv.FormatInt(int64(l), 10)
}
