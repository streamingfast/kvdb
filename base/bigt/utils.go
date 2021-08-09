// Copyright 2019 dfuse Platform Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package basebigt

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"

	"cloud.google.com/go/bigtable"
	"github.com/golang/protobuf/proto"
	"github.com/streamingfast/kvdb"
)

func IsEmptyRow(row bigtable.Row) bool {
	return len(row) <= 0
}

func rowItem(row bigtable.Row, familyColumn string) (out *bigtable.ReadItem, ok bool) {
	for _, cols := range row {
		for _, el := range cols {
			if el.Column == familyColumn {
				return &el, true
			}
		}
	}

	return nil, false
}

func ColumnItem(row bigtable.Row, familyColumn string) (item *bigtable.ReadItem, present bool) {
	return rowItem(row, familyColumn)
}

func BoolColumnItem(row bigtable.Row, familyColumn string) (bool, error) {
	item, present := ColumnItem(row, familyColumn)
	if !present {
		return false, NewErrColumnNotPresent(familyColumn)
	}

	return kvdb.ByteToBool(item.Value), nil
}

func StringColumnItem(row bigtable.Row, familyColumn string) (string, error) {
	item, present := ColumnItem(row, familyColumn)
	if !present {
		return "", NewErrColumnNotPresent(familyColumn)
	}
	return string(item.Value), nil
}

func StringListColumnItem(row bigtable.Row, familyColumn string, separator string) ([]string, error) {
	item, present := ColumnItem(row, familyColumn)
	if !present {
		return nil, NewErrColumnNotPresent(familyColumn)
	}

	// Since `strings.Split()` returns `[]string{""}`` when value is `""`, return empty array right away when there is no value
	if len(item.Value) == 0 {
		return nil, nil
	}

	return strings.Split(string(item.Value), separator), nil
}

func JSONColumnItem(row bigtable.Row, familyColumn string, v interface{}) error {
	item, present := ColumnItem(row, familyColumn)
	if !present {
		return NewErrColumnNotPresent(familyColumn)
	}

	value := item.Value
	if len(value) == 0 {
		return fmt.Errorf("empty value in column %q", familyColumn)
	}

	err := json.Unmarshal(value, v)
	if err != nil {
		return fmt.Errorf("unmarhalling error in column %q: %w", familyColumn, err)
	}

	return nil
}

func Uint64ColumnItem(row bigtable.Row, familyColumn string) (uint64, error) {
	item, present := ColumnItem(row, familyColumn)
	if !present {
		return 0, NewErrColumnNotPresent(familyColumn)
	}

	return binary.BigEndian.Uint64(item.Value), nil
}

// Column Reader interface
//
// This is a try to reduce code by reading column item value type
// more generically.

type ColumnReader interface {
	Read(row bigtable.Row, familyColumn string) (interface{}, error)
}

type ColumnReaderFunc func(row bigtable.Row, familyColumn string) (interface{}, error)

func (r ColumnReaderFunc) Read(row bigtable.Row, familyColumn string) (interface{}, error) {
	return r(row, familyColumn)
}

var BytesColumnReader = ColumnReaderFunc(func(row bigtable.Row, familyColumn string) (interface{}, error) {
	item, present := ColumnItem(row, familyColumn)
	if !present {
		return false, NewErrColumnNotPresent(familyColumn)
	}

	return item.Value, nil
})

var BigIntColumnReader = ColumnReaderFunc(func(row bigtable.Row, familyColumn string) (interface{}, error) {
	bytes, err := BytesColumnReader(row, familyColumn)
	if err != nil {
		return nil, err
	}

	return new(big.Int).SetBytes(bytes.([]byte)), nil
})

var Uint64ColumnReader = ColumnReaderFunc(func(row bigtable.Row, familyColumn string) (interface{}, error) {
	return Uint64ColumnItem(row, familyColumn)
})

// ProtoColumnItem turns the familyColumn on row into a proper unmarshalled Proto
// structure. The `protoResolver` exists so that it's possible to implement lazy loading
// of the actual proto structure to implement. Instead of directly passing the proto,
// you pass a resolver function that returns the proto to unserialized. Since the
// resolver is called only when the actual row data exists, we save useless deallocation
// at the expense of an extra function call.
func ProtoColumnItem(row bigtable.Row, familyColumn string, protoResolver func() proto.Message) error {
	item, present := ColumnItem(row, familyColumn)
	if !present {
		return NewErrColumnNotPresent(familyColumn)
	}

	value := item.Value
	if len(value) == 0 {
		return NewErrEmptyValue(familyColumn)
	}

	out := protoResolver()
	err := proto.Unmarshal(value, out)
	if err != nil {
		return fmt.Errorf("unmarshalling error in column %q: %w", familyColumn, err)
	}

	return nil
}
