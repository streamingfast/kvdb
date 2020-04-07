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

package kvdb

import (
	"fmt"
	"testing"

	"cloud.google.com/go/bigtable"
	"github.com/stretchr/testify/assert"
)

func printRow(row bigtable.Row) {
	fmt.Println("Printing row")
	for k, v := range row {
		for _, it := range v {
			fmt.Println("Row:", k, it.Timestamp, it.Row, it.Column, string(it.Value))
		}
	}
}

func TestBoolToByte(t *testing.T) {
	assert.Equal(t, byte(1), BoolToByte(true))
	assert.Equal(t, byte(0), BoolToByte(false))
}

func TestByteToBool(t *testing.T) {
	assert.Equal(t, false, ByteToBool([]byte{}))
	assert.Equal(t, false, ByteToBool([]byte{0}))
	assert.Equal(t, true, ByteToBool([]byte{1}))
	assert.Equal(t, false, ByteToBool([]byte{0, 1}))
	assert.Equal(t, true, ByteToBool([]byte{1, 0}))
}

func TestIncreaseBlockIDSUffix(t *testing.T) {
	assert.Equal(t, "00000001deadbef0", IncreaseBlockIDSuffix("00000001deadbeef"))
}

func TestHexUint16(t *testing.T) {
	assert.Equal(t, "0001", HexUint16(1))
	assert.Equal(t, "ffff", HexUint16(65535))
	assert.Equal(t, "fffe", HexUint16(65534))
}
