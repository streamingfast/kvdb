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
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/golang/protobuf/proto"
)

// B is a shortcut for (must) hex.DecodeString
var B = func(s string) []byte {
	out, err := hex.DecodeString(s)
	if err != nil {
		panic(err)
	}

	return out
}

// H is a shortcut for hex.EncodeToString
var H = hex.EncodeToString

func MustProtoMarshal(obj proto.Message) []byte {
	bytes, err := proto.Marshal(obj)
	if err != nil {
		panic(fmt.Errorf("should be able to marshal proto: %s", err))
	}

	return bytes
}

func BoolToByte(value bool) byte {
	if value {
		return byte(1)
	}
	return byte(0)
}

func Uint64ToBytes(value uint64) []byte {
	out := make([]byte, 8)
	bigEndian.PutUint64(out, value)

	return out
}

func StringListToBytes(value []string, separator string) []byte {
	joined := strings.Join(value, separator)

	return []byte(joined)
}

func ByteToBool(value []byte) bool {
	if len(value) <= 0 {
		return false
	}

	return value[0] != 0
}

func BlockNum(blockID string) uint32 {
	if len(blockID) < 8 {
		return 0
	}
	bin, err := hex.DecodeString(blockID[:8])
	if err != nil {
		return 0
	}
	return bigEndian.Uint32(bin)
}

var bigEndian = binary.BigEndian

// increaseBlockIDSuffix increases the last bits of the blockID, to
// make it to the next block in an InfiniteRange search. WARN: if the
// last characters are: "ffffffff", then it will overflow, and return
// a *lower* key.
func IncreaseBlockIDSuffix(blockID string) string {
	suffix := blockID[len(blockID)-8:]
	bin, err := hex.DecodeString(suffix)
	if err != nil {
		return blockID
	}
	lastBits := bigEndian.Uint32(bin)
	lastBits++
	return blockID[:len(blockID)-8] + fmt.Sprintf("%08x", lastBits)
}

func HexRevBlockNum(blockNum uint32) string {
	return HexUint32(math.MaxUint32 - blockNum)
}

func HexRevBlockNum64(blockNum uint64) string {
	return HexUint64(math.MaxUint64 - blockNum)
}

func FromRevBlockNum64(input string) (uint64, error) {
	val, err := strconv.ParseUint(input, 16, 64)
	if err != nil {
		return 0, err
	}

	return math.MaxUint64 - uint64(val), nil
}

func HexName(name uint64) string {
	return fmt.Sprintf("%016x", name)
}

func HexUint16(input uint16) string {
	return fmt.Sprintf("%04x", input)
}

func FromHexUint16(input string) (uint16, error) {
	val, err := strconv.ParseUint(input, 16, 16)
	if err != nil {
		return 0, err
	}
	return uint16(val), nil
}

func HexUint32(input uint32) string {
	return fmt.Sprintf("%08x", input)
}

func HexUint64(input uint64) string {
	return fmt.Sprintf("%016x", input)
}

func FromHexUint64(input string) (uint64, error) {
	val, err := strconv.ParseUint(input, 16, 64)
	if err != nil {
		return 0, err
	}

	return uint64(val), nil
}

func ReversedBlockID(blockID string) string {
	blockNum := BlockNum(blockID)
	return HexRevBlockNum(blockNum) + blockID[8:]
}

func ReversedUint16(input uint16) uint16 {
	return math.MaxUint16 - input
}

// BlockIdentifier is present only for testing purposes. This
// wrap a string of the format `<Num><letter>` and add block ref
// methods `Num` and `ID`. This way in tests, it's easy to create
// block identifier for example `1a` or `3c` to represents blocks
// num + id consicely.
type BlockIdentifier string

func (i BlockIdentifier) ID() string { return string(i) }
func (i BlockIdentifier) Num() (out uint64) {
	out, _ = strconv.ParseUint(string(i)[0:1], 10, 64)
	return
}
