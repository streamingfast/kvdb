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
	"fmt"
	"testing"

	"cloud.google.com/go/bigtable"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStringListColumnItem(t *testing.T) {
	tests := []struct {
		columnValue  []byte
		expectedList []string
		expectedErr  error
	}{
		{nil, nil, nil},
		{[]byte{}, nil, nil},
		{[]byte("a"), []string{"a"}, nil},
		{[]byte("a:b:c"), []string{"a", "b", "c"}, nil},
		{[]byte("a::c"), []string{"a", "", "c"}, nil},
		{[]byte(":ac:"), []string{"", "ac", ""}, nil},
		{[]byte("a,b,c"), []string{"a,b,c"}, nil},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			row := bigtable.Row{}
			row["any"] = []bigtable.ReadItem{{
				Column: "test",
				Value:  test.columnValue,
			}}

			actual, err := StringListColumnItem(row, "test", ":")
			if test.expectedErr == nil {
				require.NoError(t, err)
				assert.Equal(t, test.expectedList, actual)
			} else {
				assert.Equal(t, test.expectedErr, err)
			}
		})
	}
}
