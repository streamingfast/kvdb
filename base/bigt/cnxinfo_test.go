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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDSNParser(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expect      DSN
		expectError string
	}{
		{
			name:   "happy",
			input:  "bigtable://project.instance/tbl?createTables=true",
			expect: DSN{Project: "project", Instance: "instance", TablePrefix: "tbl", CreateTables: true, MaxBlocksBeforeFlush: 10, MaxDurationBeforeFlush: 10 * time.Second},
		},
		{
			name:   "specificFlushingProps",
			input:  "bigtable://project.instance/tbl?max-blocks-before-flush=50&max-seconds-before-flush=5",
			expect: DSN{Project: "project", Instance: "instance", TablePrefix: "tbl", CreateTables: false, MaxBlocksBeforeFlush: 50, MaxDurationBeforeFlush: 5 * time.Second},
		},
		{
			name:        "empty table prefix",
			input:       "bigtable://project.instance",
			expectError: "dsn: invalid tablePrefix (in path segment), cannot be empty",
		},
		{
			name:        "too many table prefixes",
			input:       "bigtable://project.instance/tbl/useless",
			expectError: "dsn: path component invalid, should only have tablePrefix in there",
		},
		{
			name:        "invalid host short",
			input:       "bigtable://project/tbl",
			expectError: "dsn: invalid, ensure host component looks like 'project.instance'",
		},
		{
			name:        "invalid host long",
			input:       "bigtable://project.instance.whatever/tbl",
			expectError: "dsn: invalid, ensure host component looks like 'project.instance'",
		},
		{
			name:        "malformed createTables",
			input:       "bigtable://project.instance/tbl?createTables=boo",
			expectError: "dsn: invalid parameter for createTables, use true or false",
		},
		{
			name:        "invalid max-blocks",
			input:       "bigtable://project.instance/tbl?max-blocks-before-flush=r2d2",
			expectError: "dsn: invalid parameter for max-blocks-before-flush, strconv.ParseUint: parsing \"r2d2\": invalid syntax",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dsn, err := ParseDSN(test.input)
			if test.expectError == "" {
				assert.NoError(t, err)
				assert.Equal(t, test.expect, *dsn)
			} else {
				assert.Error(t, err)
				assert.Equal(t, test.expectError, err.Error())
			}
		})
	}
}
