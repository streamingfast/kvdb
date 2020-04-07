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
	"net/url"
	"strconv"
	"strings"
	"time"
)

type DSN struct {
	Project                string
	Instance               string
	TablePrefix            string
	CreateTables           bool
	MaxBlocksBeforeFlush   uint64
	MaxDurationBeforeFlush time.Duration
}

func ParseDSN(dsn string) (*DSN, error) {
	// bigtable://project.instance/tblPrefix?createTables=true
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}

	hostParts := strings.Split(u.Host, ".")
	if len(hostParts) != 2 {
		return nil, fmt.Errorf("dsn: invalid, ensure host component looks like 'project.instance'")
	}

	maxSecondsBeforeFlush := uint64(10)
	if qMaxSeconds := u.Query().Get("max-seconds-before-flush"); qMaxSeconds != "" {
		ms, err := strconv.ParseUint(qMaxSeconds, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("dsn: invalid parameter for max-blocks-before-flush, %s", err)
		}
		maxSecondsBeforeFlush = ms
	}

	maxBlocksBeforeFlush := uint64(10)
	if qMaxBlocks := u.Query().Get("max-blocks-before-flush"); qMaxBlocks != "" {
		mb, err := strconv.ParseUint(qMaxBlocks, 10, 32)
		if err != nil {
			return nil, fmt.Errorf("dsn: invalid parameter for max-blocks-before-flush, %s", err)
		}
		maxBlocksBeforeFlush = mb
	}

	create := u.Query().Get("createTables")
	switch create {
	case "":
		create = "false"
	case "true", "false":
	default:
		return nil, fmt.Errorf("dsn: invalid parameter for createTables, use true or false")
	}

	path := strings.Trim(u.Path, "/")
	if len(strings.Split(path, "/")) > 1 {
		return nil, fmt.Errorf("dsn: path component invalid, should only have tablePrefix in there")
	}
	if path == "" {
		return nil, fmt.Errorf("dsn: invalid tablePrefix (in path segment), cannot be empty")
	}

	return &DSN{
		Project:                hostParts[0],
		Instance:               hostParts[1],
		TablePrefix:            path,
		CreateTables:           create == "true",
		MaxBlocksBeforeFlush:   maxBlocksBeforeFlush,
		MaxDurationBeforeFlush: time.Duration(maxSecondsBeforeFlush) * time.Second,
	}, nil
}

// DEPRECATED, use DSN instead
type ConnectionInfo struct {
	Project     string
	Instance    string
	TablePrefix string
}

// Deprecated: Use DSN instead. Explode connection string `connection` into its three parts.
func NewConnectionInfo(connection string) (info *ConnectionInfo, err error) {
	parts := strings.Split(connection, ":")
	if len(parts) != 3 {
		return nil, fmt.Errorf("database connection info should be <project>:<instance>:<prefix>")
	}

	return &ConnectionInfo{Project: parts[0], Instance: parts[1], TablePrefix: parts[2]}, nil
}
