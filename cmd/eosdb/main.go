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

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/dfuse-io/kvdb/eosdb"
	_ "github.com/dfuse-io/kvdb/eosdb/bigt"
	pbdeos "github.com/dfuse-io/pbgo/dfuse/codecs/deos"
)

func main() {
	zlog.Info("starting")

	target := "bigtable://dev.dev/eosdb"
	// target := "mysql://root:@(127.0.0.1:4000)/"
	// target := "sqlite3:///tmp/bob"
	// target := "sqlite3://:mem:"
	if len(os.Args) > 1 {
		target = os.Args[1]
	}

	db, err := eosdb.New(target)
	if err != nil {
		log.Fatalln("Error loading client:", err)
	}

	evts, err := db.GetTransactionEvents(
		context.Background(),
		"7d0455375d87308385d818145b5d496c816d73965f570511ac7c9655d1f7e8b4",
	)
	check("get trx", err)

	trx := pbdeos.MergeTransactionEvents(evts, func(id string) bool { return true })

	trxJSON, err := json.MarshalIndent(trx, "", "  ")
	check("marshal", err)

	fmt.Println(string(trxJSON))
}

func check(prefix string, err error) {
	if err != nil {
		fmt.Printf("%s: %s\n", prefix, err)
		os.Exit(1)
	}
}
