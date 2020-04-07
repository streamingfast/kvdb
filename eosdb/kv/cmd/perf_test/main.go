package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/dfuse-io/bstream"
	_ "github.com/dfuse-io/bstream/codecs/deos"
	"github.com/dfuse-io/derr"
	"github.com/dfuse-io/dstore"
	"github.com/dfuse-io/kvdb/eosdb"
	_ "github.com/dfuse-io/kvdb/eosdb/kv"
	_ "github.com/dfuse-io/kvdb/eosdb/sql"
	_ "github.com/dfuse-io/kvdb/store/badger"
	_ "github.com/dfuse-io/kvdb/store/cznickv"
	pbbstream "github.com/dfuse-io/pbgo/dfuse/bstream/v1"
	pbdeos "github.com/dfuse-io/pbgo/dfuse/codecs/deos"
)

func main() {
	driverType := "badger"
	perfTestDBFilePath := "/tmp/perf-test-dfuse-" + driverType + ".db"
	err := os.RemoveAll(perfTestDBFilePath)
	if err != nil && !strings.Contains(err.Error(), "no such file or directory") {
		panic(err)
	}

	dsn := fmt.Sprintf(driverType+"://%s", perfTestDBFilePath)
	db, err := eosdb.New(dsn)
	if err != nil {
		panic(err)
	}
	derr.Check("new eosdb", err)

	chainID, _ := hex.DecodeString("aca376f206b8fc25a6ed44dbdc66547c36c6c33e3a119ffbeaef943642f0e906")
	db.SetWriterChainID(chainID)

	blockStore, err := dstore.NewDBinStore("gs://example/")
	derr.Check("block store", err)

	ctx := context.Background()

	blockCount := 0
	ticks := 0
	totalBlockCount := 0
	start := time.Now()
	fs := bstream.NewFileSource(pbbstream.Protocol_EOS, blockStore, 112900000, 2, nil, bstream.HandlerFunc(func(blk *bstream.Block, obj interface{}) error {
		err := db.PutBlock(ctx, blk.ToNative().(*pbdeos.Block))
		if err != nil {
			return fmt.Errorf("handle block: %w", err)
		}
		blockCount++
		totalBlockCount++
		if time.Since(start) > 1*time.Second {
			fmt.Printf("block per sec %d - @%d\n", blockCount, blk.Number)
			start = time.Now()

			if totalBlockCount >= 1000 {
				return fmt.Errorf("stopping processing")
			}
			ticks++
			blockCount = 0
		}

		return nil
	}))
	time.Now().UnixNano()
	fs.Run()
	db.Flush(ctx)
	fmt.Printf("total ticks: %d --- avg block/s %.2f\n", ticks, float64(totalBlockCount/ticks))
	fmt.Println("done")
}
