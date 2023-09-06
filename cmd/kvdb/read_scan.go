package main

import (
	"fmt"

	"github.com/streamingfast/kvdb/cmd/kvdb/decoder"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"

	"github.com/spf13/cobra"
	. "github.com/streamingfast/cli"
	"go.uber.org/zap"
)

var ReadScanCmd = Command(readScanRunE,
	"scan <start-key> <exclusive-end-key>",
	"Scans for a given key range",
	ExactArgs(2),
	Flags(func(flags *pflag.FlagSet) {
		flags.Uint64("limit", 100, "Number of value to return, 0 is unbounded")
	}),
)

func readScanRunE(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	kvdb, err := getKV()
	if err != nil {
		return err
	}

	outputDecoder, err := decoder.NewDecoder(viper.GetString("read-global-decoder"))
	if err != nil {
		return fmt.Errorf("decoder: %w", err)
	}

	keyDecoder, err := decoder.NewDecoder(viper.GetString("read-global-key-decoder"))
	if err != nil {
		return fmt.Errorf("key decoder: %w", err)
	}

	startKey := args[0]
	exclusivelyEndKey := args[1]
	limit := viper.GetUint64("read-scan-limit")
	zlog.Info("store scan key",
		zap.String("start_key", startKey),
		zap.String("exclusively_end_key", exclusivelyEndKey),
		zap.Uint64("limit", limit),
	)

	itr := kvdb.Scan(ctx, []byte(startKey), []byte(exclusivelyEndKey), int(limit))

	keyCount := 0
	fmt.Printf("Scanning keys [%q,%q)\n", startKey, exclusivelyEndKey)
	fmt.Println("")
	for itr.Next() {
		keyCount++
		it := itr.Item()
		fmt.Printf("%s\t->\t%s\n", keyDecoder.Decode(it.Key), outputDecoder.Decode(it.Value))
	}
	if err := itr.Err(); err != nil {
		return fmt.Errorf("iteration failed: %w", err)
	}
	fmt.Println("")
	fmt.Printf("Found %d keys\n", keyCount)

	return nil
}
