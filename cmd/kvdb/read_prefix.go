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

var ReadPrefixCmd = Command(readPrefixRunE,
	"prefix <prefix>",
	"Retrieve keys by prefix",
	ExactArgs(1),
	Flags(func(flags *pflag.FlagSet) {
		flags.Uint64("limit", 100, "Number of value to return, 0 is unbounded")
	}),
)

func readPrefixRunE(cmd *cobra.Command, args []string) error {
	ctx := cmd.Context()

	kvdb, err := getKV()
	if err != nil {
		return err
	}

	outputDecoder, err := decoder.NewDecoder(viper.GetString("read-global-decoder"))
	if err != nil {
		return fmt.Errorf("decoder: %w", err)
	}

	prefix := args[0]
	limit := viper.GetUint64("read-prefix-limit")
	zlog.Info("store prefix",
		zap.String("prefix", prefix),
		zap.Uint64("limit", limit),
	)

	itr := kvdb.Prefix(ctx, []byte(prefix), int(limit))

	keyCount := 0
	fmt.Printf("keys with prefix: %s", prefix)
	fmt.Println("")
	for itr.Next() {
		keyCount++
		it := itr.Item()
		fmt.Printf("%s\t->\t%s\n", string(it.Key), outputDecoder.Decode(it.Value))
	}
	if err := itr.Err(); err != nil {
		return fmt.Errorf("iteration failed: %w", err)
	}
	fmt.Println("")
	fmt.Printf("Found %d keys\n", keyCount)

	return nil
}
