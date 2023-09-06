package main

import (
	"fmt"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
	"github.com/streamingfast/kvdb/cmd/kvdb/decoder"
	"github.com/streamingfast/kvdb/cmd/kvdb/formatter"

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
		flags.String("prefix-format", "ascii", "Prefix format such as ascii, hex, base58, base64, solanaATL, etc.")
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

	keyDecoder, err := decoder.NewDecoder(viper.GetString("read-global-key-decoder"))
	if err != nil {
		return fmt.Errorf("key decoder: %w", err)
	}

	prefixDecoder, err := formatter.NewDecoder(viper.GetString("read-prefix-prefix-format"))
	if err != nil {
		return fmt.Errorf("prefix decoder: %w", err)
	}

	prefix := args[0]
	limit := viper.GetUint64("read-prefix-limit")
	zlog.Info("store prefix",
		zap.String("prefix", prefix),
		zap.Uint64("limit", limit),
	)

	p, err := prefixDecoder.Decode(prefix)
	if err != nil {
		return fmt.Errorf("decoding prefix: %w", err)
	}
	itr := kvdb.Prefix(ctx, p, int(limit))

	keyCount := 0
	fmt.Println("")
	for itr.Next() {
		keyCount++
		it := itr.Item()
		fmt.Printf("Key: %s\n", keyDecoder.Decode(it.Key))
		fmt.Printf("Value(s): %s\n", outputDecoder.Decode(it.Value))
	}
	if err := itr.Err(); err != nil {
		return fmt.Errorf("iteration failed: %w", err)
	}
	fmt.Println("")
	fmt.Printf("Found %d keys\n", keyCount)

	return nil
}
