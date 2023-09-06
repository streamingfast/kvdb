package main

import (
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/spf13/viper"
	"github.com/streamingfast/kvdb/cmd/kvdb/decoder"

	"github.com/streamingfast/kvdb/store"

	"github.com/spf13/cobra"
	. "github.com/streamingfast/cli"
	"go.uber.org/zap"
)

var ReadGetCmd = Command(readGetRunE,
	"get <key>",
	"Retrieve a key",
	ExactArgs(1),
)

func readGetRunE(cmd *cobra.Command, args []string) error {
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
	_ = keyDecoder

	key := args[0]
	zlog.Info("store get key",
		zap.String("key", key),
	)

	keyVal, err := hex.DecodeString(args[0])
	if err != nil {
		return fmt.Errorf("key decoder: %w", err)
	}
	value, err := kvdb.Get(ctx, keyVal)

	if err != nil {
		if errors.Is(err, store.ErrNotFound) {
			fmt.Println("")
			fmt.Printf("Key ->\t%s\tNOT FOUND\n", key)
			return nil
		}
		return fmt.Errorf("failed to get key: %w", err)
	}

	fmt.Println("")
	fmt.Printf("Key\t->\t%s\n", key)
	fmt.Printf("Value\t->\t%s\n", outputDecoder.Decode(value))
	return nil
}
