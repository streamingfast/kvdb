package main

import (
	"fmt"

	"github.com/spf13/viper"
	"github.com/streamingfast/kvdb/store"
	"go.uber.org/zap"
)

func getKV() (store.KVStore, error) {
	dsn := viper.GetString("global-dsn")
	if dsn == "" {
		return nil, fmt.Errorf("dsn is required")
	}
	zlog.Info("setting up store", zap.String("dsn", dsn))
	s, err := store.New(dsn)
	if err != nil {
		return nil, fmt.Errorf("create store: %w", err)
	}
	return s, nil
}
