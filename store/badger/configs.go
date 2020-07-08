package badger

import "go.uber.org/zap"

func (s *Store) EnableEmpty() {
}

func (s *Store) SetLogger(logger *zap.Logger) {
	s.zlogger = logger
}