package tikv

import "go.uber.org/zap"

func (s *Store) EnableEmpty() {
	s.emptyValuePossible = true
}

func (s *Store) SetLogger(logger *zap.Logger) {
	s.zlogger = logger
}