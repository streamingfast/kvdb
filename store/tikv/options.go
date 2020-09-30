package tikv

func (s *Store) EnableEmpty() {
	zlog.Info("enabling possible empty value on store implementation")
	s.emptyValuePossible = true
}
