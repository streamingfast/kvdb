package badger

func (s *Store) EnableEmpty() {
	zlog.Info("discarding possible empty value on store implementation, not required for this store")
}
