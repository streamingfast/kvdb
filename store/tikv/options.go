package tikv

func (s *Store) EnableEmpty() {
	s.emptyValuePossible = true
}
