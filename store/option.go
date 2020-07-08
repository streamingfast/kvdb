package store

type EmtpyValeEnabler interface {
	EnableEmpty()
}

type Option interface {
	apply(s KVStore)
}

type emptyValueOpt struct {
}

func WithEmptyValue() Option {
	return emptyValueOpt{}
}

func (e emptyValueOpt) apply(s KVStore) {
	if f, ok := s.(EmtpyValeEnabler); ok {
		f.EnableEmpty()
	}
}
