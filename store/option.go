package store

type EmtpyValueEnabler interface {
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
	if f, ok := s.(EmtpyValueEnabler); ok {
		f.EnableEmpty()
	}
}
