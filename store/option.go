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

type ReadOptions struct {
	KeyOnly bool
}

type ReadOption interface {
	Apply(o *ReadOptions)
}

func KeyOnly() ReadOption {
	return keyOnlyReadOption{}
}

type keyOnlyReadOption struct{}

func (o keyOnlyReadOption) Apply(opts *ReadOptions) {
	opts.KeyOnly = true
}
