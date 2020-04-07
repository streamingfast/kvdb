package store

import (
	"context"
	"sync"
)

type Iterator struct {
	ctx        context.Context
	cancelFunc func()
	items      chan *KV
	errorCh    chan error
	lastItem   *KV
	err        error
	once       sync.Once
}

// NewIterator provides a streaming resultset for key/value queries
func NewIterator(ctx context.Context) *Iterator {
	newCtx, cancel := context.WithCancel(ctx)
	return &Iterator{
		ctx:        newCtx,
		cancelFunc: cancel,
		items:      make(chan *KV, 100),
		errorCh:    make(chan error, 1),
	}
}

//
// Reading primitives
//

func (it *Iterator) Next() bool {
	if it.err != nil {
		return false
	}
	var val *KV
	var ok bool
	select {
	case val, ok = <-it.items:
		if !ok {
			return false
		}
		it.lastItem = val

	case err := <-it.errorCh:
		it.err = err
		return false
	}

	return true
}

func (it *Iterator) Item() *KV {
	return it.lastItem
}

func (it *Iterator) Err() error {
	return it.err
}

func (it *Iterator) Close() {
	it.cancelFunc()
}

//
// Results gathering primitives
//
func (it *Iterator) PushItem(res *KV) {
	it.items <- res
}

func (it *Iterator) PushFinished() {
	close(it.items)
}

func (it *Iterator) PushError(err error) {
	it.once.Do(func() {
		it.errorCh <- err
	})
}

func (it *Iterator) Context() context.Context {
	return it.ctx
}
