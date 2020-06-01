package store

import (
	"context"
	"sync"
)

type Iterator struct {
	ctx      context.Context
	items    chan *KV
	errorCh  chan error
	lastItem *KV
	err      error
	once     sync.Once
}

// NewIterator provides a streaming resultset for key/value queries
func NewIterator(ctx context.Context) *Iterator {
	return &Iterator{
		ctx:     ctx,
		items:   make(chan *KV, 100),
		errorCh: make(chan error, 1),
	}
}

//
// Reading primitives
//

func (it *Iterator) Next() bool {
	if it.err != nil {
		return false
	}

	select {
	case val := <-it.items:
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

//
// Results gathering primitives
//
func (it *Iterator) PushItem(res *KV) bool {
	select {
	case <-it.ctx.Done():
		it.PushError(it.ctx.Err())
		return false
	case it.items <- res:
		return true
	}
}

func (it *Iterator) PushFinished() {
	it.once.Do(func() {
		close(it.errorCh)
	})
}

func (it *Iterator) PushError(err error) {
	it.once.Do(func() {
		it.errorCh <- err
		close(it.errorCh)
	})
}
