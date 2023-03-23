package store

import (
	"context"
	"sync"
)

// Iterator can end in any of those scenarios:
//
//  1. PushError() is called by the db backend
//  2. PushComplete() is called by the db backend and
//     Next() is called by consumer until items channel is empty
//  3. The context given by the consumer is cancelled, notifying
//     the db backend and (hopefully) causing a PushError() to be called with context.Canceled
//
// In any of these cases, the following call to Next() returns false.
//
// Assumptions:
//
// * Next() must never be called again after it returned `false`
// * No other Push...() function is called PushFinished() or PushError().
// * Next(), Item() and Error() are never called concurrently.
// * PushItem(), PushFinished() and PushError() are never called concurrently.
// * If the reader wants to finish early, it should close the context to prevent waste
type Iterator struct {
	ctx               context.Context
	items             chan KV
	errorCh           chan error
	lastItem          KV
	err               error
	once              sync.Once
	parallelProviders sync.WaitGroup
}

// NewIterator provides a streaming resultset for key/value queries
func NewIterator(ctx context.Context) *Iterator {
	it := &Iterator{
		ctx:     ctx,
		items:   make(chan KV, 100),
		errorCh: make(chan error, 1),
	}
	it.parallelProviders.Add(1)
	return it
}

// NewIteratorFromMultipleSources provides an iterator that can receive 'PushItem' and 'PushFinished' from multiple sources
// The Reading must be done linearly.
func NewIteratorFromMultipleSources(ctx context.Context, sources int) *Iterator {
	it := &Iterator{
		ctx:     ctx,
		items:   make(chan KV, 100),
		errorCh: make(chan error, sources),
	}
	it.parallelProviders.Add(sources)
	return it
}

//
// Reading primitives
//

func (it *Iterator) Next() bool {
	if it.err != nil {
		return false
	}

	select {
	case val, ok := <-it.items:
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

func (it *Iterator) Item() KV {
	return it.lastItem
}

func (it *Iterator) Err() error {
	return it.err
}

// Results gathering primitives
func (it *Iterator) PushItem(res KV) bool {
	select {
	case <-it.ctx.Done():
		it.PushError(it.ctx.Err())
		return false
	case it.items <- res:
		return true
	}
}

func (it *Iterator) PushFinished() {
	it.parallelProviders.Done()

	it.once.Do(func() {
		go func() {
			it.parallelProviders.Wait()
			close(it.items)
		}()
	})
}

func (it *Iterator) PushError(err error) {
	it.errorCh <- err

	it.parallelProviders.Done()
	it.once.Do(func() {

		go func() {
			it.parallelProviders.Wait()
			close(it.errorCh)
		}()
	})
}
