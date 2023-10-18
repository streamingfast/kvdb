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
// * Next(), Item() and Err() are never called concurrently.
// * PushItem(), PushFinished() and PushError() are never called concurrently.
// * If the reader wants to finish early, it should close the context to prevent waste
type Iterator struct {
	ctx        context.Context
	items      chan KV
	errorCh    chan error
	lastItem   KV
	err        error
	nextCalled bool
	once       sync.Once
}

// NewIterator provides a streaming resultset for key/value queries. The correct pattern
// iteration pattern is:
//
//	it := store.Prefix(ctx, prefix, limit)
//	for it.Next() {
//	    // do something with it.Item()
//	}
//
//	if err := it.Err(); err != nil {
//	    // handle error
//	}
func NewIterator(ctx context.Context) *Iterator {
	return &Iterator{
		ctx:     ctx,
		items:   make(chan KV, 100),
		errorCh: make(chan error, 1),
	}
}

//
// Reading primitives
//

// Next returns true if there is a next item to read, false if there is no more item or an error occurred.
//
// After Next() returns false, the Err() **must** be called to check if an error occurred. Failing to do so
// will lead to error being silently ignored in your code and hard to debug issues.
func (it *Iterator) Next() bool {
	it.nextCalled = true

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

// Item returns the last item read if Next() returned true, or nil if no item was read yet.
//
// It's important to note that Item() returns the last item read, not the next item to read.
func (it *Iterator) Item() KV {
	return it.lastItem
}

// Err returns the error that caused the iterator to stop, or nil if no error occurred.
// This method **must** be called after Next() returned false to check if an error occurred.
//
// Calling this method right after the creation of the iterator will panic as the error
// is set only after the a call to Next(), never before.
//
// If Next() is called in a for-loop, you must **not** call Err() inside the loop. This is
// because Next() returns false when and an error is encountered which means that it will
// exit the loops and any code within the if will not run.
//
// The correct iteration pattern is:
//
//	it := store.Prefix(ctx, prefix, limit)
//	for it.Next() {
//	    // do something with it.Item()
//	}
//
//	if err := it.Err(); err != nil {
//	    // handle error
//	}
func (it *Iterator) Err() error {
	if !it.nextCalled {
		panic("calling Err() without having call Next() is invalid as the error is populated only on Next() (if any)")
	}

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
	it.once.Do(func() {
		close(it.items)
	})
}

func (it *Iterator) PushError(err error) {
	it.once.Do(func() {
		it.errorCh <- err
		close(it.errorCh)
	})
}
