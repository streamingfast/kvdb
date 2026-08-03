package store

import (
	"fmt"
	"strconv"
	"sync"
	"time"

	"go.uber.org/zap/zapcore"
)

// BatchOp accumulates pending operations until one of the configured thresholds is reached.
//
// All its methods are safe for concurrent use. Note however that a caller that composes
// multiple calls together, like checking [BatchOp.WouldFlushNext] before calling [BatchOp.Op],
// must still hold its own lock across the whole sequence, otherwise two goroutines can both
// decide that no flush is needed and both append to the batch.
type BatchOp struct {
	sizeThreshold int
	putsThreshold int
	timeThreshold time.Duration

	// lock guards every mutable field below, it is never held while calling back into
	// the caller so it can never participate in a lock cycle.
	lock      sync.Mutex
	batch     []*KV
	size      int
	puts      int
	lastReset time.Time

	largestEntry *KV
}

func NewBatchOp(sizeThreshold int, optsThreshold int, timeThreshold time.Duration) *BatchOp {
	b := &BatchOp{
		sizeThreshold: sizeThreshold,
		putsThreshold: optsThreshold,
		timeThreshold: timeThreshold,
	}
	b.Reset()
	return b
}

func (b *BatchOp) Op(key, value []byte) {
	b.lock.Lock()
	defer b.lock.Unlock()

	entry := &KV{key, value}

	b.size += entry.Size()
	b.puts++
	b.batch = append(b.batch, entry)

	if b.largestEntry.Size() < entry.Size() {
		b.largestEntry = entry
	}
}

func (b *BatchOp) ShouldFlush() bool {
	b.lock.Lock()
	defer b.lock.Unlock()

	if len(b.batch) == 0 {
		return false
	}

	return b.shouldFlush(b.size, b.puts)
}

// WouldFlushNext determines if adding another item with the specified `len(key) + len(value)` would trigger
// a flush of the batch. This can be used to push a batch preemptively before inserting and
// item that would make the batch bigger than allowed max size.
func (b *BatchOp) WouldFlushNext(key []byte, value []byte) bool {
	b.lock.Lock()
	defer b.lock.Unlock()

	return b.shouldFlush(b.size+len(key)+len(value), b.puts+1)
}

// shouldFlush must be called while holding the lock.
func (b *BatchOp) shouldFlush(size int, opCount int) bool {
	if b.sizeThreshold > 0 && size > b.sizeThreshold {
		return true
	}
	if b.putsThreshold > 0 && opCount >= b.putsThreshold {
		return true
	}
	if b.timeThreshold != 0 && time.Since(b.lastReset) > b.timeThreshold {
		return true
	}
	return false
}

func (b *BatchOp) Size() int {
	b.lock.Lock()
	defer b.lock.Unlock()

	return b.size
}

// GetBatch returns the pending entries. The returned slice must not be mutated by the caller
// and is only valid until the next [BatchOp.Op] or [BatchOp.Reset] call.
func (b *BatchOp) GetBatch() []*KV {
	b.lock.Lock()
	defer b.lock.Unlock()

	return b.batch
}

func (b *BatchOp) Reset() {
	b.lock.Lock()
	defer b.lock.Unlock()

	capacity := 1024
	if b.putsThreshold > 0 {
		capacity = b.putsThreshold
	}

	b.batch = make([]*KV, 0, capacity)
	b.size = 0
	b.puts = 0
	b.lastReset = time.Now()
	b.largestEntry = nil
}

func (b *BatchOp) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	b.lock.Lock()
	defer b.lock.Unlock()

	sizeThreshold := "None"
	if b.sizeThreshold > 0 {
		sizeThreshold = strconv.FormatInt(int64(b.sizeThreshold), 10)
	}

	opThreshold := "None"
	if b.putsThreshold > 0 {
		opThreshold = strconv.FormatInt(int64(b.putsThreshold), 10)
	}

	timeThreshold := "None"
	if b.timeThreshold > 0 {
		timeThreshold = b.timeThreshold.String()
	}

	enc.AddString("size", fmt.Sprintf("%d (limit %s)", b.size, sizeThreshold))
	enc.AddString("ops", fmt.Sprintf("%d (limit %s)", b.puts, opThreshold))

	elapsedSinceLastReset := "N/A"
	if !b.lastReset.IsZero() {
		elapsedSinceLastReset = (time.Now().Sub(b.lastReset)).String()
	}

	enc.AddString("time", fmt.Sprintf("%s (limit %s)", elapsedSinceLastReset, timeThreshold))

	if b.largestEntry != nil {
		enc.AddString("largest_entry", fmt.Sprintf("%x (key %d bytes, value %d bytes)", b.largestEntry.Key, len(b.largestEntry.Key), len(b.largestEntry.Value)))
	}

	return nil
}
