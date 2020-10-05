package store

import (
	"time"

	"go.uber.org/zap/zapcore"
)

type BatchOp struct {
	sizeThreshold int
	putsThreshold int
	timeThreshold time.Duration

	batch     []*KV
	size      int
	puts      int
	lastReset time.Time
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
	b.size += len(key) + len(value)
	b.puts++
	b.batch = append(b.batch, &KV{key, value})
}

func (b *BatchOp) ShouldFlush() bool {
	if len(b.batch) == 0 {
		return false
	}

	return b.shouldFlush(b.size, b.puts)
}

// WouldFlushNext determines if addind another item with the specified size would trigger
// a flush of the batch. This can be used to push a batch pre-emptively
func (b *BatchOp) WouldFlushNext(size int) bool {
	return b.shouldFlush(b.size+size, b.puts+1)
}

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
	return b.size
}

func (b *BatchOp) GetBatch() []*KV {
	return b.batch
}

func (b *BatchOp) Reset() {
	capacity := 1024
	if b.putsThreshold > 0 {
		capacity = b.putsThreshold
	}

	b.batch = make([]*KV, 0, capacity)
	b.size = 0
	b.puts = 0
	b.lastReset = time.Now()
}

func (b *BatchOp) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddInt("size_threshold", b.sizeThreshold)
	enc.AddInt("ops_threshold", b.putsThreshold)
	enc.AddDuration("time_threshold", b.timeThreshold)
	return nil
}
