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
	if b.sizeThreshold > 0 && b.size > b.sizeThreshold {
		return true
	}
	if b.putsThreshold > 0 && b.puts >= b.putsThreshold {
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
	b.batch = make([]*KV, 0, 1024)
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
