package store

import (
	"fmt"
	"strconv"
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
	entry := &KV{key, value}

	b.size += entry.Size()
	b.puts++
	b.batch = append(b.batch, entry)

	if b.largestEntry.Size() < entry.Size() {
		b.largestEntry = entry
	}
}

func (b *BatchOp) ShouldFlush() bool {
	if len(b.batch) == 0 {
		return false
	}

	return b.shouldFlush(b.size, b.puts)
}

// WouldFlushNext determines if adding another item with the specified `len(key) + len(value)` would trigger
// a flush of the batch. This can be used to push a batch preemptively before inserting and
// item that would make the batch bigger than allowed max size.
func (b *BatchOp) WouldFlushNext(key []byte, value []byte) bool {
	return b.shouldFlush(b.size+len(key)+len(value), b.puts+1)
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
	b.largestEntry = nil
}

func (b *BatchOp) MarshalLogObject(enc zapcore.ObjectEncoder) error {
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
