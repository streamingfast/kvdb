package store

import "time"

type BatchPut struct {
	sizeThreshold int
	putsThreshold int
	timeThreshold time.Duration

	batch     []*KV
	size      int
	puts      int
	lastReset time.Time
}

func NewBatchPut(sizeThreshold int, putsThreshold int, timeThreshold time.Duration) *BatchPut {
	b := &BatchPut{
		sizeThreshold: sizeThreshold,
		putsThreshold: putsThreshold,
		timeThreshold: timeThreshold,
	}
	b.Reset()
	return b
}

func (b *BatchPut) Put(key, value []byte) {
	b.size += len(key) + len(value)
	b.puts++
	b.batch = append(b.batch, &KV{key, value})
}

func (b *BatchPut) ShouldFlush() bool {
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

func (b *BatchPut) GetBatch() []*KV {
	return b.batch
}

func (b *BatchPut) Reset() {
	b.batch = make([]*KV, 0, 1024)
	b.size = 0
	b.puts = 0
	b.lastReset = time.Now()
}
