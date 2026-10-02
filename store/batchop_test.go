package store

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBatchOpTakeAndRestore(t *testing.T) {
	batch := NewBatchOp(0, 0, 0)
	batch.Op([]byte("a"), []byte("1"))
	batch.Op([]byte("b"), []byte("2"))

	taken := batch.Take()
	require.Len(t, taken, 2)
	assert.Empty(t, batch.GetBatch())
	assert.Zero(t, batch.Size())

	batch.Op([]byte("c"), []byte("3"))
	batch.Restore(taken)

	var keys []string
	for _, entry := range batch.GetBatch() {
		keys = append(keys, string(entry.Key))
	}

	assert.Equal(t, []string{"a", "b", "c"}, keys, "restored entries go ahead of the ones added since")
	assert.Equal(t, 6, batch.Size())
}
