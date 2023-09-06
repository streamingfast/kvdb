package decoder

import (
	"encoding/binary"
	"fmt"
	"github.com/mr-tron/base58"
	"math"
)

var _ Decode = (*SolanaATLDecoder)(nil)

type SolanaATLDecoder struct {
}

func (h *SolanaATLDecoder) Decode(data []byte) string {
	key := base58.Encode(data[1:33])
	blockNum := math.MaxUint64 - binary.BigEndian.Uint64(data[33:])
	return fmt.Sprintf("Account [%s] BlockNum [%d]", key, blockNum)
}
