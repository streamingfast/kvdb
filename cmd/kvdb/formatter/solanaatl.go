package formatter

import (
	"github.com/mr-tron/base58"
)

var _ Decode = (*SolanaATLDecoder)(nil)

type SolanaATLDecoder struct {
}

func (h *SolanaATLDecoder) Decode(data string) ([]byte, error) {
	keyBytes, _ := base58.Decode(data)
	return append([]byte{0}, keyBytes...), nil
}
