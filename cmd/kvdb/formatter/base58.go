package formatter

import (
	"github.com/mr-tron/base58"
)

var _ Decode = (*Base58Decoder)(nil)

type Base58Decoder struct {
}

func (h *Base58Decoder) Decode(data string) ([]byte, error) {
	return base58.Decode(data)
}
