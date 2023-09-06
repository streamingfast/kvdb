package decoder

import (
	"github.com/mr-tron/base58"
)

var _ Decode = (*Base58Decoder)(nil)

type Base58Decoder struct {
}

func (h *Base58Decoder) Decode(data []byte) string {
	return base58.Encode(data)
}
