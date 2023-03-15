package decoder

import "encoding/hex"

var _ Decode = (*HexDecoder)(nil)

type HexDecoder struct {
}

func (h *HexDecoder) Decode(data []byte) string {
	return hex.EncodeToString(data)
}
