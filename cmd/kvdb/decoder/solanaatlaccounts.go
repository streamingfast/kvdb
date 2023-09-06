package decoder

import (
	"github.com/mr-tron/base58"
	"strings"
)

var _ Decode = (*SolanaATLAccountsDecoder)(nil)

type SolanaATLAccountsDecoder struct {
}

func (h *SolanaATLAccountsDecoder) Decode(data []byte) string {
	sb := &strings.Builder{}
	sb.WriteString("\n")
	for i := 0; i < len(data); i += 32 {
		sb.WriteString("\t")
		sb.WriteString(base58.Encode(data[i : i+32]))
		sb.WriteString("\n")
	}
	return sb.String()
}
