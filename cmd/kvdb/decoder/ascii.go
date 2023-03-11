package decoder

var _ Decode = (*AsciiDecoder)(nil)

type AsciiDecoder struct {
}

func (a *AsciiDecoder) Decode(data []byte) string {
	return string(data)
}
