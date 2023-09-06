package formatter

var _ Decode = (*AsciiDecoder)(nil)

type AsciiDecoder struct {
}

func (a *AsciiDecoder) Decode(data string) ([]byte, error) {
	return []byte(data), nil
}
