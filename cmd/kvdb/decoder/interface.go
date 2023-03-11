package decoder

import (
	"fmt"
	"strings"
)

type Decode interface {
	Decode([]byte) string
}

func NewDecoder(scheme string) (Decode, error) {
	if scheme == "ascii" {
		return &AsciiDecoder{}, nil
	}

	if scheme == "hex" {
		return &HexDecoder{}, nil
	}

	if strings.HasPrefix(scheme, "proto") {
		decoder, err := newProtoDecoder(scheme)
		if err != nil {
			return nil, fmt.Errorf("proto decoder: %w", err)
		}
		return decoder, nil
	}

	return nil, fmt.Errorf("unknown decoding scheme %q", scheme)
}
