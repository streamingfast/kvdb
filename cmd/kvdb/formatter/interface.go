package formatter

import (
	"fmt"
)

type Decode interface {
	Decode(data string) ([]byte, error)
}

func NewDecoder(scheme string) (Decode, error) {
	if scheme == "ascii" {
		return &AsciiDecoder{}, nil
	}

	if scheme == "base58" {
		return &Base58Decoder{}, nil
	}

	if scheme == "solanaATL" {
		return &SolanaATLDecoder{}, nil
	}

	return nil, fmt.Errorf("unknown decoding scheme %q", scheme)
}
