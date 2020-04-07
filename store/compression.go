package store

import (
	"bytes"
	"fmt"

	"github.com/klauspost/compress/zstd"
)

type Compressor interface {
	Compress(in []byte) []byte
	Decompress(in []byte) ([]byte, error)
}

func NewCompressor(mode string) (Compressor, error) {
	switch mode {
	case "", "zstd":
		return NewZstdCompressor(), nil
	case "none", "false", "no":
		return NewNoOpCompressor(), nil
	default:
		return nil, fmt.Errorf("invalid compression value, use zstd (by default) or 'none'")
	}
}

type NoOpCompressor struct{}

func NewNoOpCompressor() *NoOpCompressor {
	return &NoOpCompressor{}
}

func (NoOpCompressor) Compress(in []byte) []byte {
	return in
}
func (NoOpCompressor) Decompress(in []byte) ([]byte, error) {
	return in, nil
}

type ZstdCompressor struct {
	dec *zstd.Decoder
	enc *zstd.Encoder
}

func NewZstdCompressor() *ZstdCompressor {
	enc, _ := zstd.NewWriter(nil) // Errors only on failed `opts` application
	dec, _ := zstd.NewReader(nil)
	return &ZstdCompressor{
		dec: dec,
		enc: enc,
	}
}

func (c *ZstdCompressor) Compress(in []byte) (out []byte) {
	if len(in) > 256 {
		return c.enc.EncodeAll(in, out)
	}
	return in
}

var zstdMagicBytes = []byte{0x28, 0xB5, 0x2F, 0xFD}

func (c *ZstdCompressor) Decompress(in []byte) ([]byte, error) {
	if len(in) > 4 && bytes.Compare(in[:4], zstdMagicBytes) == 0 {
		buf, err := c.dec.DecodeAll(in, nil)
		if err != nil {
			return nil, err
		}
		return buf, nil
	}

	return in, nil
}
