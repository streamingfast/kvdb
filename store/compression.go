package store

import (
	"bytes"
	"fmt"

	"github.com/klauspost/compress/zstd"
	"go.uber.org/zap/zapcore"
)

type Compressor interface {
	Compress(in []byte) []byte
	Decompress(in []byte) ([]byte, error)

	zapcore.ObjectMarshaler
}

func NewCompressor(mode string, thresholdInBytes int) (Compressor, error) {
	switch mode {
	case "zst", "zstd":
		return NewZstdCompressor(thresholdInBytes), nil
	case "", "none", "false", "no":
		return NewNoOpCompressor(), nil
	default:
		return nil, fmt.Errorf("invalid compression value, use '' or zstd (for legacy support) or 'none'")
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

func (NoOpCompressor) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("compression", "none")
	return nil
}

type ZstdCompressor struct {
	enc              *zstd.Encoder
	dec              *zstd.Decoder
	thresholdInBytes int
}

func NewZstdCompressor(thresholdInBytes int) *ZstdCompressor {
	// There can be errors only when using `opts` parameters, so it's safe to ignore them here
	enc, _ := zstd.NewWriter(nil)
	dec, _ := zstd.NewReader(nil)

	return &ZstdCompressor{
		enc:              enc,
		dec:              dec,
		thresholdInBytes: thresholdInBytes,
	}
}

func (c *ZstdCompressor) Compress(in []byte) (out []byte) {
	if len(in) > c.thresholdInBytes {
		return c.enc.EncodeAll(in, out)
	}

	return in
}

var zstdMagicBytes = []byte{0x28, 0xB5, 0x2F, 0xFD}

func (c *ZstdCompressor) Decompress(in []byte) ([]byte, error) {
	if bytes.HasPrefix(in, zstdMagicBytes) {
		// We pre-allocate a bit the array to reduce allocation, at least the compression size is a good start
		buf, err := c.dec.DecodeAll(in, make([]byte, 0, len(in)))
		if err != nil {
			return nil, err
		}

		return buf, nil
	}

	return in, nil
}

func (c *ZstdCompressor) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	enc.AddString("compression", "zstd")
	enc.AddInt("compression_size_threshold", c.thresholdInBytes)
	return nil
}
