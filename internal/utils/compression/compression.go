package compression

import (
	"bytes"
	"compress/gzip"
	"io/ioutil"

	"golang.org/x/xerrors"
)

type Compression string

const (
	CompressionGzip Compression = "gzip"
)

func Compress(data []byte, compression Compression) ([]byte, error) {
	if len(data) == 0 {
		return data, nil
	}

	if compression == CompressionGzip {
		var buf bytes.Buffer
		zw := gzip.NewWriter(&buf)
		if _, err := zw.Write(data); err != nil {
			return nil, xerrors.Errorf("failed to write compressed data: %w", err)
		}
		if err := zw.Close(); err != nil {
			return nil, xerrors.Errorf("failed to close writer: %w", err)
		}

		return buf.Bytes(), nil
	}

	return nil, xerrors.Errorf("failed to compress with unsupported type %v", compression)
}

func Decompress(data []byte, compression Compression) ([]byte, error) {
	if len(data) == 0 {
		return data, nil
	}

	if compression == CompressionGzip {
		zr, err := gzip.NewReader(bytes.NewBuffer(data))
		if err != nil {
			return nil, xerrors.Errorf("failed to initiate reader: %w", err)
		}
		decoded, err := ioutil.ReadAll(zr)
		if err != nil {
			return nil, xerrors.Errorf("failed to read data: %w", err)
		}
		if err := zr.Close(); err != nil {
			return nil, xerrors.Errorf("failed to close reader: %w", err)
		}
		return decoded, nil
	}

	return nil, xerrors.Errorf("failed to decompress with unsupported type %v", compression)
}
