package compression

import (
	"testing"

	"github.com/coinbase/chainnode/internal/utils/testutil"
)

func TestCompress(t *testing.T) {
	tests := []struct {
		testName    string
		data        []byte
		compression Compression
	}{
		{
			"nilData",
			nil,
			CompressionGzip,
		},
		{
			"emptyData",
			[]byte{},
			CompressionGzip,
		},
		{
			"blockDataCompression",
			[]byte(`
			{
				"hash": "0xbaa42c",
				"number": "0xacc290",
			}`),
			CompressionGzip,
		},
	}
	for _, test := range tests {
		t.Run(test.testName, func(t *testing.T) {
			require := testutil.Require(t)

			compressed, err := Compress(test.data, test.compression)
			require.NoError(err)

			decompressed, err := Decompress(compressed, test.compression)
			require.NoError(err)
			require.Equal(decompressed, test.data)
		})
	}
}

func TestCompress_UnknownCompression(t *testing.T) {
	require := testutil.Require(t)

	data := []byte(`
			{
				"hash": "0xbaa42c",
				"number": "0xacc290",
			}`)
	compressed, err := Compress(data, "foo")
	require.Error(err)
	require.Nil(compressed)
}
