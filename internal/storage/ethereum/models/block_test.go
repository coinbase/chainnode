package models

import (
	"testing"
	"time"

	"github.com/coinbase/chainnode/internal/api"
	"github.com/coinbase/chainnode/internal/api/ethereum"
	"github.com/coinbase/chainnode/internal/utils/compression"
	"github.com/coinbase/chainnode/internal/utils/testutil"
)

func TestBlock(t *testing.T) {
	require := testutil.Require(t)
	tag := uint32(1)
	height := uint64(123123)
	sequence := api.Sequence(555)
	hash := "0x1"
	data := []byte("test-data")

	compressed, err := compression.Compress(data, compression.CompressionGzip)
	require.NoError(err)

	require.Equal("1#blocks#123123", MakeBlockPartitionKey(tag, height))
	require.Equal("000000000000022b", MakeBlockSortKey(sequence))

	blockTimes := []time.Time{
		testutil.MustTime("2020-11-24T16:07:21Z"),
		{},
	}
	for _, blockTime := range blockTimes {
		block := ethereum.NewBlock(tag, height, hash, sequence, blockTime, data)
		entry, err := MakeBlockDDBEntry(block)
		require.NoError(err)
		require.NotNil(entry)
		testutil.MustTime(entry.UpdatedAt)

		metadata := &ethereum.BlockMetadata{
			Tag:       tag,
			Height:    height,
			Hash:      hash,
			Sequence:  sequence,
			BlockTime: blockTime,
		}

		objectKey, err := entry.MakeObjectKey()
		require.NoError(err)
		require.Equal("1/blocks/123123/555", objectKey)

		pk, err := entry.GetPartitionKey()
		require.NoError(err)
		require.Equal("1#blocks#123123", pk)

		sk, err := entry.GetSortKey()
		require.NoError(err)
		require.Equal("000000000000022b", sk)

		actualData, err := entry.GetData()
		require.NoError(err)
		require.Equal(compressed, actualData)

		var actualBlock ethereum.Block
		err = entry.AsAPI(&actualBlock)
		require.NoError(err)
		require.False(actualBlock.UpdatedAt.IsZero())
		block.UpdatedAt = actualBlock.UpdatedAt
		require.Equal(block, &actualBlock)
		require.Equal(blockTime, actualBlock.BlockTime)

		actualMetadata, err := entry.ToBlockMetadata()
		require.NoError(err)
		require.False(actualMetadata.UpdatedAt.IsZero())
		metadata.UpdatedAt = actualMetadata.UpdatedAt
		require.Equal(metadata, actualMetadata)
		require.Equal(blockTime, actualMetadata.BlockTime)
	}
}
