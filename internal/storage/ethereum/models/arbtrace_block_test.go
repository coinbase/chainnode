package models

import (
	"testing"
	"time"

	"github.com/coinbase/chainnode/internal/api"
	"github.com/coinbase/chainnode/internal/api/ethereum"
	"github.com/coinbase/chainnode/internal/utils/compression"
	"github.com/coinbase/chainnode/internal/utils/testutil"
)

func TestArbtraceBlock(t *testing.T) {
	require := testutil.Require(t)
	tag := uint32(1)
	height := uint64(10000000)
	hash := "0xab123"
	sequence := api.Sequence(555)
	data := []byte("test-data")
	compressed, err := compression.Compress(data, compression.CompressionGzip)
	require.NoError(err)

	require.Equal("1#arbtrace-block#10000000", MakeArbtraceBlockPartitionKey(tag, height))
	require.Equal("000000000000022b", MakeArbtraceBlockSortKey(sequence))

	blockTimes := []time.Time{
		testutil.MustTime("2020-11-24T16:07:21Z"),
		{},
	}

	for _, blockTime := range blockTimes {
		trace := ethereum.NewTrace(tag, height, hash, sequence, data, blockTime)
		entry, err := MakeArbtraceBlockDDBEntry(trace)
		require.NoError(err)
		require.NotNil(entry)
		testutil.MustTime(entry.UpdatedAt)

		objectKey, err := entry.MakeObjectKey()
		require.NoError(err)
		require.Equal("1/arbtrace-block/10000000/555", objectKey)

		pk, err := entry.GetPartitionKey()
		require.NoError(err)
		require.Equal("1#arbtrace-block#10000000", pk)

		sk, err := entry.GetSortKey()
		require.NoError(err)
		require.Equal("000000000000022b", sk)

		actualData, err := entry.GetData()
		require.NoError(err)
		require.Equal(compressed, actualData)

		var actualTrace ethereum.Trace
		err = entry.AsAPI(&actualTrace)
		require.NoError(err)
		require.False(actualTrace.UpdatedAt.IsZero())
		trace.UpdatedAt = actualTrace.UpdatedAt
		require.Equal(trace, &actualTrace)
	}
}
