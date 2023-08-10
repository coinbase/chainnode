package models

import (
	"testing"
	"time"

	"github.com/coinbase/chainnode/internal/api"
	"github.com/coinbase/chainnode/internal/api/ethereum"
	"github.com/coinbase/chainnode/internal/utils/compression"
	"github.com/coinbase/chainnode/internal/utils/testutil"
)

func TestLogs(t *testing.T) {
	require := testutil.Require(t)
	tag := uint32(1)
	height := uint64(123123)
	hash := "0x1"
	sequence := api.Sequence(555)
	data := []byte("test-data")
	logsBloom := "0x231"
	compressed, err := compression.Compress(data, compression.CompressionGzip)
	require.NoError(err)

	require.Equal("1#logs#123123", MakeLogsPartitionKey(tag, height))
	require.Equal("000000000000022b", MakeLogsSortKey(sequence))

	blockTimes := []time.Time{
		testutil.MustTime("2020-11-24T16:07:21Z"),
		{},
	}

	for _, blockTime := range blockTimes {
		logs := ethereum.NewLogs(tag, height, hash, sequence, logsBloom, data, blockTime)
		entry, err := MakeLogsDDBEntry(logs)
		require.NoError(err)
		require.NotNil(entry)
		testutil.MustTime(entry.UpdatedAt)

		objectKey, err := entry.MakeObjectKey()
		require.NoError(err)
		require.Equal("1/logs/123123/555", objectKey)

		pk, err := entry.GetPartitionKey()
		require.NoError(err)
		require.Equal("1#logs#123123", pk)

		sk, err := entry.GetSortKey()
		require.NoError(err)
		require.Equal("000000000000022b", sk)

		actualData, err := entry.GetData()
		require.NoError(err)
		require.Equal(compressed, actualData)

		var actualLogs ethereum.Logs
		err = entry.AsAPI(&actualLogs)
		require.NoError(err)
		require.False(actualLogs.UpdatedAt.IsZero())
		logs.UpdatedAt = actualLogs.UpdatedAt
		require.Equal(logs, &actualLogs)
	}
}
