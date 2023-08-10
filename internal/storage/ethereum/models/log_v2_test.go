package models

import (
	"testing"
	"time"

	"github.com/coinbase/chainnode/internal/api"
	"github.com/coinbase/chainnode/internal/api/ethereum"
	"github.com/coinbase/chainnode/internal/utils/compression"
	"github.com/coinbase/chainnode/internal/utils/testutil"
)

func TestLogsV2(t *testing.T) {
	require := testutil.Require(t)
	tag := uint32(1)
	height := uint64(123123)
	hash := "0x1"
	sequence := api.Sequence(555)
	data := []byte("test-data")
	logsBloom := "0x231"
	partitionSize := uint64(500)
	compressed, err := compression.Compress(data, compression.CompressionGzip)
	require.NoError(err)

	require.Equal("1#logs-v2#123123", MakeLogsV2PartitionKey(tag, height))
	require.Equal("000000000000022b", MakeLogsV2SortKey(sequence))

	blockTimes := []time.Time{
		testutil.MustTime("2020-11-24T16:07:21Z"),
		{},
	}

	for _, blockTime := range blockTimes {
		logs := ethereum.NewLogs(tag, height, hash, sequence, logsBloom, data, blockTime)
		entry, err := MakeLogsV2DDBEntry(logs, partitionSize)
		require.NoError(err)
		require.NotNil(entry)
		testutil.MustTime(entry.UpdatedAt)

		objectKey, err := entry.MakeObjectKey()
		require.NoError(err)
		require.Equal("1/logs-v2/123123/555", objectKey)

		pk, err := entry.GetPartitionKey()
		require.NoError(err)
		require.Equal("1#logs-v2#123123", pk)

		sk, err := entry.GetSortKey()
		require.NoError(err)
		require.Equal("000000000000022b", sk)

		actualData, err := entry.GetData()
		require.NoError(err)
		require.Equal(compressed, actualData)

		require.Equal("1#00000000000000f6", entry.IndexPartitionKey)
		require.Equal("000000000001e0f3", entry.IndexSortKey)

		var actualLogs ethereum.Logs
		err = entry.AsAPI(&actualLogs)
		require.NoError(err)
		require.False(actualLogs.UpdatedAt.IsZero())
		logs.UpdatedAt = actualLogs.UpdatedAt
		require.Equal(logs, &actualLogs)

		entryLite := &LogsLiteDDBEntry{
			PartitionKey: pk,
			SortKey:      sk,
			LogsBloom:    logsBloom,
		}
		var actualLogsLite ethereum.LogsLite
		err = entryLite.AsLogsLite(&actualLogsLite)
		require.NoError(err)
		require.Equal(tag, actualLogsLite.Tag)
		require.Equal(height, actualLogsLite.Height)
		require.Equal(sequence, actualLogsLite.Sequence)
		require.Equal(logsBloom, actualLogsLite.LogsBloom)
	}
}
