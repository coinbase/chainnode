package models

import (
	"testing"
	"time"

	"github.com/coinbase/chainnode/internal/api"
	"github.com/coinbase/chainnode/internal/api/ethereum"
	"github.com/coinbase/chainnode/internal/utils/compression"
	"github.com/coinbase/chainnode/internal/utils/testutil"
)

func TestTransactionReceipt(t *testing.T) {
	require := testutil.Require(t)
	tag := uint32(1)
	sequence := api.Sequence(555)
	hash := "0xo123"
	data := []byte("test-data")
	compressed, err := compression.Compress(data, compression.CompressionGzip)
	require.NoError(err)

	require.Equal("1#transaction-receipts#0xo123", MakeTransactionReceiptPartitionKey(tag, hash))
	require.Equal("000000000000022b", MakeTransactionReceiptSortKey(sequence))

	blockTimes := []time.Time{
		testutil.MustTime("2020-11-24T16:07:21Z"),
		{},
	}

	for _, blockTime := range blockTimes {
		receipt := ethereum.NewTransactionReceipt(tag, hash, sequence, data, blockTime)
		entry, err := MakeTransactionReceiptDDBEntry(receipt)
		require.NoError(err)
		require.NotNil(entry)
		testutil.MustTime(entry.UpdatedAt)

		objectKey, err := entry.MakeObjectKey()
		require.NoError(err)
		require.Equal("1/transaction-receipts/0xo123/555", objectKey)

		pk, err := entry.GetPartitionKey()
		require.NoError(err)
		require.Equal("1#transaction-receipts#0xo123", pk)

		sk, err := entry.GetSortKey()
		require.NoError(err)
		require.Equal("000000000000022b", sk)

		actualData, err := entry.GetData()
		require.NoError(err)
		require.Equal(compressed, actualData)

		var actualReceipt ethereum.TransactionReceipt
		err = entry.AsAPI(&actualReceipt)
		require.NoError(err)
		require.False(actualReceipt.UpdatedAt.IsZero())
		receipt.UpdatedAt = actualReceipt.UpdatedAt
		require.Equal(receipt, &actualReceipt)
	}
}
