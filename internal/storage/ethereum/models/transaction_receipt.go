package models

import (
	"fmt"

	"golang.org/x/xerrors"

	"github.com/coinbase/chainnode/internal/api"
	"github.com/coinbase/chainnode/internal/api/ethereum"
	"github.com/coinbase/chainnode/internal/storage/collection"
	"github.com/coinbase/chainnode/internal/utils/compression"
	"github.com/coinbase/chainnode/internal/utils/reflectutil"
	"github.com/coinbase/chainnode/internal/utils/timeutil"
)

type TransactionReceiptDDBEntry struct {
	*collection.BaseItem

	Hash      string `dynamodbav:"hash"`
	BlockTime string `dynamodbav:"block_time"`
}

var _ collection.Item = (*TransactionReceiptDDBEntry)(nil)

func MakeTransactionReceiptDDBEntry(t *ethereum.TransactionReceipt) (*TransactionReceiptDDBEntry, error) {
	data, err := compression.Compress(t.Data, compression.CompressionGzip)
	if err != nil {
		return nil, xerrors.Errorf("failed to compress the data: %w", err)
	}

	blockTime := ""
	if !t.BlockTime.IsZero() {
		blockTime = timeutil.TimeToISO8601(t.BlockTime)
	}

	entry := &TransactionReceiptDDBEntry{
		BaseItem: collection.NewBaseItem(
			MakeTransactionReceiptPartitionKey(t.Tag, t.Hash),
			MakeTransactionReceiptSortKey(t.Sequence),
			t.Tag,
		).WithData(data),
		Hash:      t.Hash,
		BlockTime: blockTime,
	}
	return entry, nil
}

func MakeTransactionReceiptPartitionKey(tag uint32, hash string) string {
	return fmt.Sprintf("%d#%s#%s", tag, ethereum.CollectionTransactionReceipts, hash)
}

func MakeTransactionReceiptSortKey(sequence api.Sequence) string {
	return sequence.AsPaddedHex()
}

func (e *TransactionReceiptDDBEntry) MakeObjectKey() (string, error) {
	sequence, err := e.getSequence()
	if err != nil {
		return "", xerrors.Errorf("failed to make object key: %w", err)
	}
	return fmt.Sprintf("%d/%s/%s/%s", e.Tag, ethereum.CollectionTransactionReceipts, e.Hash, sequence.AsDecimal()), nil
}

func (e *TransactionReceiptDDBEntry) AsAPI(value interface{}) error {
	sequence, err := e.getSequence()
	if err != nil {
		return xerrors.Errorf("failed to parse sequence from TransactionReceiptDDBEntry: %w", err)
	}

	data, err := compression.Decompress(e.Data, compression.CompressionGzip)
	if err != nil {
		return xerrors.Errorf("failed to decompress data from TransactionReceiptDDBEntry: %w", err)
	}

	blockTime, err := e.ParseTimestamp(e.BlockTime, true)
	if err != nil {
		return xerrors.Errorf("unexpected BlockTime in entry: %w", err)
	}

	// TODO: make it required after we backfill the data
	updatedAt, err := e.ParseTimestamp(e.UpdatedAt, true)
	if err != nil {
		return xerrors.Errorf("unexpected UpdatedAt in entry: %w", err)
	}

	err = reflectutil.Populate(value, &ethereum.TransactionReceipt{
		Tag:       e.Tag,
		Hash:      e.Hash,
		Sequence:  sequence,
		Data:      data,
		BlockTime: blockTime,
		UpdatedAt: updatedAt,
	})
	if err != nil {
		return xerrors.Errorf("failed to populate the value: %w", err)
	}

	return nil
}

func (e *TransactionReceiptDDBEntry) getSequence() (api.Sequence, error) {
	return api.ParsePaddedHexSequence(e.SortKey)
}
