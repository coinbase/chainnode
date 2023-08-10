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

type BlockByHashWithoutFullTxDDBEntry struct {
	*collection.BaseItem

	Height    uint64 `dynamodbav:"height"`
	Hash      string `dynamodbav:"hash"`
	BlockTime string `dynamodbav:"block_time"`
}

var _ collection.Item = (*BlockByHashWithoutFullTxDDBEntry)(nil)

func MakeBlockByHashWithoutFullTxDDBEntry(b *ethereum.Block) (*BlockByHashWithoutFullTxDDBEntry, error) {
	data, err := compression.Compress(b.Data, compression.CompressionGzip)
	if err != nil {
		return nil, xerrors.Errorf("failed to compress the data: %w", err)
	}

	blockTime := timeutil.TimeToISO8601(b.BlockTime)

	entry := &BlockByHashWithoutFullTxDDBEntry{
		BaseItem: collection.NewBaseItem(
			MakeBlockByHashWithoutFullTxPartitionKey(b.Tag, b.Hash),
			MakeBlockByHashWithoutFullTxSortKey(b.Sequence),
			b.Tag,
		).WithData(data),
		Height:    b.Height,
		Hash:      b.Hash,
		BlockTime: blockTime,
	}
	return entry, nil
}

func MakeBlockByHashWithoutFullTxPartitionKey(tag uint32, hash string) string {
	return fmt.Sprintf("%d#blocks-by-hash-without-full-tx#%s", tag, hash)
}

func MakeBlockByHashWithoutFullTxSortKey(sequence api.Sequence) string {
	return sequence.AsPaddedHex()
}

func (e *BlockByHashWithoutFullTxDDBEntry) MakeObjectKey() (string, error) {
	sequence, err := e.getSequence()
	if err != nil {
		return "", xerrors.Errorf("failed to make object key: %w", err)
	}

	return fmt.Sprintf("%d/blocks-by-hash-without-full-tx/%s/%s", e.Tag, e.Hash, sequence.AsDecimal()), nil
}

func (e *BlockByHashWithoutFullTxDDBEntry) AsAPI(value interface{}) error {
	sequence, err := e.getSequence()
	if err != nil {
		return xerrors.Errorf("failed to parse sequence from BlockByHashWithoutFullTxDDBEntry: %w", err)
	}

	data, err := compression.Decompress(e.Data, compression.CompressionGzip)
	if err != nil {
		return xerrors.Errorf("failed to decompress data from BlockByHashWithoutFullTxDDBEntry: %w", err)
	}

	blockTime, err := e.ParseTimestamp(e.BlockTime, true)
	if err != nil {
		return xerrors.Errorf("unexpected BlockTime in entry: %w", err)
	}

	updatedAt, err := e.ParseTimestamp(e.UpdatedAt, false)
	if err != nil {
		return xerrors.Errorf("unexpected UpdatedAt in entry: %w", err)
	}

	err = reflectutil.Populate(value, &ethereum.Block{
		Tag:       e.Tag,
		Height:    e.Height,
		Hash:      e.Hash,
		Data:      data,
		Sequence:  sequence,
		BlockTime: blockTime,
		UpdatedAt: updatedAt,
	})
	if err != nil {
		return xerrors.Errorf("failed to populate the value: %w", err)
	}

	return nil
}

func (e *BlockByHashWithoutFullTxDDBEntry) getSequence() (api.Sequence, error) {
	return api.ParsePaddedHexSequence(e.SortKey)
}
