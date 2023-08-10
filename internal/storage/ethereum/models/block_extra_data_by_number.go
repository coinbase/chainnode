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

type BlockExtraDataByNumberDDBEntry struct {
	*collection.BaseItem

	Height    uint64 `dynamodbav:"height"`
	Hash      string `dynamodbav:"hash"`
	BlockTime string `dynamodbav:"block_time"`
}

var _ collection.Item = (*BlockExtraDataByNumberDDBEntry)(nil)

func MakeBlockExtraDataByNumberDDBEntry(b *ethereum.BlockExtraData) (*BlockExtraDataByNumberDDBEntry, error) {
	data, err := compression.Compress(b.Data, compression.CompressionGzip)
	if err != nil {
		return nil, xerrors.Errorf("failed to compress the data: %w", err)
	}

	blockTime := timeutil.TimeToISO8601(b.BlockTime)

	entry := &BlockExtraDataByNumberDDBEntry{
		BaseItem: collection.NewBaseItem(
			MakeBlockExtraDataByNumberPartitionKey(b.Tag, b.Height),
			MakeBlockExtraDataByNumberSortKey(b.Sequence),
			b.Tag,
		).WithData(data),
		Height:    b.Height,
		Hash:      b.Hash,
		BlockTime: blockTime,
	}
	return entry, nil
}

func MakeBlockExtraDataByNumberPartitionKey(tag uint32, height uint64) string {
	return fmt.Sprintf("%d#blocks-extra-data-by-number#%d", tag, height)
}

func MakeBlockExtraDataByNumberSortKey(sequence api.Sequence) string {
	return sequence.AsPaddedHex()
}

func (e *BlockExtraDataByNumberDDBEntry) MakeObjectKey() (string, error) {
	sequence, err := e.getSequence()
	if err != nil {
		return "", xerrors.Errorf("failed to make object key: %w", err)
	}

	return fmt.Sprintf("%d/blocks-extra-data-by-number/%d/%s", e.Tag, e.Height, sequence.AsDecimal()), nil
}

func (e *BlockExtraDataByNumberDDBEntry) AsAPI(value interface{}) error {
	sequence, err := e.getSequence()
	if err != nil {
		return xerrors.Errorf("failed to parse sequence from BlockExtraDataByNumberDDBEntry: %w", err)
	}

	data, err := compression.Decompress(e.Data, compression.CompressionGzip)
	if err != nil {
		return xerrors.Errorf("failed to decompress data from BlockExtraDataByNumberDDBEntry: %w", err)
	}

	blockTime, err := e.ParseTimestamp(e.BlockTime, true)
	if err != nil {
		return xerrors.Errorf("unexpected BlockTime in entry: %w", err)
	}

	updatedAt, err := e.ParseTimestamp(e.UpdatedAt, false)
	if err != nil {
		return xerrors.Errorf("unexpected UpdatedAt in entry: %w", err)
	}

	err = reflectutil.Populate(value, &ethereum.BlockExtraData{
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

func (e *BlockExtraDataByNumberDDBEntry) getSequence() (api.Sequence, error) {
	return api.ParsePaddedHexSequence(e.SortKey)
}
