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

type BlockDDBEntry struct {
	*collection.BaseItem

	Height    uint64 `dynamodbav:"height"`
	Hash      string `dynamodbav:"hash"`
	BlockTime string `dynamodbav:"block_time"`
}

const (
	BlockTagName    = "tag"
	BlockHeightName = "height"
	BlockHashName   = "hash"
	BlockTimeName   = "block_time"
)

var _ collection.Item = (*BlockDDBEntry)(nil)

func MakeBlockDDBEntry(b *ethereum.Block) (*BlockDDBEntry, error) {
	data, err := compression.Compress(b.Data, compression.CompressionGzip)
	if err != nil {
		return nil, xerrors.Errorf("failed to compress the data: %w", err)
	}

	blockTime := timeutil.TimeToISO8601(b.BlockTime)

	entry := &BlockDDBEntry{
		BaseItem: collection.NewBaseItem(
			MakeBlockPartitionKey(b.Tag, b.Height),
			MakeBlockSortKey(b.Sequence),
			b.Tag,
		).WithData(data),
		Height:    b.Height,
		Hash:      b.Hash,
		BlockTime: blockTime,
	}
	return entry, nil
}

func MakeBlockPartitionKey(tag uint32, height uint64) string {
	return fmt.Sprintf("%d#blocks#%d", tag, height)
}

func MakeBlockSortKey(sequence api.Sequence) string {
	return sequence.AsPaddedHex()
}

func (e *BlockDDBEntry) MakeObjectKey() (string, error) {
	sequence, err := e.getSequence()
	if err != nil {
		return "", xerrors.Errorf("failed to make object key: %w", err)
	}

	return fmt.Sprintf("%d/blocks/%d/%s", e.Tag, e.Height, sequence.AsDecimal()), nil
}

func (e *BlockDDBEntry) AsAPI(value interface{}) error {
	sequence, err := e.getSequence()
	if err != nil {
		return xerrors.Errorf("failed to parse sequence from BlockDDBEntry: %w", err)
	}

	data, err := compression.Decompress(e.Data, compression.CompressionGzip)
	if err != nil {
		return xerrors.Errorf("failed to decompress data from BlockDDBEntry: %w", err)
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

func (e *BlockDDBEntry) ToBlockMetadata() (*ethereum.BlockMetadata, error) {
	sequence, err := e.getSequence()
	if err != nil {
		return nil, xerrors.Errorf("failed to parse sequence from BlockDDBEntry: %w", err)
	}

	blockTime, err := e.ParseTimestamp(e.BlockTime, true)
	if err != nil {
		return nil, xerrors.Errorf("unexpected BlockTime in entry: %w", err)
	}

	// TODO: make it required after we backfill the data
	updatedAt, err := e.ParseTimestamp(e.UpdatedAt, true)
	if err != nil {
		return nil, xerrors.Errorf("unexpected UpdatedAt in entry: %w", err)
	}

	return &ethereum.BlockMetadata{
		Tag:       e.Tag,
		Height:    e.Height,
		Hash:      e.Hash,
		Sequence:  sequence,
		BlockTime: blockTime,
		UpdatedAt: updatedAt,
	}, nil
}

func (e *BlockDDBEntry) getSequence() (api.Sequence, error) {
	return api.ParsePaddedHexSequence(e.SortKey)
}
