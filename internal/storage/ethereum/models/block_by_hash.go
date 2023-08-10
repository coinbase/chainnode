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

type BlockByHashDDBEntry struct {
	*collection.BaseItem

	Height    uint64 `dynamodbav:"height"`
	Hash      string `dynamodbav:"hash"`
	BlockTime string `dynamodbav:"block_time"`
}

var _ collection.Item = (*BlockByHashDDBEntry)(nil)

func MakeBlockByHashDDBEntry(b *ethereum.Block) (*BlockByHashDDBEntry, error) {
	data, err := compression.Compress(b.Data, compression.CompressionGzip)
	if err != nil {
		return nil, xerrors.Errorf("failed to compress the data: %w", err)
	}

	blockTime := timeutil.TimeToISO8601(b.BlockTime)

	entry := &BlockByHashDDBEntry{
		BaseItem: collection.NewBaseItem(
			MakeBlockByHashPartitionKey(b.Tag, b.Hash),
			MakeBlockByHashSortKey(b.Sequence),
			b.Tag,
		).WithData(data),
		Height:    b.Height,
		Hash:      b.Hash,
		BlockTime: blockTime,
	}
	return entry, nil
}

func MakeBlockByHashPartitionKey(tag uint32, hash string) string {
	return fmt.Sprintf("%d#blocks-by-hash#%s", tag, hash)
}

func MakeBlockByHashSortKey(sequence api.Sequence) string {
	return sequence.AsPaddedHex()
}

func (e *BlockByHashDDBEntry) MakeObjectKey() (string, error) {
	sequence, err := e.getSequence()
	if err != nil {
		return "", xerrors.Errorf("failed to make object key: %w", err)
	}

	return fmt.Sprintf("%d/blocks-by-hash/%s/%s", e.Tag, e.Hash, sequence.AsDecimal()), nil
}

func (e *BlockByHashDDBEntry) AsAPI(value interface{}) error {
	sequence, err := e.getSequence()
	if err != nil {
		return xerrors.Errorf("failed to parse sequence from BlockByHashDDBEntry: %w", err)
	}

	data, err := compression.Decompress(e.Data, compression.CompressionGzip)
	if err != nil {
		return xerrors.Errorf("failed to decompress data from BlockByHashDDBEntry: %w", err)
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

func (e *BlockByHashDDBEntry) getSequence() (api.Sequence, error) {
	return api.ParsePaddedHexSequence(e.SortKey)
}
