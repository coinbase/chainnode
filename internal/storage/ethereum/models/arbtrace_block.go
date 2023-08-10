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

type ArbtraceBlockDDBEntry struct {
	*collection.BaseItem

	Hash      string `dynamodbav:"hash"`
	Height    uint64 `dynamodbav:"height"`
	BlockTime string `dynamodbav:"block_time"`
}

var _ collection.Item = (*ArbtraceBlockDDBEntry)(nil)

func MakeArbtraceBlockDDBEntry(t *ethereum.Trace) (*ArbtraceBlockDDBEntry, error) {
	data, err := compression.Compress(t.Data, compression.CompressionGzip)
	if err != nil {
		return nil, xerrors.Errorf("failed to compress the data: %w", err)
	}

	blockTime := timeutil.TimeToISO8601(t.BlockTime)

	entry := &ArbtraceBlockDDBEntry{
		BaseItem: collection.NewBaseItem(
			MakeArbtraceBlockPartitionKey(t.Tag, t.Height),
			MakeArbtraceBlockSortKey(t.Sequence),
			t.Tag,
		).WithData(data),
		Hash:      t.Hash,
		Height:    t.Height,
		BlockTime: blockTime,
	}
	return entry, nil
}

func MakeArbtraceBlockPartitionKey(tag uint32, height uint64) string {
	return fmt.Sprintf("%d#arbtrace-block#%d", tag, height)
}

func MakeArbtraceBlockSortKey(sequence api.Sequence) string {
	return sequence.AsPaddedHex()
}

func (e *ArbtraceBlockDDBEntry) MakeObjectKey() (string, error) {
	sequence, err := e.getSequence()
	if err != nil {
		return "", xerrors.Errorf("failed to get object key: %w", err)
	}

	return fmt.Sprintf("%d/arbtrace-block/%d/%s", e.Tag, e.Height, sequence.AsDecimal()), nil
}

func (e *ArbtraceBlockDDBEntry) getSequence() (api.Sequence, error) {
	return api.ParsePaddedHexSequence(e.SortKey)
}

func (e *ArbtraceBlockDDBEntry) AsAPI(value interface{}) error {
	sequence, err := e.getSequence()
	if err != nil {
		return xerrors.Errorf("failed to parse sequence from ArbtraceBlockDDBEntry: %w", err)
	}

	data, err := compression.Decompress(e.Data, compression.CompressionGzip)
	if err != nil {
		return xerrors.Errorf("failed to decompress data from ArbtraceBlockDDBEntry: %w", err)
	}

	blockTime, err := e.ParseTimestamp(e.BlockTime, true)
	if err != nil {
		return xerrors.Errorf("unexpected BlockTime in entry: %w", err)
	}

	updatedAt, err := e.ParseTimestamp(e.UpdatedAt, false)
	if err != nil {
		return xerrors.Errorf("unexpected UpdatedAt in entry: %w", err)
	}

	err = reflectutil.Populate(value, &ethereum.Trace{
		Tag:       e.Tag,
		Sequence:  sequence,
		Height:    e.Height,
		Hash:      e.Hash,
		Data:      data,
		BlockTime: blockTime,
		UpdatedAt: updatedAt,
	})
	if err != nil {
		return xerrors.Errorf("failed to populate the value: %w", err)
	}

	return nil
}
