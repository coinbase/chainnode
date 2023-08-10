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

type TraceByNumberDDBEntry struct {
	*collection.BaseItem

	Hash      string `dynamodbav:"hash"`
	Height    uint64 `dynamodbav:"height"`
	BlockTime string `dynamodbav:"block_time"`
}

var _ collection.Item = (*TraceByNumberDDBEntry)(nil)

func MakeTraceByNumberDDBEntry(t *ethereum.Trace) (*TraceByNumberDDBEntry, error) {
	data, err := compression.Compress(t.Data, compression.CompressionGzip)
	if err != nil {
		return nil, xerrors.Errorf("failed to compress the data: %w", err)
	}

	blockTime := timeutil.TimeToISO8601(t.BlockTime)

	entry := &TraceByNumberDDBEntry{
		BaseItem: collection.NewBaseItem(
			MakeTraceByNumberPartitionKey(t.Tag, t.Height),
			MakeTraceByNumberSortKey(t.Sequence),
			t.Tag,
		).WithData(data),
		Hash:      t.Hash,
		Height:    t.Height,
		BlockTime: blockTime,
	}
	return entry, nil
}

func MakeTraceByNumberPartitionKey(tag uint32, height uint64) string {
	return fmt.Sprintf("%d#traces-by-number#%d", tag, height)
}

func MakeTraceByNumberSortKey(sequence api.Sequence) string {
	return sequence.AsPaddedHex()
}

func (e *TraceByNumberDDBEntry) MakeObjectKey() (string, error) {
	sequence, err := e.getSequence()
	if err != nil {
		return "", xerrors.Errorf("failed to get object key: %w", err)
	}

	return fmt.Sprintf("%d/traces-by-number/%d/%s", e.Tag, e.Height, sequence.AsDecimal()), nil
}

func (e *TraceByNumberDDBEntry) getSequence() (api.Sequence, error) {
	return api.ParsePaddedHexSequence(e.SortKey)
}

func (e *TraceByNumberDDBEntry) AsAPI(value interface{}) error {
	sequence, err := e.getSequence()
	if err != nil {
		return xerrors.Errorf("failed to parse sequence from TraceByNumberDDBEntry: %w", err)
	}

	data, err := compression.Decompress(e.Data, compression.CompressionGzip)
	if err != nil {
		return xerrors.Errorf("failed to decompress data from TraceByNumberDDBEntry: %w", err)
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
