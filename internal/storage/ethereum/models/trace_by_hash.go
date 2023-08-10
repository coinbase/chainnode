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

type TraceByHashDDBEntry struct {
	*collection.BaseItem

	Hash      string `dynamodbav:"hash"`
	Height    uint64 `dynamodbav:"height"`
	BlockTime string `dynamodbav:"block_time"`
}

var _ collection.Item = (*TraceByHashDDBEntry)(nil)

func MakeTraceByHashDDBEntry(t *ethereum.Trace) (*TraceByHashDDBEntry, error) {
	data, err := compression.Compress(t.Data, compression.CompressionGzip)
	if err != nil {
		return nil, xerrors.Errorf("failed to compress the data: %w", err)
	}

	blockTime := timeutil.TimeToISO8601(t.BlockTime)

	entry := &TraceByHashDDBEntry{
		BaseItem: collection.NewBaseItem(
			MakeTraceByHashPartitionKey(t.Tag, t.Hash),
			MakeTraceByHashSortKey(t.Sequence),
			t.Tag,
		).WithData(data),
		Hash:      t.Hash,
		Height:    t.Height,
		BlockTime: blockTime,
	}
	return entry, nil
}

func MakeTraceByHashPartitionKey(tag uint32, hash string) string {
	return fmt.Sprintf("%d#traces-by-hash#%s", tag, hash)
}

func MakeTraceByHashSortKey(sequence api.Sequence) string {
	return sequence.AsPaddedHex()
}

func (e *TraceByHashDDBEntry) MakeObjectKey() (string, error) {
	sequence, err := e.getSequence()
	if err != nil {
		return "", xerrors.Errorf("failed to get object key: %w", err)
	}

	return fmt.Sprintf("%d/traces-by-hash/%s/%s", e.Tag, e.Hash, sequence.AsDecimal()), nil
}

func (e *TraceByHashDDBEntry) getSequence() (api.Sequence, error) {
	return api.ParsePaddedHexSequence(e.SortKey)
}

func (e *TraceByHashDDBEntry) AsAPI(value interface{}) error {
	sequence, err := e.getSequence()
	if err != nil {
		return xerrors.Errorf("failed to parse sequence from TraceByHashDDBEntry: %w", err)
	}

	data, err := compression.Decompress(e.Data, compression.CompressionGzip)
	if err != nil {
		return xerrors.Errorf("failed to decompress data from TraceByHashDDBEntry: %w", err)
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
