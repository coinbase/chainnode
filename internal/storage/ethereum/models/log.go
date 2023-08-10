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

type LogsDDBEntry struct {
	*collection.BaseItem

	Height    uint64 `dynamodbav:"height"`
	Hash      string `dynamodbav:"hash"`
	LogsBloom string `dynamodbav:"logs_bloom"`
	BlockTime string `dynamodbav:"block_time"`
}

const LogsBloomName = "logs_bloom"

var _ collection.Item = (*LogsDDBEntry)(nil)

func MakeLogsDDBEntry(l *ethereum.Logs) (*LogsDDBEntry, error) {
	data, err := compression.Compress(l.Data, compression.CompressionGzip)
	if err != nil {
		return nil, xerrors.Errorf("failed to compress the data: %w", err)
	}

	blockTime := timeutil.TimeToISO8601(l.BlockTime)

	entry := &LogsDDBEntry{
		BaseItem: collection.NewBaseItem(
			MakeLogsPartitionKey(l.Tag, l.Height),
			MakeLogsSortKey(l.Sequence),
			l.Tag,
		).WithData(data),
		Height:    l.Height,
		Hash:      l.Hash,
		LogsBloom: l.LogsBloom,
		BlockTime: blockTime,
	}
	return entry, nil
}

func MakeLogsPartitionKey(tag uint32, height uint64) string {
	return fmt.Sprintf("%d#logs#%d", tag, height)
}

func MakeLogsSortKey(sequence api.Sequence) string {
	return sequence.AsPaddedHex()
}

func (e *LogsDDBEntry) MakeObjectKey() (string, error) {
	sequence, err := e.getSequence()
	if err != nil {
		return "", xerrors.Errorf("failed to get object key: %w", err)
	}

	return fmt.Sprintf("%d/logs/%d/%s", e.Tag, e.Height, sequence.AsDecimal()), nil
}

func (e *LogsDDBEntry) getSequence() (api.Sequence, error) {
	return api.ParsePaddedHexSequence(e.SortKey)
}

func (e *LogsDDBEntry) AsAPI(value interface{}) error {
	sequence, err := e.getSequence()
	if err != nil {
		return xerrors.Errorf("failed to parse sequence from LogsDDBEntry: %w", err)
	}

	data, err := compression.Decompress(e.Data, compression.CompressionGzip)
	if err != nil {
		return xerrors.Errorf("failed to decompress data from LogsDDBEntry: %w", err)
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

	err = reflectutil.Populate(value, &ethereum.Logs{
		Tag:       e.Tag,
		Sequence:  sequence,
		Height:    e.Height,
		Hash:      e.Hash,
		LogsBloom: e.LogsBloom,
		Data:      data,
		BlockTime: blockTime,
		UpdatedAt: updatedAt,
	})
	if err != nil {
		return xerrors.Errorf("failed to populate the value: %w", err)
	}

	return nil
}
