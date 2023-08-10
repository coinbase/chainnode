package models

import (
	"fmt"
	"strconv"
	"strings"

	"golang.org/x/xerrors"

	"github.com/coinbase/chainnode/internal/api"
	"github.com/coinbase/chainnode/internal/api/ethereum"
	"github.com/coinbase/chainnode/internal/storage/collection"
	"github.com/coinbase/chainnode/internal/utils/compression"
	"github.com/coinbase/chainnode/internal/utils/reflectutil"
	"github.com/coinbase/chainnode/internal/utils/timeutil"
)

type LogsV2DDBEntry struct {
	*collection.BaseItem

	// used for gsi: logs_by_block_range
	IndexPartitionKey string `dynamodbav:"bbr_pk"`
	IndexSortKey      string `dynamodbav:"bbr_sk"`

	Hash      string `dynamodbav:"hash"`
	Height    uint64 `dynamodbav:"height"`
	LogsBloom string `dynamodbav:"logs_bloom"`
	BlockTime string `dynamodbav:"block_time"`
}

// LogsLiteDDBEntry a light version for LogsV2DDBEntry in order to speed up query time
type LogsLiteDDBEntry struct {
	PartitionKey string `dynamodbav:"pk"`
	SortKey      string `dynamodbav:"sk"`
	LogsBloom    string `dynamodbav:"logs_bloom"`
}

var _ collection.Item = (*LogsV2DDBEntry)(nil)

const (
	LogsByBlockRangePartitionKeyName = "bbr_pk"
	LogsByBlockRangeSortKeyName      = "bbr_sk"
)

func MakeLogsV2DDBEntry(l *ethereum.Logs, partitionSize uint64) (*LogsV2DDBEntry, error) {
	data, err := compression.Compress(l.Data, compression.CompressionGzip)
	if err != nil {
		return nil, xerrors.Errorf("failed to compress the data: %w", err)
	}

	blockTime := timeutil.TimeToISO8601(l.BlockTime)

	entry := &LogsV2DDBEntry{
		BaseItem: collection.NewBaseItem(
			MakeLogsV2PartitionKey(l.Tag, l.Height),
			MakeLogsV2SortKey(l.Sequence),
			l.Tag,
		).WithData(data),
		IndexPartitionKey: MakeLogsV2IndexPartitionKey(l.Tag, l.Height, partitionSize),
		IndexSortKey:      MakeLogsV2IndexSortKey(l.Height),
		Height:            l.Height,
		Hash:              l.Hash,
		LogsBloom:         l.LogsBloom,
		BlockTime:         blockTime,
	}
	return entry, nil
}

func MakeLogsV2PartitionKey(tag uint32, height uint64) string {
	return fmt.Sprintf("%d#logs-v2#%d", tag, height)
}

func MakeLogsV2SortKey(sequence api.Sequence) string {
	return sequence.AsPaddedHex()
}

func MakeLogsV2IndexPartitionKey(tag uint32, height uint64, partitionSize uint64) string {
	partitionIndex := height / partitionSize
	return fmt.Sprintf("%d#%s", tag, heightToPaddedHex(partitionIndex))
}

func MakeLogsV2IndexSortKey(height uint64) string {
	return heightToPaddedHex(height)
}

func (e *LogsV2DDBEntry) MakeObjectKey() (string, error) {
	sequence, err := e.getSequence()
	if err != nil {
		return "", xerrors.Errorf("failed to get object key: %w", err)
	}

	return fmt.Sprintf("%d/logs-v2/%d/%s", e.Tag, e.Height, sequence.AsDecimal()), nil
}

func (e *LogsV2DDBEntry) getSequence() (api.Sequence, error) {
	return api.ParsePaddedHexSequence(e.SortKey)
}

func heightToPaddedHex(height uint64) string {
	index := api.Index(height)
	return index.AsPaddedHex()
}

func (e *LogsV2DDBEntry) AsAPI(value interface{}) error {
	sequence, err := e.getSequence()
	if err != nil {
		return xerrors.Errorf("failed to parse sequence from LogsV2DDBEntry: %w", err)
	}

	data, err := compression.Decompress(e.Data, compression.CompressionGzip)
	if err != nil {
		return xerrors.Errorf("failed to decompress data from LogsV2DDBEntry: %w", err)
	}

	blockTime, err := e.ParseTimestamp(e.BlockTime, true)
	if err != nil {
		return xerrors.Errorf("unexpected BlockTime in entry: %w", err)
	}

	updatedAt, err := e.ParseTimestamp(e.UpdatedAt, false)
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

func (e *LogsLiteDDBEntry) AsLogsLite(value interface{}) error {
	sequence, err := api.ParsePaddedHexSequence(e.SortKey)
	if err != nil {
		return xerrors.Errorf("failed to parse sequence from LogsLiteDDBEntry: %w", err)
	}

	pkStrings := strings.Split(e.PartitionKey, "#")
	if len(pkStrings) != 3 {
		return xerrors.Errorf("invalid pk value: %w", err)
	}

	height, err := strconv.ParseUint(pkStrings[2], 10, 64)
	if err != nil {
		return xerrors.Errorf("failed to parse height from LogsLiteDDBEntry: %w", err)
	}

	tag, err := strconv.ParseUint(pkStrings[0], 10, 32)
	if err != nil {
		return xerrors.Errorf("failed to parse tag from pk")
	}

	err = reflectutil.Populate(value, &ethereum.LogsLite{
		Tag:       uint32(tag),
		Sequence:  sequence,
		Height:    height,
		LogsBloom: e.LogsBloom,
	})
	if err != nil {
		return xerrors.Errorf("failed to populate the value: %w", err)
	}

	return nil
}
