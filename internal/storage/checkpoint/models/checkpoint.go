package models

import (
	"fmt"

	"golang.org/x/xerrors"

	"github.com/coinbase/chainnode/internal/storage/collection"

	"github.com/coinbase/chainnode/internal/api"
	"github.com/coinbase/chainnode/internal/utils/reflectutil"
	"github.com/coinbase/chainnode/internal/utils/timeutil"
)

type CheckpointDDBEntry struct {
	*collection.BaseItem

	Sequence      api.Sequence `dynamodbav:"sequence"`
	Height        uint64       `dynamodbav:"height"`
	LastBlockTime string       `dynamodbav:"last_block_time"`
}

const (
	CheckpointTagName           = "tag"
	CheckpointSequenceName      = "sequence"
	CheckpointHeightName        = "height"
	CheckpointLastBlockTimeName = "last_block_time"

	PartitionKeyIndexWithoutSharding   = 0
	PartitionKeyStartIndexWithSharding = 1
)

var _ collection.Item = (*CheckpointDDBEntry)(nil)

func MakeCheckpointDDBEntry(c *api.Checkpoint, partitionKeyIndex int) (*CheckpointDDBEntry, error) {
	blockTime := timeutil.TimeToISO8601(c.LastBlockTime)
	return &CheckpointDDBEntry{
		BaseItem: collection.NewBaseItem(
			MakeCheckpointPartitionKey(c.Tag, partitionKeyIndex),
			MakeCheckpointSortKey(c.Collection),
			c.Tag,
		),
		Sequence:      c.Sequence,
		Height:        c.Height,
		LastBlockTime: blockTime,
	}, nil
}

func MakeCheckpointPartitionKey(tag uint32, index int) string {
	if index == PartitionKeyIndexWithoutSharding {
		return fmt.Sprintf("%v#checkpoints", tag)
	} else {
		return fmt.Sprintf("%v#checkpoints#%v", tag, index)
	}
}

func MakeCheckpointSortKey(collection api.Collection) string {
	return collection.String()
}

func (e *CheckpointDDBEntry) MakeObjectKey() (string, error) {
	return "", api.ErrNotImplemented
}

func (e *CheckpointDDBEntry) GetObjectKey() (string, error) {
	return "", api.ErrNotImplemented
}

func (e *CheckpointDDBEntry) GetData() ([]byte, error) {
	return nil, api.ErrNotImplemented
}

func (e *CheckpointDDBEntry) SetObjectKey(string) error {
	return api.ErrNotImplemented
}

func (e *CheckpointDDBEntry) SetData([]byte) error {
	return api.ErrNotImplemented
}

func (e *CheckpointDDBEntry) AsAPI(value interface{}) error {
	updatedAt, err := e.ParseTimestamp(e.UpdatedAt, true)
	if err != nil {
		return xerrors.Errorf("unexpected UpdatedAt in entry: %w", err)
	}

	lastBlockTime, err := e.ParseTimestamp(e.LastBlockTime, true)
	if err != nil {
		return xerrors.Errorf("unexpected LastBlockTime in entry: %w", err)
	}

	err = reflectutil.Populate(value, &api.Checkpoint{
		Collection:    e.getCollection(),
		Tag:           e.Tag,
		Sequence:      e.Sequence,
		Height:        e.Height,
		LastBlockTime: lastBlockTime,
		UpdatedAt:     updatedAt,
	})
	if err != nil {
		return xerrors.Errorf("failed to populate the value: %w", err)
	}

	return nil
}

func (e *CheckpointDDBEntry) getCollection() api.Collection {
	return api.Collection(e.SortKey)
}

func GetCheckpointKeyMap(tag uint32, col api.Collection, partitionKeyIndex int) (collection.StringMap, error) {
	sortKey := MakeCheckpointSortKey(col)
	return collection.StringMap{
		"pk": MakeCheckpointPartitionKey(tag, partitionKeyIndex),
		"sk": sortKey,
	}, nil
}
