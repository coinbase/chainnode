package api

import (
	"time"

	"github.com/golang/protobuf/ptypes/timestamp"
)

type Checkpoint struct {
	Collection    Collection
	Tag           uint32
	Sequence      Sequence
	Height        uint64
	LastBlockTime time.Time

	UpdatedAt time.Time // set by storage
}

func NewCheckpoint(
	collection Collection,
	tag uint32,
	sequence Sequence,
	height uint64,
) *Checkpoint {
	return &Checkpoint{
		Collection: collection,
		Tag:        tag,
		Sequence:   sequence,
		Height:     height,
	}
}

func (c *Checkpoint) WithLastBlockTime(time time.Time) *Checkpoint {
	c.LastBlockTime = time
	return c
}

func (c *Checkpoint) WithLastBlockTimestamp(timestamp *timestamp.Timestamp) *Checkpoint {
	if timestamp.GetSeconds() != 0 || timestamp.GetNanos() != 0 {
		c.LastBlockTime = timestamp.AsTime()
	}
	return c
}

func (c *Checkpoint) Empty() bool {
	return c.Tag == 0 || c.Sequence == InitialSequence
}

func (c *Checkpoint) Present() bool {
	return !c.Empty()
}

func (c *Checkpoint) IsGlobal() bool {
	return c.Collection.IsGlobal()
}
