package internal

import (
	"context"

	"go.uber.org/fx"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainnode/internal/api"
	"github.com/coinbase/chainnode/internal/storage"
)

type (
	Checkpointer interface {
		Get(ctx context.Context, collection api.Collection, tag uint32) (*api.Checkpoint, error)
		GetEarliest(ctx context.Context, collections []api.Collection, tag uint32) (*api.Checkpoint, error)
		Set(ctx context.Context, checkpoint *api.Checkpoint) error
		GetInitialCheckpoint(collection api.Collection, tag uint32) *api.Checkpoint
	}

	CheckpointerParams struct {
		fx.In
		Storage storage.CheckpointStorage
	}

	checkpointer struct {
		storage storage.CheckpointStorage
	}
)

func NewCheckpointer(params CheckpointerParams) Checkpointer {
	return &checkpointer{
		storage: params.Storage,
	}
}

func (c *checkpointer) Get(ctx context.Context, collection api.Collection, tag uint32) (*api.Checkpoint, error) {
	if tag == 0 {
		return nil, xerrors.Errorf("tag is not specified")
	}

	checkpoint, err := c.storage.GetCheckpoint(ctx, collection, tag)
	if err != nil {
		if xerrors.Is(err, storage.ErrItemNotFound) {
			return c.GetInitialCheckpoint(collection, tag), nil
		}

		return nil, xerrors.Errorf("failed to get checkpoint (collection=%v, tag=%v): %w", collection, tag, err)
	}

	if checkpoint.Empty() {
		return c.GetInitialCheckpoint(collection, tag), nil
	}

	return checkpoint, nil
}

func (c *checkpointer) Set(ctx context.Context, checkpoint *api.Checkpoint) error {
	if err := c.storage.PersistCheckpoint(ctx, checkpoint); err != nil {
		return xerrors.Errorf("failed to persist checkpoint (checkpoint=%+v): %w", checkpoint, err)
	}

	return nil
}

func (c *checkpointer) GetEarliest(ctx context.Context, collections []api.Collection, tag uint32) (*api.Checkpoint, error) {
	if tag == 0 {
		return nil, xerrors.Errorf("tag is not specified")
	}

	checkpoints, err := c.storage.GetCheckpoints(ctx, collections, tag)
	if err != nil {
		if xerrors.Is(err, storage.ErrItemNotFound) {
			return c.GetInitialCheckpoint(collections[0], tag), nil
		}

		return nil, xerrors.Errorf("failed to get checkpoints (collections=%v, tag=%v): %w", collections, tag, err)
	}

	if len(checkpoints) == 0 {
		return c.GetInitialCheckpoint(collections[0], tag), nil
	}

	checkpoint := checkpoints[0]
	for i := 1; i < len(checkpoints); i++ {
		// get the smallest checkpoint across the collections
		if checkpoints[i].Sequence < checkpoint.Sequence {
			checkpoint = checkpoints[i]
		}
	}

	if checkpoint.Empty() {
		return c.GetInitialCheckpoint(checkpoint.Collection, tag), nil
	}

	return checkpoint, nil
}

func (c *checkpointer) GetInitialCheckpoint(collection api.Collection, tag uint32) *api.Checkpoint {
	return api.NewCheckpoint(collection, tag, api.InitialSequence, 0)
}
