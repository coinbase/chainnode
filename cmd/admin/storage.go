package main

import (
	"time"

	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainnode/internal/api"
	"github.com/coinbase/chainnode/internal/controller"
)

var (
	storageCommand = NewCommand("storage", nil)

	checkpointFlags struct {
		collection string
		tag        uint32
		sequence   int64
		height     uint64
	}

	checkpointCommand = NewCommand("checkpoint", nil)

	checkpointGetCommand = NewCommand("get", func() error {
		var deps struct {
			fx.In
			Checkpointer controller.Checkpointer
		}

		app, err := NewApp(fx.Populate(&deps))
		if err != nil {
			return xerrors.Errorf("failed to create command: %w", err)
		}
		defer app.Close()

		ctx := app.Manager.Context()
		collection := checkpointFlags.collection
		tag := app.Config.Tag.GetEffectiveTag(checkpointFlags.tag)

		checkpoint, err := deps.Checkpointer.Get(ctx, api.Collection(collection), tag)
		if err != nil {
			return xerrors.Errorf("failed to get checkpoint: %w", err)
		}

		if checkpoint.Empty() {
			return xerrors.New("failed to find checkpoint")
		}

		app.Logger.Info("found checkpoint", zap.Reflect("checkpoint", checkpoint))
		return nil
	})

	checkpointSetCommand = NewCommand("set", func() error {
		var deps struct {
			fx.In
			Checkpointer controller.Checkpointer
		}

		app, err := NewApp(fx.Populate(&deps))
		if err != nil {
			return xerrors.Errorf("failed to create command: %w", err)
		}
		defer app.Close()

		ctx := app.Manager.Context()
		collection := api.Collection(checkpointFlags.collection)
		tag := app.Config.Tag.GetEffectiveTag(checkpointFlags.tag)

		if app.Config.IsCollectionSynchronized(collection) {
			return xerrors.New("cannot update the checkpoint for a synchronized collection")
		}

		sequence := api.Sequence(checkpointFlags.sequence)
		height := checkpointFlags.height

		checkpoint, err := deps.Checkpointer.Get(ctx, collection, tag)
		if err != nil {
			return xerrors.Errorf("failed to get checkpoint: %w", err)
		}

		if checkpoint.Empty() {
			return xerrors.New("failed to get checkpoint")
		}

		newCheckpoint := &api.Checkpoint{
			Collection:    checkpoint.Collection,
			Tag:           checkpoint.Tag,
			Sequence:      sequence,
			Height:        height,
			LastBlockTime: time.Time{},
			UpdatedAt:     time.Time{},
		}
		app.Logger.Info(
			"setting checkpoint",
			zap.Reflect("from", checkpoint),
			zap.Reflect("to", newCheckpoint),
		)

		if !app.Confirm("Are you sure you want to set checkpoint?") {
			return nil
		}

		if err := deps.Checkpointer.Set(ctx, newCheckpoint); err != nil {
			return xerrors.Errorf("failed to set checkpoint: %w", err)
		}

		return nil
	})

	checkpointResetCommand = NewCommand("reset", func() error {
		var deps struct {
			fx.In
			Checkpointer controller.Checkpointer
		}

		app, err := NewApp(fx.Populate(&deps))
		if err != nil {
			return xerrors.Errorf("failed to create command: %w", err)
		}
		defer app.Close()

		ctx := app.Manager.Context()
		collection := api.Collection(checkpointFlags.collection)
		tag := app.Config.Tag.GetEffectiveTag(checkpointFlags.tag)

		if app.Config.IsCollectionSynchronized(collection) {
			return xerrors.New("cannot reset the checkpoint for a synchronized collection")
		}

		checkpoint, err := deps.Checkpointer.Get(ctx, collection, tag)
		if err != nil {
			return xerrors.Errorf("failed to get checkpoint: %w", err)
		}

		if checkpoint.Empty() {
			return xerrors.New("failed to get checkpoint")
		}

		newCheckpoint := deps.Checkpointer.GetInitialCheckpoint(collection, tag)
		app.Logger.Info(
			"setting checkpoint",
			zap.Reflect("from", checkpoint),
			zap.Reflect("to", newCheckpoint),
		)

		if !app.Confirm("DANGEROUS: Are you sure you want to reset checkpoint? This will reset the history of the collection") {
			return nil
		}

		if err := deps.Checkpointer.Set(ctx, newCheckpoint); err != nil {
			return xerrors.Errorf("failed to set checkpoint: %w", err)
		}

		return nil
	})
)

func init() {
	rootCommand.AddCommand(storageCommand)
	storageCommand.AddCommand(checkpointCommand)
	checkpointCommand.AddCommand(checkpointGetCommand)
	checkpointCommand.AddCommand(checkpointSetCommand)
	checkpointCommand.AddCommand(checkpointResetCommand)

	checkpointCommand.StringVar(&checkpointFlags.collection, "collection", "", true)
	checkpointCommand.Uint32Var(&checkpointFlags.tag, "tag", 0, false)
	checkpointCommand.Int64Var(&checkpointFlags.sequence, "sequence", 0, false)
	checkpointCommand.Uint64Var(&checkpointFlags.height, "height", 0, false)
}
