package activity

import (
	"context"

	"go.temporal.io/sdk/workflow"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainnode/internal/api"
	"github.com/coinbase/chainnode/internal/cadence"
	"github.com/coinbase/chainnode/internal/clients/chainstorage"
	"github.com/coinbase/chainnode/internal/controller"
	"github.com/coinbase/chainnode/internal/utils/fxparams"
	"github.com/coinbase/chainnode/internal/utils/syncgroup"
	"github.com/coinbase/chainnode/internal/workflow/instrument"
)

type (
	Transformer struct {
		baseActivity
		chainStorageClient chainstorage.Client
		controller         controller.Controller
	}

	TransformerParams struct {
		fx.In
		fxparams.Params
		Runtime            cadence.Runtime
		ChainStorageClient chainstorage.Client
		Controller         controller.Controller
	}

	TransformerRequest struct {
		Tag         uint32         `validate:"required"`
		Collection  api.Collection `validate:"required"`
		Events      []*api.Event   `validate:"required"`
		Parallelism uint64
	}

	TransformerResponse struct{}
)

func NewTransformer(params TransformerParams) *Transformer {
	a := &Transformer{
		baseActivity:       newBaseActivity(ActivityTransformer, params.Runtime),
		chainStorageClient: params.ChainStorageClient,
		controller:         params.Controller,
	}
	a.register(a.execute)
	return a
}

func (a *Transformer) Execute(ctx workflow.Context, request *TransformerRequest) (*TransformerResponse, error) {
	var response TransformerResponse
	err := a.executeActivity(
		ctx,
		request,
		&response,
		instrument.WithScopeTag("collection", request.Collection.String()),
	)
	return &response, err
}

func (a *Transformer) execute(ctx context.Context, request *TransformerRequest) (*TransformerResponse, error) {
	logger := a.getLogger(ctx)
	tag := request.Tag
	collection := request.Collection

	if request.Parallelism <= 1 {
		for i := range request.Events {
			event := request.Events[i]
			if err := a.transformEvent(ctx, logger, tag, collection, event); err != nil {
				return nil, xerrors.Errorf("failed to transform event (%+v): %w", event, err)
			}
		}
	} else {
		group, ctx := syncgroup.New(ctx, syncgroup.WithThrottling(int(request.Parallelism)))
		for i := range request.Events {
			event := request.Events[i]
			group.Go(func() error {
				if err := a.transformEvent(ctx, logger, tag, collection, event); err != nil {
					return xerrors.Errorf("failed to transform event (%+v): %w", event, err)
				}

				return nil
			})
		}

		if err := group.Wait(); err != nil {
			return nil, err
		}
	}

	return &TransformerResponse{}, nil
}

func (a *Transformer) transformEvent(
	ctx context.Context,
	logger *zap.Logger,
	tag uint32,
	collection api.Collection,
	event *api.Event,
) error {
	logger.Debug(
		"transforming event",
		zap.String("collection", collection.String()),
		zap.Reflect("event", event),
	)

	// 1. Exclude events with api.EventRemoved/Unknown since ChainNode does not handle reorgs
	// 2. Exclude events with block-skipped: true. these are historical reorg events/blocks which are not on canonical chain.
	if event.Type != api.EventAdded || event.Block.Skipped {
		return nil
	}

	blockID := event.Block
	block, err := a.chainStorageClient.GetBlockWithTag(ctx, blockID.Tag, blockID.Height, blockID.Hash)
	if err != nil {
		return xerrors.Errorf("failed to get block from chain storage: %w", err)
	}

	indexer, err := a.controller.Indexer(collection)
	if err != nil {
		return xerrors.Errorf("failed to get indexer: %w", err)
	}

	err = indexer.Index(ctx, tag, event, block)
	if err != nil {
		return xerrors.Errorf("failed to index the event: %w", err)
	}

	return nil
}
