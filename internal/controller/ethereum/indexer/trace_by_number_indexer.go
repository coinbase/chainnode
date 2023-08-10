package indexer

import (
	"context"

	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainnode/internal/api"
	"github.com/coinbase/chainnode/internal/config"
	"github.com/coinbase/chainnode/internal/controller/internal"
	"github.com/coinbase/chainnode/internal/storage"
	"github.com/coinbase/chainnode/internal/utils/arbitrumutil"
	"github.com/coinbase/chainnode/internal/utils/fxparams"
	"github.com/coinbase/chainnode/internal/utils/log"
)

type (
	traceByNumberIndexer struct {
		config  *config.Config
		logger  *zap.Logger
		storage storage.EthereumStorage
	}

	traceByNumberIndexerParams struct {
		fx.In
		fxparams.Params
		Storage storage.EthereumStorage
	}
)

func NewTraceByNumberIndexer(params traceByNumberIndexerParams) internal.Indexer {
	return &traceByNumberIndexer{
		config:  params.Config,
		logger:  log.WithPackage(params.Logger),
		storage: params.Storage,
	}
}

func (i *traceByNumberIndexer) Index(
	ctx context.Context,
	tag uint32,
	event *api.Event,
	block *api.Block,
) error {
	trace, err := parseTraceFromBlock(tag, event, block)
	if err != nil {
		return xerrors.Errorf("failed to parse trace from block: %w", err)
	}

	var msg string
	// Persist into correct collection based on the block height
	if arbitrumutil.IsArbitrumAndBeforeNITROUpgrade(block.GetBlockchain(), block.GetNetwork(), event.GetBlock().GetHeight()) {
		if err := i.storage.PersistArbtraceBlock(ctx, trace); err != nil {
			return xerrors.Errorf("failed to persist arbtraceBlock (tag=%v, event={%+v}, block={%+v}): %w", tag, event, block.Metadata, err)
		}
		msg = "indexed arbtraceBlock"
	} else {
		if err := i.storage.PersistTraceByNumber(ctx, trace); err != nil {
			return xerrors.Errorf("failed to persist traceByNumber (tag=%v, event={%+v}, block={%+v}): %w", tag, event, block.Metadata, err)
		}
		msg = "indexed traceByNumber"
	}

	i.logger.Info(
		msg,
		zap.Uint32("tag", tag),
		zap.Reflect("event", event),
		zap.Reflect("block", block.Metadata),
	)

	return nil
}
