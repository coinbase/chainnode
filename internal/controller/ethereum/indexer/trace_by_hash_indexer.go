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
	traceByHashIndexer struct {
		config  *config.Config
		logger  *zap.Logger
		storage storage.EthereumStorage
	}

	TraceByHashIndexerParams struct {
		fx.In
		fxparams.Params
		Storage storage.EthereumStorage
	}

	headerLiteForTrace struct {
		BlockTime EthereumQuantity `json:"timestamp"`
	}
)

func NewTraceByHashIndexer(params TraceByHashIndexerParams) internal.Indexer {
	return &traceByHashIndexer{
		config:  params.Config,
		logger:  log.WithPackage(params.Logger),
		storage: params.Storage,
	}
}

func (i *traceByHashIndexer) Index(
	ctx context.Context,
	tag uint32,
	event *api.Event,
	block *api.Block,
) error {
	//skip parsing for blocks earlier than nitro upgrade in Arbitrum
	if arbitrumutil.IsArbitrumAndBeforeNITROUpgrade(block.GetBlockchain(), block.GetNetwork(), event.GetBlock().GetHeight()) {
		return nil
	}

	trace, err := parseTraceFromBlock(tag, event, block)
	if err != nil {
		return xerrors.Errorf("failed to parse trace from block: %w", err)
	}

	if err := i.storage.PersistTraceByHash(ctx, trace); err != nil {
		return xerrors.Errorf("failed to persist traceByHash (tag=%v, event={%+v}, block={%+v}): %w", tag, event, block.Metadata, err)
	}

	i.logger.Info(
		"indexed traceByHash",
		zap.Uint32("tag", tag),
		zap.Reflect("event", event),
		zap.Reflect("block", block.Metadata),
	)

	return nil
}
