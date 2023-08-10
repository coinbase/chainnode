package indexer

import (
	"context"
	"encoding/json"

	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainnode/internal/api"
	"github.com/coinbase/chainnode/internal/api/ethereum"
	"github.com/coinbase/chainnode/internal/config"
	"github.com/coinbase/chainnode/internal/controller/internal"
	"github.com/coinbase/chainnode/internal/storage"
	"github.com/coinbase/chainnode/internal/utils/fxparams"
	"github.com/coinbase/chainnode/internal/utils/log"
)

type (
	blockIndexer struct {
		config  *config.Config
		logger  *zap.Logger
		storage storage.EthereumStorage
	}

	BlockIndexerParams struct {
		fx.In
		fxparams.Params
		Storage storage.EthereumStorage
	}

	headerLiteForBlock struct {
		BlockTime EthereumQuantity `json:"timestamp"`
	}
)

func NewBlockIndexer(params BlockIndexerParams) internal.Indexer {
	return &blockIndexer{
		config:  params.Config,
		logger:  log.WithPackage(params.Logger),
		storage: params.Storage,
	}
}

func (i *blockIndexer) Index(ctx context.Context, tag uint32, event *api.Event, block *api.Block) error {
	sequence := api.ParseSequenceNum(event.GetSequenceNum())

	blockID := event.GetBlock()
	blockHeader := block.GetEthereum().GetHeader()
	if len(blockHeader) == 0 {
		return xerrors.Errorf("failed to find block header")
	}

	var headerLit headerLiteForBlock
	if err := json.Unmarshal(blockHeader, &headerLit); err != nil {
		return xerrors.Errorf("failed to parse header into headerLiteForBlock: %w", err)
	}

	newBlock := ethereum.NewBlock(tag, blockID.GetHeight(), blockID.GetHash(), sequence, headerLit.BlockTime.AsTime(), blockHeader)

	if err := i.storage.PersistBlock(ctx, newBlock); err != nil {
		return xerrors.Errorf("failed to persist block (tag=%v, event={%+v}, block={%+v}): %w", tag, event, block.GetMetadata(), err)
	}

	i.logger.Info(
		"indexed block",
		zap.Uint32("tag", tag),
		zap.Reflect("event", event),
		zap.Reflect("block", block.Metadata),
		zap.Int("size", len(blockHeader)),
	)

	return nil
}
