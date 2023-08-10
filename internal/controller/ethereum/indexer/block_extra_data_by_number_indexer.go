package indexer

import (
	"context"
	"encoding/json"

	"github.com/golang/protobuf/proto"
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
	blockExtraDataByNumberIndexer struct {
		config  *config.Config
		logger  *zap.Logger
		storage storage.EthereumStorage
	}

	BlockExtraDataByNumberIndexerParams struct {
		fx.In
		fxparams.Params
		Storage storage.EthereumStorage
	}
)

func NewBlockExtraDataByNumberIndexer(params BlockExtraDataByNumberIndexerParams) internal.Indexer {
	return &blockExtraDataByNumberIndexer{
		config:  params.Config,
		logger:  log.WithPackage(params.Logger),
		storage: params.Storage,
	}
}

func (i *blockExtraDataByNumberIndexer) Index(ctx context.Context, tag uint32, event *api.Event, block *api.Block) error {
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

	var data []byte
	var err error
	extraData := block.GetEthereum().GetPolygon()
	if extraData != nil {
		data, err = proto.Marshal(block.GetEthereum().GetPolygon())
		if err != nil {
			return xerrors.Errorf("failed to marshal extra data: %w", err)
		}
	}

	blockExtraData := ethereum.NewBlockExtraData(tag, blockID.GetHeight(), blockID.GetHash(), sequence, headerLit.BlockTime.AsTime(), data)
	if err := i.storage.PersistBlockExtraDataByNumber(ctx, blockExtraData); err != nil {
		return xerrors.Errorf("failed to persist blockExtraData (tag=%v, event={%+v}, block={%+v}): %w", tag, event, block.GetMetadata(), err)
	}

	i.logger.Info(
		"indexed block extra data by number",
		zap.Uint32("tag", tag),
		zap.Reflect("event", event),
		zap.Reflect("block", block.Metadata),
		zap.Int("size", len(data)),
	)

	return nil
}
