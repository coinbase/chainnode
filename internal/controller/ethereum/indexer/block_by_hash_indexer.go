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
	"github.com/coinbase/chainnode/internal/utils/syncgroup"
)

type (
	// blockByHashIndexer is responsible for indexing BlockByHash and BlockByHashWithoutFullTx
	blockByHashIndexer struct {
		config  *config.Config
		logger  *zap.Logger
		storage storage.EthereumStorage
	}

	BlockByHashIndexerParams struct {
		fx.In
		fxparams.Params
		Storage storage.EthereumStorage
	}

	headerLiteForBlockByHash struct {
		BlockTime EthereumQuantity `json:"timestamp"`
	}

	headerLiteForBlockByHashWithoutFullTx struct {
		BlockTime    EthereumQuantity                              `json:"timestamp"`
		Transactions []*transactionLiteForBlockByHashWithoutFullTx `json:"transactions"`
	}

	transactionLiteForBlockByHashWithoutFullTx struct {
		Hash json.RawMessage `json:"hash"`
	}
)

const (
	transactionsFieldName = "transactions"
)

func NewBlockByHashIndexer(params BlockByHashIndexerParams) internal.Indexer {
	return &blockByHashIndexer{
		config:  params.Config,
		logger:  log.WithPackage(params.Logger),
		storage: params.Storage,
	}
}

func (i *blockByHashIndexer) Index(ctx context.Context, tag uint32, event *api.Event, block *api.Block) error {
	sequence := api.ParseSequenceNum(event.GetSequenceNum())

	blockHeader := block.GetEthereum().GetHeader()
	if len(blockHeader) == 0 {
		return xerrors.Errorf("failed to find block header")
	}

	group, ctx := syncgroup.New(ctx)

	group.Go(func() error {
		_, err := i.indexBlockByHash(ctx, tag, event, block, sequence, blockHeader)
		if err != nil {
			return xerrors.Errorf("failed to index BlockByHash: %w", err)
		}
		return nil
	})

	group.Go(func() error {
		var err error
		_, err = i.indexBlockByHashWithoutFullTx(ctx, tag, event, block, sequence, blockHeader)
		if err != nil {
			return xerrors.Errorf("failed to index BlockByHashWithoutFullTx: %w", err)
		}
		return nil
	})

	if err := group.Wait(); err != nil {
		return xerrors.Errorf("failed to finish index task: %w", err)
	}

	return nil
}

func (i *blockByHashIndexer) indexBlockByHash(
	ctx context.Context,
	tag uint32,
	event *api.Event,
	block *api.Block,
	sequence api.Sequence,
	blockHeader []byte,
) (*ethereum.Block, error) {
	var headerLit headerLiteForBlockByHash
	if err := json.Unmarshal(blockHeader, &headerLit); err != nil {
		return nil, xerrors.Errorf("failed to parse header into headerLiteForBlockByHash: %w", err)
	}

	blockID := event.GetBlock()
	newBlock := ethereum.NewBlock(tag, blockID.GetHeight(), blockID.GetHash(), sequence, headerLit.BlockTime.AsTime(), blockHeader)

	if err := i.storage.PersistBlockByHash(ctx, newBlock); err != nil {
		return nil, xerrors.Errorf("failed to persist block by hash (tag=%v, event={%+v}, block={%+v}): %w", tag, event, block.GetMetadata(), err)
	}

	i.logger.Info(
		"indexed blockByHash",
		zap.Uint32("tag", tag),
		zap.Reflect("event", event),
		zap.Reflect("block", block.Metadata),
		zap.Int("size", len(blockHeader)),
	)
	return newBlock, nil
}

func (i *blockByHashIndexer) indexBlockByHashWithoutFullTx(
	ctx context.Context,
	tag uint32,
	event *api.Event,
	block *api.Block,
	sequence api.Sequence,
	blockHeader []byte,
) (*ethereum.Block, error) {
	var headerLit headerLiteForBlockByHashWithoutFullTx
	if err := json.Unmarshal(blockHeader, &headerLit); err != nil {
		return nil, xerrors.Errorf("failed to parse header into headerLiteForBlockByHashWithoutFullTx: %w", err)
	}

	data, err := i.parseDataWithoutFullTxFromBlockHeader(blockHeader)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse data from block header: %w", err)
	}

	blockID := event.GetBlock()
	newBlock := ethereum.NewBlock(tag, blockID.GetHeight(), blockID.GetHash(), sequence, headerLit.BlockTime.AsTime(), data)
	if err := i.storage.PersistBlockByHashWithoutFullTx(ctx, newBlock); err != nil {
		return nil, xerrors.Errorf("failed to persist block by hash without full tx (tag=%v, event={%+v}, block={%+v}): %w", tag, event, block.GetMetadata(), err)
	}

	i.logger.Info(
		"indexed blockByHashWithoutFullTx",
		zap.Uint32("tag", tag),
		zap.Reflect("event", event),
		zap.Reflect("block", block.Metadata),
		zap.Int("size", len(data)),
	)
	return newBlock, nil
}

func (i *blockByHashIndexer) parseDataWithoutFullTxFromBlockHeader(blockHeader []byte) ([]byte, error) {
	var blockData map[string]json.RawMessage
	err := json.Unmarshal(blockHeader, &blockData)
	if err != nil {
		return nil, xerrors.Errorf("failed to unmarshal block header: %w", err)
	}

	if _, ok := blockData[transactionsFieldName]; !ok || len(blockData[transactionsFieldName]) == 0 {
		return blockHeader, nil
	}

	var txs []transactionLiteForBlockByHashWithoutFullTx
	if err = json.Unmarshal(blockData[transactionsFieldName], &txs); err != nil {
		return nil, xerrors.Errorf("failed to unmarshal blockData[%s]: %w", transactionsFieldName, err)
	}

	txHashes := make([]json.RawMessage, len(txs))
	for i, tx := range txs {
		txHashes[i] = tx.Hash
	}

	txHashesMarshaled, err := json.Marshal(txHashes)
	if err != nil {
		return nil, xerrors.Errorf("failed to marshal txHashes: %w", err)
	}

	blockData["transactions"] = txHashesMarshaled

	result, err := json.Marshal(blockData)
	if err != nil {
		return nil, xerrors.Errorf("failed to marshal blockData: %w", err)
	}

	return result, nil
}
