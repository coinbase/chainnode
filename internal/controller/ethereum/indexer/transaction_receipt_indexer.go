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
	transactionReceiptIndexer struct {
		config  *config.Config
		logger  *zap.Logger
		storage storage.EthereumStorage
	}

	TransactionReceiptParams struct {
		fx.In
		fxparams.Params
		Storage storage.EthereumStorage
	}

	headerLiteForReceipt struct {
		BlockTime EthereumQuantity `json:"timestamp"`
	}

	receiptLite struct {
		Hash EthereumHexString `json:"transactionHash"`
	}
)

func NewTransactionReceiptIndexer(params TransactionReceiptParams) internal.Indexer {
	return &transactionReceiptIndexer{
		config:  params.Config,
		logger:  log.WithPackage(params.Logger),
		storage: params.Storage,
	}
}

func (i *transactionReceiptIndexer) Index(ctx context.Context, tag uint32, event *api.Event, block *api.Block) error {
	sequence := api.ParseSequenceNum(event.GetSequenceNum())

	blockHeader := block.GetEthereum().GetHeader()
	if len(blockHeader) == 0 {
		return xerrors.Errorf("failed to find block header")
	}

	var headerLite headerLiteForReceipt
	if err := json.Unmarshal(blockHeader, &headerLite); err != nil {
		return xerrors.Errorf("failed to unmarshal to headerLite: %w", err)
	}

	rawReceipts := block.GetEthereum().GetTransactionReceipts()
	if len(rawReceipts) == 0 {
		return nil
	}

	receiptsLite := make([]*receiptLite, len(rawReceipts))
	for i, rawReceipt := range rawReceipts {
		var receiptLite receiptLite
		if err := json.Unmarshal(rawReceipt, &receiptLite); err != nil {
			return xerrors.Errorf("failed to unmarshal receipt: %w", err)
		}

		receiptsLite[i] = &receiptLite
	}

	blockTime := headerLite.BlockTime.AsTime()
	receipts := make([]*ethereum.TransactionReceipt, len(rawReceipts))
	for i, rawReceipt := range rawReceipts {
		receipts[i] = ethereum.NewTransactionReceipt(
			tag, receiptsLite[i].Hash.Value(),
			sequence,
			rawReceipt,
			blockTime,
		)
	}

	if err := i.storage.PersistTransactionReceipts(ctx, receipts); err != nil {
		return xerrors.Errorf("failed to persist receipts (tag=%v, event={%+v}, block={%+v}): %w",
			tag, event, block.GetMetadata(), err)
	}

	i.logger.Info(
		"indexed receipts",
		zap.Uint32("tag", tag),
		zap.Reflect("event", event),
		zap.Reflect("block", block.Metadata),
		zap.Int("num", len(receipts)),
	)

	return nil
}
