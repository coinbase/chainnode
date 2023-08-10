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
	transactionIndexer struct {
		config  *config.Config
		logger  *zap.Logger
		storage storage.EthereumStorage
	}

	TransactionParams struct {
		fx.In
		fxparams.Params
		Storage storage.EthereumStorage
	}

	headerLiteForTransaction struct {
		BlockTime    EthereumQuantity `json:"timestamp"`
		Transactions []*transaction   `json:"transactions"`
	}

	headerWithTransactions struct {
		RawTransactions []json.RawMessage `json:"transactions"`
	}

	transaction struct {
		Hash EthereumHexString `json:"hash"`
	}
)

func NewTransactionIndexer(params TransactionParams) internal.Indexer {
	return &transactionIndexer{
		config:  params.Config,
		logger:  log.WithPackage(params.Logger),
		storage: params.Storage,
	}
}

func (i *transactionIndexer) Index(ctx context.Context, tag uint32, event *api.Event, block *api.Block) error {
	sequence := api.ParseSequenceNum(event.GetSequenceNum())

	blockHeader := block.GetEthereum().GetHeader()
	if len(blockHeader) == 0 {
		return xerrors.Errorf("failed to find block header")
	}

	var headerLite headerLiteForTransaction
	if err := json.Unmarshal(blockHeader, &headerLite); err != nil {
		return xerrors.Errorf("failed to unmarshal to headerLite: %w", err)
	}

	var header headerWithTransactions
	if err := json.Unmarshal(blockHeader, &header); err != nil {
		return xerrors.Errorf("failed to unmarshal to header: %w", err)
	}

	if len(headerLite.Transactions) != len(header.RawTransactions) {
		return xerrors.Errorf(
			"unexpected number of transactions returned (len(headerLite)=%v, len(header)=%v)",
			len(headerLite.Transactions), len(header.RawTransactions))
	}

	if len(headerLite.Transactions) == 0 {
		return nil
	}

	blockTime := headerLite.BlockTime.AsTime()
	transactions := make([]*ethereum.Transaction, len(headerLite.Transactions))
	for i, tx := range headerLite.Transactions {
		hash := tx.Hash
		transactions[i] = ethereum.NewTransaction(tag, hash.Value(), sequence, header.RawTransactions[i], blockTime)
	}

	if err := i.storage.PersistTransactions(ctx, transactions); err != nil {
		return xerrors.Errorf(
			"failed to persist transactions (tag=%v, event={%+v}, block={%+v}): %w",
			tag, event, block.GetMetadata(), err)
	}

	i.logger.Info(
		"indexed transactions",
		zap.Uint32("tag", tag),
		zap.Reflect("event", event),
		zap.Reflect("block", block.Metadata),
		zap.Int("num", len(transactions)),
	)

	return nil
}
