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
	logIndexerV2 struct {
		config  *config.Config
		logger  *zap.Logger
		storage storage.EthereumStorage
	}

	LogIndexerV2Params struct {
		fx.In
		fxparams.Params
		Storage storage.EthereumStorage
	}

	// ethereumHeader
	headerLiteForLogs struct {
		BlockTime EthereumQuantity  `json:"timestamp"`
		LogsBloom EthereumHexString `json:"logsBloom"`
	}

	transactionReceipt struct {
		Logs []json.RawMessage `json:"logs"`
	}
)

func NewLogIndexerV2(params LogIndexerV2Params) internal.Indexer {
	return &logIndexerV2{
		config:  params.Config,
		logger:  log.WithPackage(params.Logger),
		storage: params.Storage,
	}
}

func (i *logIndexerV2) Index(ctx context.Context, tag uint32, event *api.Event, block *api.Block) error {
	logs, err := processLogsFromEvent(tag, event, block)
	if err != nil {
		return xerrors.Errorf("failed to process logs from event: %w", err)
	}

	if err := i.storage.PersistLogsV2(ctx, logs); err != nil {
		return xerrors.Errorf("failed to persist log (tag=%v, event={%+v}, block={%+v}): %w", tag, event, block.Metadata, err)
	}

	i.logger.Info(
		"indexed logs-v2",
		zap.Uint32("tag", tag),
		zap.Reflect("event", event),
		zap.Reflect("block", block.Metadata),
	)

	return nil
}

// extractLogsFromReceipt extracts logs data from a raw transaction receipt
func extractLogsFromReceipt(rawReceipt []byte) ([]json.RawMessage, error) {
	var receiptLit transactionReceipt
	if err := json.Unmarshal(rawReceipt, &receiptLit); err != nil {
		return nil, xerrors.Errorf("failed to parse receipt into TransactionReceipt: %w", err)
	}
	return receiptLit.Logs, nil
}

func processLogsFromEvent(tag uint32, event *api.Event, block *api.Block) (*ethereum.Logs, error) {
	sequence := api.ParseSequenceNum(event.GetSequenceNum())

	blockID := event.GetBlock()
	receipts := block.GetEthereum().GetTransactionReceipts()
	blockHeader := block.GetEthereum().GetHeader()
	if len(blockHeader) == 0 {
		return nil, xerrors.Errorf("failed to find block header")
	}

	// parse raw header
	var headerLit headerLiteForLogs
	if err := json.Unmarshal(blockHeader, &headerLit); err != nil {
		return nil, xerrors.Errorf("failed to parse header: %w", err)
	}
	logsBloom := headerLit.LogsBloom
	blockTime := headerLit.BlockTime.AsTime()

	// marshal raw receipts
	rawLogs := make([]json.RawMessage, 0)
	for _, receipt := range receipts {
		batch, err := extractLogsFromReceipt(receipt)
		if err != nil {
			return nil, xerrors.Errorf("failed to parse receipt: %w", err)
		}
		rawLogs = append(rawLogs, batch...)
	}

	data, err := json.Marshal(rawLogs)
	if err != nil {
		return nil, xerrors.Errorf("failed to marshal logs: %w", err)
	}

	logs := ethereum.NewLogs(
		tag, blockID.GetHeight(), blockID.GetHash(), sequence, logsBloom.Value(), data, blockTime)
	return logs, nil
}
