package ethereum

import (
	"go.uber.org/fx"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainnode/internal/api"
	xapi "github.com/coinbase/chainnode/internal/api/ethereum"
	"github.com/coinbase/chainnode/internal/controller/internal"
)

type (
	controller struct {
		checkpointer                  internal.Checkpointer
		handler                       internal.Handler
		cronTasks                     []internal.CronTask
		reverseProxies                []internal.ReverseProxy
		blockIndexer                  internal.Indexer
		blockByHashIndexer            internal.Indexer
		logIndexerV2                  internal.Indexer
		transactionIndexer            internal.Indexer
		transactionReceiptIndexer     internal.Indexer
		traceByHashIndexer            internal.Indexer
		traceByNumberIndexer          internal.Indexer
		blockExtraDataByNumberIndexer internal.Indexer
	}

	ControllerParams struct {
		fx.In
		Checkpointer                  internal.Checkpointer
		Handler                       internal.Handler        `name:"ethereum"`
		CronTasks                     []internal.CronTask     `group:"ethereum"`
		ReverseProxies                []internal.ReverseProxy `name:"ethereum"`
		BlockIndexer                  internal.Indexer        `name:"ethereum/block"`
		BlockByHashIndexer            internal.Indexer        `name:"ethereum/blockByHash"`
		LogIndexerV2                  internal.Indexer        `name:"ethereum/logV2"`
		TransactionIndexer            internal.Indexer        `name:"ethereum/transaction"`
		TransactionReceiptIndexer     internal.Indexer        `name:"ethereum/transactionReceipt"`
		TraceByHashIndexer            internal.Indexer        `name:"ethereum/traceByHash"`
		TraceByNumberIndexer          internal.Indexer        `name:"ethereum/traceByNumber"`
		BlockExtraDataByNumberIndexer internal.Indexer        `name:"ethereum/blockExtraDataByNumber"`
	}
)

func NewController(params ControllerParams) internal.Controller {
	return &controller{
		checkpointer:                  params.Checkpointer,
		handler:                       params.Handler,
		cronTasks:                     params.CronTasks,
		reverseProxies:                params.ReverseProxies,
		blockIndexer:                  params.BlockIndexer,
		blockByHashIndexer:            params.BlockByHashIndexer,
		logIndexerV2:                  params.LogIndexerV2,
		transactionIndexer:            params.TransactionIndexer,
		transactionReceiptIndexer:     params.TransactionReceiptIndexer,
		traceByHashIndexer:            params.TraceByHashIndexer,
		traceByNumberIndexer:          params.TraceByNumberIndexer,
		blockExtraDataByNumberIndexer: params.BlockExtraDataByNumberIndexer,
	}
}

func (c *controller) Checkpointer() internal.Checkpointer {
	return c.checkpointer
}

func (c *controller) Handler() internal.Handler {
	return c.handler
}

func (c *controller) Indexer(collection api.Collection) (internal.Indexer, error) {
	switch collection {
	case xapi.CollectionBlocks:
		return c.blockIndexer, nil
	case xapi.CollectionBlocksByHash:
		return c.blockByHashIndexer, nil
	case xapi.CollectionLogsV2:
		return c.logIndexerV2, nil
	case xapi.CollectionTransactions:
		return c.transactionIndexer, nil
	case xapi.CollectionTransactionReceipts:
		return c.transactionReceiptIndexer, nil
	case xapi.CollectionTracesByHash:
		return c.traceByHashIndexer, nil
	case xapi.CollectionTracesByNumber:
		return c.traceByNumberIndexer, nil
	case xapi.CollectionBlocksExtraDataByNumber:
		return c.blockExtraDataByNumberIndexer, nil
	default:
		return nil, xerrors.Errorf("collection %v: %w", collection, api.ErrNotImplemented)
	}
}

func (c *controller) CronTasks() []internal.CronTask {
	return c.cronTasks
}

func (c *controller) ReverseProxies() []internal.ReverseProxy {
	return c.reverseProxies
}
