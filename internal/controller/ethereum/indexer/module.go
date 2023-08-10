package indexer

import (
	"go.uber.org/fx"
)

var Module = fx.Options(
	fx.Provide(fx.Annotated{
		Name:   "ethereum/block",
		Target: NewBlockIndexer,
	}),
	fx.Provide(fx.Annotated{
		Name:   "ethereum/logV2",
		Target: NewLogIndexerV2,
	}),
	fx.Provide(fx.Annotated{
		Name:   "ethereum/transaction",
		Target: NewTransactionIndexer,
	}),
	fx.Provide(fx.Annotated{
		Name:   "ethereum/transactionReceipt",
		Target: NewTransactionReceiptIndexer,
	}),
	fx.Provide(fx.Annotated{
		Name:   "ethereum/traceByHash",
		Target: NewTraceByHashIndexer,
	}),
	fx.Provide(fx.Annotated{
		Name:   "ethereum/traceByNumber",
		Target: NewTraceByNumberIndexer,
	}),
	fx.Provide(fx.Annotated{
		Name:   "ethereum/blockByHash",
		Target: NewBlockByHashIndexer,
	}),
	fx.Provide(fx.Annotated{
		Name:   "ethereum/blockExtraDataByNumber",
		Target: NewBlockExtraDataByNumberIndexer,
	}),
)
