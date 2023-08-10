package handler

import (
	"context"
	"encoding/json"
	"strconv"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/eth/tracers"
	"github.com/uber-go/tally/v4"
	"go.uber.org/zap"

	"github.com/coinbase/chainnode/internal/clients/blockchain/jsonrpc"
	"github.com/coinbase/chainnode/internal/server/rpc"
	"github.com/coinbase/chainnode/internal/utils/instrument"
)

type (
	instrumentInterceptor struct {
		next         Receiver
		proxyMetrics *proxyMetrics
		metrics      *metrics
	}

	metrics struct {
		instrumentBlockNumber                         instrument.Call
		instrumentGetBlockByHash                      instrument.Call
		instrumentGetBlockByNumber                    instrument.Call
		instrumentGetBlockByNumberProxy               instrument.Call
		instrumentGetBlockTransactionCountByHash      instrument.Call
		instrumentGetBlockTransactionCountByNumber    instrument.Call
		instrumentGetUncleCountByBlockHash            instrument.Call
		instrumentGetUncleCountByBlockNumber          instrument.Call
		instrumentGetLogs                             instrument.Call
		instrumentGetTransactionByHash                instrument.Call
		instrumentGetTransactionReceipt               instrument.Call
		instrumentGetTransactionByBlockHashAndIndex   instrument.Call
		instrumentGetTransactionByBlockNumberAndIndex instrument.Call
		instrumentTraceBlockByHash                    instrument.Call
		instrumentTraceBlockByNumber                  instrument.Call
		instrumentArbtraceBlock                       instrument.Call
		instrumentChainId                             instrument.Call
		instrumentVersion                             instrument.Call
		instrumentSyncing                             instrument.Call
		instrumentListening                           instrument.Call
		instrumentGetAuthor                           instrument.Call
	}
)

const (
	handlerScopeName  = "handler"
	loggerMsg         = "handler.request"
	methodField       = "method"
	proxyField        = "proxy"
	latencyLevelField = "latency_level"

	LatencyLevelHigh    = "high"
	LatencyLevelDefault = "default"
)

var (
	methodLatencyLevelMap = map[string]string{
		EthGetLogs.Name: LatencyLevelHigh,
	}
)

func WithInstrumentInterceptor(next Receiver, scope tally.Scope, logger *zap.Logger) Receiver {
	return &instrumentInterceptor{
		next:         next,
		metrics:      newMetrics(scope, logger),
		proxyMetrics: newProxyMetrics(scope, logger),
	}
}

func (i *instrumentInterceptor) BlockNumber(ctx context.Context) (json.RawMessage, error) {
	return i.intercept(
		ctx,
		i.metrics.instrumentBlockNumber,
		func(ctx context.Context) (json.RawMessage, error) {
			return i.next.BlockNumber(ctx)
		},
		nil,
	)
}

func (i *instrumentInterceptor) GetBlockByHash(ctx context.Context, blockHash common.Hash, returnFullTx bool) (json.RawMessage, error) {
	return i.intercept(
		ctx,
		i.metrics.instrumentGetBlockByHash,
		func(ctx context.Context) (json.RawMessage, error) {
			return i.next.GetBlockByHash(ctx, blockHash, returnFullTx)
		},
		jsonrpc.Params{blockHash, returnFullTx},
	)
}

func (i *instrumentInterceptor) GetBlockTransactionCountByHash(ctx context.Context, blockHash common.Hash) (json.RawMessage, error) {
	return i.intercept(
		ctx,
		i.metrics.instrumentGetBlockTransactionCountByHash,
		func(ctx context.Context) (json.RawMessage, error) {
			return i.next.GetBlockTransactionCountByHash(ctx, blockHash)
		},
		jsonrpc.Params{blockHash},
	)
}

func (i *instrumentInterceptor) GetBlockTransactionCountByNumber(ctx context.Context, blockNumber rpc.BlockNumber) (json.RawMessage, error) {
	return i.intercept(
		ctx,
		i.metrics.instrumentGetBlockTransactionCountByNumber,
		func(ctx context.Context) (json.RawMessage, error) {
			return i.next.GetBlockTransactionCountByNumber(ctx, blockNumber)
		},
		jsonrpc.Params{blockNumber},
	)
}

func (i *instrumentInterceptor) GetUncleCountByBlockHash(ctx context.Context, blockHash common.Hash) (json.RawMessage, error) {
	return i.intercept(
		ctx,
		i.metrics.instrumentGetUncleCountByBlockHash,
		func(ctx context.Context) (json.RawMessage, error) {
			return i.next.GetUncleCountByBlockHash(ctx, blockHash)
		},
		jsonrpc.Params{blockHash},
	)
}

func (i *instrumentInterceptor) GetUncleCountByBlockNumber(ctx context.Context, blockNumber rpc.BlockNumber) (json.RawMessage, error) {
	return i.intercept(
		ctx,
		i.metrics.instrumentGetUncleCountByBlockNumber,
		func(ctx context.Context) (json.RawMessage, error) {
			return i.next.GetUncleCountByBlockNumber(ctx, blockNumber)
		},
		jsonrpc.Params{blockNumber},
	)
}

func (i *instrumentInterceptor) GetBlockByNumber(ctx context.Context, blockNumber rpc.BlockNumber, returnFullTx bool) (json.RawMessage, error) {
	var call instrument.Call
	switch blockNumber {
	case rpc.PendingBlockNumber:
		call = i.metrics.instrumentGetBlockByNumberProxy
	default:
		call = i.metrics.instrumentGetBlockByNumber
	}

	return i.intercept(
		ctx,
		call,
		func(ctx context.Context) (json.RawMessage, error) {
			return i.next.GetBlockByNumber(ctx, blockNumber, returnFullTx)
		},
		jsonrpc.Params{blockNumber, returnFullTx},
	)
}

func (i *instrumentInterceptor) GetLogs(ctx context.Context, criteria FilterCriteria) (json.RawMessage, error) {
	return i.intercept(
		ctx,
		i.metrics.instrumentGetLogs,
		func(ctx context.Context) (json.RawMessage, error) {
			return i.next.GetLogs(ctx, criteria)
		},
		jsonrpc.Params{criteria},
	)
}

func (i *instrumentInterceptor) GetTransactionByHash(ctx context.Context, txHash common.Hash) (json.RawMessage, error) {
	return i.intercept(
		ctx,
		i.metrics.instrumentGetTransactionByHash,
		func(ctx context.Context) (json.RawMessage, error) {
			return i.next.GetTransactionByHash(ctx, txHash)
		},
		jsonrpc.Params{txHash},
	)
}

func (i *instrumentInterceptor) GetTransactionReceipt(ctx context.Context, txHash common.Hash) (json.RawMessage, error) {
	return i.intercept(
		ctx,
		i.metrics.instrumentGetTransactionReceipt,
		func(ctx context.Context) (json.RawMessage, error) {
			return i.next.GetTransactionReceipt(ctx, txHash)
		},
		jsonrpc.Params{txHash},
	)
}

func (i *instrumentInterceptor) GetTransactionByBlockHashAndIndex(ctx context.Context, blockHash common.Hash, index rpc.DecimalOrHex) (json.RawMessage, error) {
	return i.intercept(
		ctx,
		i.metrics.instrumentGetTransactionByBlockHashAndIndex,
		func(ctx context.Context) (json.RawMessage, error) {
			return i.next.GetTransactionByBlockHashAndIndex(ctx, blockHash, index)
		},
		jsonrpc.Params{blockHash, index},
	)
}

func (i *instrumentInterceptor) GetTransactionByBlockNumberAndIndex(ctx context.Context, blockNumber rpc.BlockNumber, index rpc.DecimalOrHex) (json.RawMessage, error) {
	return i.intercept(
		ctx,
		i.metrics.instrumentGetTransactionByBlockNumberAndIndex,
		func(ctx context.Context) (json.RawMessage, error) {
			return i.next.GetTransactionByBlockNumberAndIndex(ctx, blockNumber, index)
		},
		jsonrpc.Params{blockNumber, index},
	)
}

func (i *instrumentInterceptor) TraceBlockByHash(ctx context.Context, hash common.Hash, traceConfig *tracers.TraceConfig) (json.RawMessage, error) {
	return i.intercept(
		ctx,
		i.metrics.instrumentTraceBlockByHash,
		func(ctx context.Context) (json.RawMessage, error) {
			return i.next.TraceBlockByHash(ctx, hash, traceConfig)
		},
		jsonrpc.Params{hash, traceConfig},
	)
}

func (i *instrumentInterceptor) TraceBlockByNumber(ctx context.Context, blockNumber rpc.BlockNumber, traceConfig *tracers.TraceConfig) (json.RawMessage, error) {
	return i.intercept(
		ctx,
		i.metrics.instrumentTraceBlockByNumber,
		func(ctx context.Context) (json.RawMessage, error) {
			return i.next.TraceBlockByNumber(ctx, blockNumber, traceConfig)
		},
		jsonrpc.Params{blockNumber, traceConfig},
	)
}

func (i *instrumentInterceptor) Block(ctx context.Context, blockNumber rpc.BlockNumber) (json.RawMessage, error) {
	return i.intercept(
		ctx,
		i.metrics.instrumentArbtraceBlock,
		func(ctx context.Context) (json.RawMessage, error) {
			return i.next.Block(ctx, blockNumber)
		},
		jsonrpc.Params{blockNumber},
	)
}

func (i *instrumentInterceptor) ChainId(ctx context.Context) (json.RawMessage, error) {
	return i.intercept(
		ctx,
		i.metrics.instrumentChainId,
		func(ctx context.Context) (json.RawMessage, error) {
			return i.next.ChainId(ctx)
		},
		nil,
	)
}

func (i *instrumentInterceptor) Version(ctx context.Context) (json.RawMessage, error) {
	return i.intercept(
		ctx,
		i.metrics.instrumentVersion,
		func(ctx context.Context) (json.RawMessage, error) {
			return i.next.Version(ctx)
		},
		nil,
	)
}

func (i *instrumentInterceptor) Syncing(ctx context.Context) (json.RawMessage, error) {
	return i.intercept(
		ctx,
		i.metrics.instrumentSyncing,
		func(ctx context.Context) (json.RawMessage, error) {
			return i.next.Syncing(ctx)
		},
		nil,
	)
}

func (i *instrumentInterceptor) Listening(ctx context.Context) (json.RawMessage, error) {
	return i.intercept(
		ctx,
		i.metrics.instrumentListening,
		func(ctx context.Context) (json.RawMessage, error) {
			return i.next.Listening(ctx)
		},
		nil,
	)
}

func (i *instrumentInterceptor) GetAuthor(ctx context.Context, blockNumber rpc.BlockNumber) (json.RawMessage, error) {
	return i.intercept(
		ctx,
		i.metrics.instrumentGetAuthor,
		func(ctx context.Context) (json.RawMessage, error) {
			return i.next.GetAuthor(ctx, blockNumber)
		},
		jsonrpc.Params{blockNumber},
	)
}

func (i *instrumentInterceptor) intercept(ctx context.Context, call instrument.Call, receiver receiverFn, params jsonrpc.Params) (json.RawMessage, error) {
	var res json.RawMessage
	err := call.Instrument(
		ctx,
		func(ctx context.Context) error {
			v, err := receiver(ctx)
			if err != nil {
				return err
			}

			res = v
			return nil
		},
		instrument.WithLoggerFields(
			zap.Reflect("params", params),
		),
	)
	return res, err
}

func newMetrics(scope tally.Scope, logger *zap.Logger) *metrics {
	scope = scope.SubScope(handlerScopeName)
	return &metrics{
		instrumentBlockNumber:                         newInstrument(EthBlockNumber, scope, logger, filterClientError),
		instrumentGetBlockByHash:                      newInstrument(EthGetBlockByHash, scope, logger, filterClientError),
		instrumentGetBlockByNumber:                    newInstrument(EthGetBlockByNumber, scope, logger, filterClientError),
		instrumentGetBlockByNumberProxy:               newInstrumentWithProxyTag(EthGetBlockByNumber, scope, logger, filterClientError, true),
		instrumentGetBlockTransactionCountByHash:      newInstrument(EthGetBlockTransactionCountByHash, scope, logger, filterClientError),
		instrumentGetBlockTransactionCountByNumber:    newInstrument(EthGetBlockTransactionCountByNumber, scope, logger, filterClientError),
		instrumentGetUncleCountByBlockHash:            newInstrument(EthGetUncleCountByBlockHash, scope, logger, filterClientError),
		instrumentGetUncleCountByBlockNumber:          newInstrument(EthGetUncleCountByBlockNumber, scope, logger, filterClientError),
		instrumentGetLogs:                             newInstrument(EthGetLogs, scope, logger, filterClientError),
		instrumentGetTransactionByHash:                newInstrument(EthGetTransactionByHash, scope, logger, filterClientError),
		instrumentGetTransactionReceipt:               newInstrument(EthGetTransactionReceipt, scope, logger, filterClientError),
		instrumentGetTransactionByBlockHashAndIndex:   newInstrument(EthGetTransactionByBlockHashAndIndex, scope, logger, filterClientError),
		instrumentGetTransactionByBlockNumberAndIndex: newInstrument(EthGetTransactionByBlockNumberAndIndex, scope, logger, filterClientError),
		instrumentTraceBlockByHash:                    newInstrument(DebugTraceBlockByHash, scope, logger, filterClientError),
		instrumentTraceBlockByNumber:                  newInstrument(DebugTraceBlockByNumber, scope, logger, filterClientError),
		instrumentArbtraceBlock:                       newInstrument(ArbtraceBlock, scope, logger, filterClientError),
		instrumentChainId:                             newInstrument(EthChainId, scope, logger, filterClientError),
		instrumentVersion:                             newInstrument(NetVersion, scope, logger, filterClientError),
		instrumentSyncing:                             newInstrument(EthSyncing, scope, logger, filterClientError),
		instrumentListening:                           newInstrument(NetListening, scope, logger, filterClientError),
		instrumentGetAuthor:                           newInstrument(BorGetAuthor, scope, logger, filterClientError),
	}
}

func newInstrument(method *jsonrpc.RequestMethod, scope tally.Scope, logger *zap.Logger, filterError instrument.FilterFn) instrument.Call {
	isProxy := proxyMethods[method.Name]
	return newInstrumentWithProxyTag(method, scope, logger, filterError, isProxy)
}

func newInstrumentWithProxyTag(method *jsonrpc.RequestMethod, scope tally.Scope, logger *zap.Logger, filterError instrument.FilterFn, isProxy bool) instrument.Call {
	methodName := method.Name
	latencyLevel := getLatencyLevel(methodName)

	scope = scope.Tagged(map[string]string{
		methodField:       methodName,
		proxyField:        strconv.FormatBool(isProxy),
		latencyLevelField: latencyLevel,
	})
	logger = logger.With(
		zap.String(methodField, methodName),
		zap.Bool(proxyField, isProxy),
		zap.String(latencyLevelField, latencyLevel),
	)
	return instrument.NewCall(
		scope,
		"request",
		instrument.WithLogger(logger, loggerMsg),
		instrument.WithTracer("handler.request", map[string]string{methodField: methodName}),
		instrument.WithFilter(filterError),
	)
}

func getLatencyLevel(methodName string) string {
	if latencyLevel, ok := methodLatencyLevelMap[methodName]; ok {
		return latencyLevel
	}

	return LatencyLevelDefault
}
