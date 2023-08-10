// Code is generated by Template API Generator (TAG). DO NOT EDIT.
package handler

import (
	"context"
	"encoding/json"

	"github.com/uber-go/tally/v4"
	"go.uber.org/zap"

	"github.com/coinbase/chainnode/internal/clients/blockchain/jsonrpc"
	"github.com/coinbase/chainnode/internal/utils/instrument"
)

type (
	proxyMetrics struct {
		instrumentCall                 instrument.Call
		instrumentGetBalance           instrument.Call
		instrumentGetCode              instrument.Call
		instrumentGetTransactionCount  instrument.Call
		instrumentSendRawTransaction   instrument.Call
		instrumentGasPrice             instrument.Call
		instrumentGetStorageAt         instrument.Call
		instrumentEstimateGas          instrument.Call
		instrumentProtocolVersion      instrument.Call
		instrumentFeeHistory           instrument.Call
		instrumentMining               instrument.Call
		instrumentHashrate             instrument.Call
		instrumentAccounts             instrument.Call
		instrumentNewFilter            instrument.Call
		instrumentNewBlockFilter       instrument.Call
		instrumentUninstallFilter      instrument.Call
		instrumentGetFilterChanges     instrument.Call
		instrumentGetFilterLogs        instrument.Call
		instrumentGetWork              instrument.Call
		instrumentSubmitWork           instrument.Call
		instrumentSubmitHashrate       instrument.Call
		instrumentMaxPriorityFeePerGas instrument.Call
		instrumentGetProof             instrument.Call
		instrumentPeerCount            instrument.Call
		instrumentClientVersion        instrument.Call
	}
)

func newProxyMetrics(scope tally.Scope, logger *zap.Logger) *proxyMetrics {
	scope = scope.SubScope(handlerScopeName)
	return &proxyMetrics{
		instrumentCall:                 newInstrument(EthCall, scope, logger, filterProxyError),
		instrumentGetBalance:           newInstrument(EthGetBalance, scope, logger, filterProxyError),
		instrumentGetCode:              newInstrument(EthGetCode, scope, logger, filterProxyError),
		instrumentGetTransactionCount:  newInstrument(EthGetTransactionCount, scope, logger, filterProxyError),
		instrumentSendRawTransaction:   newInstrument(EthSendRawTransaction, scope, logger, filterProxyError),
		instrumentGasPrice:             newInstrument(EthGasPrice, scope, logger, filterProxyError),
		instrumentGetStorageAt:         newInstrument(EthGetStorageAt, scope, logger, filterProxyError),
		instrumentEstimateGas:          newInstrument(EthEstimateGas, scope, logger, filterProxyError),
		instrumentProtocolVersion:      newInstrument(EthProtocolVersion, scope, logger, filterProxyError),
		instrumentFeeHistory:           newInstrument(EthFeeHistory, scope, logger, filterProxyError),
		instrumentMining:               newInstrument(EthMining, scope, logger, filterProxyError),
		instrumentHashrate:             newInstrument(EthHashrate, scope, logger, filterProxyError),
		instrumentAccounts:             newInstrument(EthAccounts, scope, logger, filterProxyError),
		instrumentNewFilter:            newInstrument(EthNewFilter, scope, logger, filterProxyError),
		instrumentNewBlockFilter:       newInstrument(EthNewBlockFilter, scope, logger, filterProxyError),
		instrumentUninstallFilter:      newInstrument(EthUninstallFilter, scope, logger, filterProxyError),
		instrumentGetFilterChanges:     newInstrument(EthGetFilterChanges, scope, logger, filterProxyError),
		instrumentGetFilterLogs:        newInstrument(EthGetFilterLogs, scope, logger, filterProxyError),
		instrumentGetWork:              newInstrument(EthGetWork, scope, logger, filterProxyError),
		instrumentSubmitWork:           newInstrument(EthSubmitWork, scope, logger, filterProxyError),
		instrumentSubmitHashrate:       newInstrument(EthSubmitHashrate, scope, logger, filterProxyError),
		instrumentMaxPriorityFeePerGas: newInstrument(EthMaxPriorityFeePerGas, scope, logger, filterProxyError),
		instrumentGetProof:             newInstrument(EthGetProof, scope, logger, filterProxyError),
		instrumentPeerCount:            newInstrument(NetPeerCount, scope, logger, filterProxyError),
		instrumentClientVersion:        newInstrument(Web3ClientVersion, scope, logger, filterProxyError),
	}
}
func (i *instrumentInterceptor) Call(ctx context.Context, callObject interface{}, blockNumber interface{}) (json.RawMessage, error) {
	return i.intercept(
		ctx,
		i.proxyMetrics.instrumentCall,
		func(ctx context.Context) (json.RawMessage, error) {
			return i.next.Call(ctx, callObject, blockNumber)
		},
		jsonrpc.Params{callObject, blockNumber},
	)
}
func (i *instrumentInterceptor) GetBalance(ctx context.Context, address interface{}, blockNrOrHash interface{}) (json.RawMessage, error) {
	return i.intercept(
		ctx,
		i.proxyMetrics.instrumentGetBalance,
		func(ctx context.Context) (json.RawMessage, error) {
			return i.next.GetBalance(ctx, address, blockNrOrHash)
		},
		jsonrpc.Params{address, blockNrOrHash},
	)
}
func (i *instrumentInterceptor) GetCode(ctx context.Context, address interface{}, blockNrOrHash interface{}) (json.RawMessage, error) {
	return i.intercept(
		ctx,
		i.proxyMetrics.instrumentGetCode,
		func(ctx context.Context) (json.RawMessage, error) {
			return i.next.GetCode(ctx, address, blockNrOrHash)
		},
		jsonrpc.Params{address, blockNrOrHash},
	)
}
func (i *instrumentInterceptor) GetTransactionCount(ctx context.Context, address interface{}, blockNrOrHash interface{}) (json.RawMessage, error) {
	return i.intercept(
		ctx,
		i.proxyMetrics.instrumentGetTransactionCount,
		func(ctx context.Context) (json.RawMessage, error) {
			return i.next.GetTransactionCount(ctx, address, blockNrOrHash)
		},
		jsonrpc.Params{address, blockNrOrHash},
	)
}
func (i *instrumentInterceptor) SendRawTransaction(ctx context.Context, encodedTx interface{}) (json.RawMessage, error) {
	return i.intercept(
		ctx,
		i.proxyMetrics.instrumentSendRawTransaction,
		func(ctx context.Context) (json.RawMessage, error) {
			return i.next.SendRawTransaction(ctx, encodedTx)
		},
		jsonrpc.Params{encodedTx},
	)
}
func (i *instrumentInterceptor) GasPrice(ctx context.Context) (json.RawMessage, error) {
	return i.intercept(
		ctx,
		i.proxyMetrics.instrumentGasPrice,
		func(ctx context.Context) (json.RawMessage, error) {
			return i.next.GasPrice(ctx)
		},
		nil,
	)
}
func (i *instrumentInterceptor) GetStorageAt(ctx context.Context, address interface{}, key interface{}, blockNrOrHash interface{}) (json.RawMessage, error) {
	return i.intercept(
		ctx,
		i.proxyMetrics.instrumentGetStorageAt,
		func(ctx context.Context) (json.RawMessage, error) {
			return i.next.GetStorageAt(ctx, address, key, blockNrOrHash)
		},
		jsonrpc.Params{address, key, blockNrOrHash},
	)
}
func (i *instrumentInterceptor) EstimateGas(ctx context.Context, args interface{}, blockNrOrHash *interface{}) (json.RawMessage, error) {
	return i.intercept(
		ctx,
		i.proxyMetrics.instrumentEstimateGas,
		func(ctx context.Context) (json.RawMessage, error) {
			return i.next.EstimateGas(ctx, args, blockNrOrHash)
		},
		jsonrpc.Params{args, blockNrOrHash},
	)
}
func (i *instrumentInterceptor) ProtocolVersion(ctx context.Context) (json.RawMessage, error) {
	return i.intercept(
		ctx,
		i.proxyMetrics.instrumentProtocolVersion,
		func(ctx context.Context) (json.RawMessage, error) {
			return i.next.ProtocolVersion(ctx)
		},
		nil,
	)
}
func (i *instrumentInterceptor) FeeHistory(ctx context.Context, blockCount interface{}, lastBlock interface{}, rewardPercentiles interface{}) (json.RawMessage, error) {
	return i.intercept(
		ctx,
		i.proxyMetrics.instrumentFeeHistory,
		func(ctx context.Context) (json.RawMessage, error) {
			return i.next.FeeHistory(ctx, blockCount, lastBlock, rewardPercentiles)
		},
		jsonrpc.Params{blockCount, lastBlock, rewardPercentiles},
	)
}
func (i *instrumentInterceptor) Mining(ctx context.Context) (json.RawMessage, error) {
	return i.intercept(
		ctx,
		i.proxyMetrics.instrumentMining,
		func(ctx context.Context) (json.RawMessage, error) {
			return i.next.Mining(ctx)
		},
		nil,
	)
}
func (i *instrumentInterceptor) Hashrate(ctx context.Context) (json.RawMessage, error) {
	return i.intercept(
		ctx,
		i.proxyMetrics.instrumentHashrate,
		func(ctx context.Context) (json.RawMessage, error) {
			return i.next.Hashrate(ctx)
		},
		nil,
	)
}
func (i *instrumentInterceptor) Accounts(ctx context.Context) (json.RawMessage, error) {
	return i.intercept(
		ctx,
		i.proxyMetrics.instrumentAccounts,
		func(ctx context.Context) (json.RawMessage, error) {
			return i.next.Accounts(ctx)
		},
		nil,
	)
}
func (i *instrumentInterceptor) NewFilter(ctx context.Context, criteria interface{}) (json.RawMessage, error) {
	return i.intercept(
		ctx,
		i.proxyMetrics.instrumentNewFilter,
		func(ctx context.Context) (json.RawMessage, error) {
			return i.next.NewFilter(ctx, criteria)
		},
		jsonrpc.Params{criteria},
	)
}
func (i *instrumentInterceptor) NewBlockFilter(ctx context.Context) (json.RawMessage, error) {
	return i.intercept(
		ctx,
		i.proxyMetrics.instrumentNewBlockFilter,
		func(ctx context.Context) (json.RawMessage, error) {
			return i.next.NewBlockFilter(ctx)
		},
		nil,
	)
}
func (i *instrumentInterceptor) UninstallFilter(ctx context.Context, id interface{}) (json.RawMessage, error) {
	return i.intercept(
		ctx,
		i.proxyMetrics.instrumentUninstallFilter,
		func(ctx context.Context) (json.RawMessage, error) {
			return i.next.UninstallFilter(ctx, id)
		},
		jsonrpc.Params{id},
	)
}
func (i *instrumentInterceptor) GetFilterChanges(ctx context.Context, id interface{}) (json.RawMessage, error) {
	return i.intercept(
		ctx,
		i.proxyMetrics.instrumentGetFilterChanges,
		func(ctx context.Context) (json.RawMessage, error) {
			return i.next.GetFilterChanges(ctx, id)
		},
		jsonrpc.Params{id},
	)
}
func (i *instrumentInterceptor) GetFilterLogs(ctx context.Context, id interface{}) (json.RawMessage, error) {
	return i.intercept(
		ctx,
		i.proxyMetrics.instrumentGetFilterLogs,
		func(ctx context.Context) (json.RawMessage, error) {
			return i.next.GetFilterLogs(ctx, id)
		},
		jsonrpc.Params{id},
	)
}
func (i *instrumentInterceptor) GetWork(ctx context.Context) (json.RawMessage, error) {
	return i.intercept(
		ctx,
		i.proxyMetrics.instrumentGetWork,
		func(ctx context.Context) (json.RawMessage, error) {
			return i.next.GetWork(ctx)
		},
		nil,
	)
}
func (i *instrumentInterceptor) SubmitWork(ctx context.Context, nonce interface{}, hash interface{}, digest interface{}) (json.RawMessage, error) {
	return i.intercept(
		ctx,
		i.proxyMetrics.instrumentSubmitWork,
		func(ctx context.Context) (json.RawMessage, error) {
			return i.next.SubmitWork(ctx, nonce, hash, digest)
		},
		jsonrpc.Params{nonce, hash, digest},
	)
}
func (i *instrumentInterceptor) SubmitHashrate(ctx context.Context, rate interface{}, id interface{}) (json.RawMessage, error) {
	return i.intercept(
		ctx,
		i.proxyMetrics.instrumentSubmitHashrate,
		func(ctx context.Context) (json.RawMessage, error) {
			return i.next.SubmitHashrate(ctx, rate, id)
		},
		jsonrpc.Params{rate, id},
	)
}
func (i *instrumentInterceptor) MaxPriorityFeePerGas(ctx context.Context) (json.RawMessage, error) {
	return i.intercept(
		ctx,
		i.proxyMetrics.instrumentMaxPriorityFeePerGas,
		func(ctx context.Context) (json.RawMessage, error) {
			return i.next.MaxPriorityFeePerGas(ctx)
		},
		nil,
	)
}
func (i *instrumentInterceptor) GetProof(ctx context.Context, address interface{}, storageKeys interface{}, blockNrOrHash interface{}) (json.RawMessage, error) {
	return i.intercept(
		ctx,
		i.proxyMetrics.instrumentGetProof,
		func(ctx context.Context) (json.RawMessage, error) {
			return i.next.GetProof(ctx, address, storageKeys, blockNrOrHash)
		},
		jsonrpc.Params{address, storageKeys, blockNrOrHash},
	)
}
func (i *instrumentInterceptor) PeerCount(ctx context.Context) (json.RawMessage, error) {
	return i.intercept(
		ctx,
		i.proxyMetrics.instrumentPeerCount,
		func(ctx context.Context) (json.RawMessage, error) {
			return i.next.PeerCount(ctx)
		},
		nil,
	)
}
func (i *instrumentInterceptor) ClientVersion(ctx context.Context) (json.RawMessage, error) {
	return i.intercept(
		ctx,
		i.proxyMetrics.instrumentClientVersion,
		func(ctx context.Context) (json.RawMessage, error) {
			return i.next.ClientVersion(ctx)
		},
		nil,
	)
}
