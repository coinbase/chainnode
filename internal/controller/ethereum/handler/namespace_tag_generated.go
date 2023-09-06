// Code is generated by Template API Generator (TAG). DO NOT EDIT.
package handler

import (
	"context"
	"encoding/json"
)

// EthNamespace
func (n EthNamespace) Call(ctx context.Context, callObject interface{}, blockNumber interface{}) (json.RawMessage, error) {
	return n.receiver.Call(ctx, callObject, blockNumber)
}
func (n EthNamespace) GetBalance(ctx context.Context, address interface{}, blockNrOrHash interface{}) (json.RawMessage, error) {
	return n.receiver.GetBalance(ctx, address, blockNrOrHash)
}
func (n EthNamespace) GetCode(ctx context.Context, address interface{}, blockNrOrHash interface{}) (json.RawMessage, error) {
	return n.receiver.GetCode(ctx, address, blockNrOrHash)
}
func (n EthNamespace) GetTransactionCount(ctx context.Context, address interface{}, blockNrOrHash interface{}) (json.RawMessage, error) {
	return n.receiver.GetTransactionCount(ctx, address, blockNrOrHash)
}
func (n EthNamespace) SendRawTransaction(ctx context.Context, encodedTx interface{}) (json.RawMessage, error) {
	return n.receiver.SendRawTransaction(ctx, encodedTx)
}
func (n EthNamespace) GasPrice(ctx context.Context) (json.RawMessage, error) {
	return n.receiver.GasPrice(ctx)
}
func (n EthNamespace) GetStorageAt(ctx context.Context, address interface{}, key interface{}, blockNrOrHash interface{}) (json.RawMessage, error) {
	return n.receiver.GetStorageAt(ctx, address, key, blockNrOrHash)
}
func (n EthNamespace) EstimateGas(ctx context.Context, args interface{}, blockNrOrHash *interface{}) (json.RawMessage, error) {
	return n.receiver.EstimateGas(ctx, args, blockNrOrHash)
}
func (n EthNamespace) ProtocolVersion(ctx context.Context) (json.RawMessage, error) {
	return n.receiver.ProtocolVersion(ctx)
}
func (n EthNamespace) FeeHistory(ctx context.Context, blockCount interface{}, lastBlock interface{}, rewardPercentiles interface{}) (json.RawMessage, error) {
	return n.receiver.FeeHistory(ctx, blockCount, lastBlock, rewardPercentiles)
}
func (n EthNamespace) Mining(ctx context.Context) (json.RawMessage, error) {
	return n.receiver.Mining(ctx)
}
func (n EthNamespace) Hashrate(ctx context.Context) (json.RawMessage, error) {
	return n.receiver.Hashrate(ctx)
}
func (n EthNamespace) Accounts(ctx context.Context) (json.RawMessage, error) {
	return n.receiver.Accounts(ctx)
}
func (n EthNamespace) NewFilter(ctx context.Context, criteria interface{}) (json.RawMessage, error) {
	return n.receiver.NewFilter(ctx, criteria)
}
func (n EthNamespace) NewBlockFilter(ctx context.Context) (json.RawMessage, error) {
	return n.receiver.NewBlockFilter(ctx)
}
func (n EthNamespace) UninstallFilter(ctx context.Context, id interface{}) (json.RawMessage, error) {
	return n.receiver.UninstallFilter(ctx, id)
}
func (n EthNamespace) GetFilterChanges(ctx context.Context, id interface{}) (json.RawMessage, error) {
	return n.receiver.GetFilterChanges(ctx, id)
}
func (n EthNamespace) GetFilterLogs(ctx context.Context, id interface{}) (json.RawMessage, error) {
	return n.receiver.GetFilterLogs(ctx, id)
}
func (n EthNamespace) GetWork(ctx context.Context) (json.RawMessage, error) {
	return n.receiver.GetWork(ctx)
}
func (n EthNamespace) SubmitWork(ctx context.Context, nonce interface{}, hash interface{}, digest interface{}) (json.RawMessage, error) {
	return n.receiver.SubmitWork(ctx, nonce, hash, digest)
}
func (n EthNamespace) SubmitHashrate(ctx context.Context, rate interface{}, id interface{}) (json.RawMessage, error) {
	return n.receiver.SubmitHashrate(ctx, rate, id)
}
func (n EthNamespace) MaxPriorityFeePerGas(ctx context.Context) (json.RawMessage, error) {
	return n.receiver.MaxPriorityFeePerGas(ctx)
}
func (n EthNamespace) GetProof(ctx context.Context, address interface{}, storageKeys interface{}, blockNrOrHash interface{}) (json.RawMessage, error) {
	return n.receiver.GetProof(ctx, address, storageKeys, blockNrOrHash)
}

// NetNamespace
func (n NetNamespace) PeerCount(ctx context.Context) (json.RawMessage, error) {
	return n.receiver.PeerCount(ctx)
}

// Web3Namespace
func (n Web3Namespace) ClientVersion(ctx context.Context) (json.RawMessage, error) {
	return n.receiver.ClientVersion(ctx)
}