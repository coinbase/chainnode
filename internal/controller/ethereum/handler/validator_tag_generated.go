// Code is generated by Template API Generator (TAG). DO NOT EDIT.
package handler

import (
	"context"
	"encoding/json"
)

func (v *receiverValidator) Call(ctx context.Context, callObject interface{}, blockNumber interface{}) (json.RawMessage, error) {
	return v.receiver.Call(ctx, callObject, blockNumber)
}
func (v *receiverValidator) GetBalance(ctx context.Context, address interface{}, blockNrOrHash interface{}) (json.RawMessage, error) {
	return v.receiver.GetBalance(ctx, address, blockNrOrHash)
}
func (v *receiverValidator) GetCode(ctx context.Context, address interface{}, blockNrOrHash interface{}) (json.RawMessage, error) {
	return v.receiver.GetCode(ctx, address, blockNrOrHash)
}
func (v *receiverValidator) GetTransactionCount(ctx context.Context, address interface{}, blockNrOrHash interface{}) (json.RawMessage, error) {
	return v.receiver.GetTransactionCount(ctx, address, blockNrOrHash)
}
func (v *receiverValidator) SendRawTransaction(ctx context.Context, encodedTx interface{}) (json.RawMessage, error) {
	return v.receiver.SendRawTransaction(ctx, encodedTx)
}
func (v *receiverValidator) GasPrice(ctx context.Context) (json.RawMessage, error) {
	return v.receiver.GasPrice(ctx)
}
func (v *receiverValidator) GetStorageAt(ctx context.Context, address interface{}, key interface{}, blockNrOrHash interface{}) (json.RawMessage, error) {
	return v.receiver.GetStorageAt(ctx, address, key, blockNrOrHash)
}
func (v *receiverValidator) EstimateGas(ctx context.Context, args interface{}, blockNrOrHash *interface{}) (json.RawMessage, error) {
	return v.receiver.EstimateGas(ctx, args, blockNrOrHash)
}
func (v *receiverValidator) ProtocolVersion(ctx context.Context) (json.RawMessage, error) {
	return v.receiver.ProtocolVersion(ctx)
}
func (v *receiverValidator) FeeHistory(ctx context.Context, blockCount interface{}, lastBlock interface{}, rewardPercentiles interface{}) (json.RawMessage, error) {
	return v.receiver.FeeHistory(ctx, blockCount, lastBlock, rewardPercentiles)
}
func (v *receiverValidator) Mining(ctx context.Context) (json.RawMessage, error) {
	return v.receiver.Mining(ctx)
}
func (v *receiverValidator) Hashrate(ctx context.Context) (json.RawMessage, error) {
	return v.receiver.Hashrate(ctx)
}
func (v *receiverValidator) Accounts(ctx context.Context) (json.RawMessage, error) {
	return v.receiver.Accounts(ctx)
}
func (v *receiverValidator) NewFilter(ctx context.Context, criteria interface{}) (json.RawMessage, error) {
	return v.receiver.NewFilter(ctx, criteria)
}
func (v *receiverValidator) NewBlockFilter(ctx context.Context) (json.RawMessage, error) {
	return v.receiver.NewBlockFilter(ctx)
}
func (v *receiverValidator) UninstallFilter(ctx context.Context, id interface{}) (json.RawMessage, error) {
	return v.receiver.UninstallFilter(ctx, id)
}
func (v *receiverValidator) GetFilterChanges(ctx context.Context, id interface{}) (json.RawMessage, error) {
	return v.receiver.GetFilterChanges(ctx, id)
}
func (v *receiverValidator) GetFilterLogs(ctx context.Context, id interface{}) (json.RawMessage, error) {
	return v.receiver.GetFilterLogs(ctx, id)
}
func (v *receiverValidator) GetWork(ctx context.Context) (json.RawMessage, error) {
	return v.receiver.GetWork(ctx)
}
func (v *receiverValidator) SubmitWork(ctx context.Context, nonce interface{}, hash interface{}, digest interface{}) (json.RawMessage, error) {
	return v.receiver.SubmitWork(ctx, nonce, hash, digest)
}
func (v *receiverValidator) SubmitHashrate(ctx context.Context, rate interface{}, id interface{}) (json.RawMessage, error) {
	return v.receiver.SubmitHashrate(ctx, rate, id)
}
func (v *receiverValidator) MaxPriorityFeePerGas(ctx context.Context) (json.RawMessage, error) {
	return v.receiver.MaxPriorityFeePerGas(ctx)
}
func (v *receiverValidator) GetProof(ctx context.Context, address interface{}, storageKeys interface{}, blockNrOrHash interface{}) (json.RawMessage, error) {
	return v.receiver.GetProof(ctx, address, storageKeys, blockNrOrHash)
}
func (v *receiverValidator) PeerCount(ctx context.Context) (json.RawMessage, error) {
	return v.receiver.PeerCount(ctx)
}
func (v *receiverValidator) ClientVersion(ctx context.Context) (json.RawMessage, error) {
	return v.receiver.ClientVersion(ctx)
}
