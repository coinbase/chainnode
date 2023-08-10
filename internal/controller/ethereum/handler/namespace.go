package handler

import (
	"context"
	"encoding/json"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/eth/tracers"

	"github.com/coinbase/chainnode/internal/server/rpc"
)

const (
	NamespaceEth      = "eth"
	NamespaceDebug    = "debug"
	NamespaceNet      = "net"
	NamespaceWeb3     = "web3"
	NamespaceArbtrace = "arbtrace"
	NamespaceBor      = "bor"
)

type (
	EthNamespace struct {
		receiver Receiver
	}

	DebugNamespace struct {
		receiver Receiver
	}

	NetNamespace struct {
		receiver Receiver
	}

	Web3Namespace struct {
		receiver Receiver
	}

	ArbtraceNamespace struct {
		receiver Receiver
	}

	BorNamespace struct {
		receiver Receiver
	}
)

func NewEthNamespace(receiver Receiver) *EthNamespace {
	return &EthNamespace{receiver: receiver}
}

func NewDebugNamespace(receiver Receiver) *DebugNamespace {
	return &DebugNamespace{receiver: receiver}
}

func NewNetNamespace(receiver Receiver) *NetNamespace {
	return &NetNamespace{receiver: receiver}
}

func NewWeb3Namespace(receiver Receiver) *Web3Namespace {
	return &Web3Namespace{receiver: receiver}
}

func NewArbtraceNamespace(receiver Receiver) *ArbtraceNamespace {
	return &ArbtraceNamespace{receiver: receiver}
}

func NewBorNamespace(receiver Receiver) *BorNamespace {
	return &BorNamespace{receiver: receiver}
}

// eth namespace
func (n EthNamespace) BlockNumber(ctx context.Context) (json.RawMessage, error) {
	return n.receiver.BlockNumber(ctx)
}

func (n EthNamespace) GetBlockByHash(ctx context.Context, blockHash common.Hash, returnFullTx bool) (json.RawMessage, error) {
	return n.receiver.GetBlockByHash(ctx, blockHash, returnFullTx)
}

func (n EthNamespace) GetBlockByNumber(ctx context.Context, blockNumber rpc.BlockNumber, returnFullTx bool) (json.RawMessage, error) {
	return n.receiver.GetBlockByNumber(ctx, blockNumber, returnFullTx)
}

func (n EthNamespace) GetBlockTransactionCountByHash(ctx context.Context, blockHash common.Hash) (json.RawMessage, error) {
	return n.receiver.GetBlockTransactionCountByHash(ctx, blockHash)
}

func (n EthNamespace) GetBlockTransactionCountByNumber(ctx context.Context, blockNumber rpc.BlockNumber) (json.RawMessage, error) {
	return n.receiver.GetBlockTransactionCountByNumber(ctx, blockNumber)
}

func (n EthNamespace) GetUncleCountByBlockHash(ctx context.Context, blockHash common.Hash) (json.RawMessage, error) {
	return n.receiver.GetUncleCountByBlockHash(ctx, blockHash)
}

func (n EthNamespace) GetUncleCountByBlockNumber(ctx context.Context, blockNumber rpc.BlockNumber) (json.RawMessage, error) {
	return n.receiver.GetUncleCountByBlockNumber(ctx, blockNumber)
}

func (n EthNamespace) GetLogs(ctx context.Context, criteria FilterCriteria) (json.RawMessage, error) {
	return n.receiver.GetLogs(ctx, criteria)
}

func (n EthNamespace) GetTransactionByHash(ctx context.Context, txHash common.Hash) (json.RawMessage, error) {
	return n.receiver.GetTransactionByHash(ctx, txHash)
}

func (n EthNamespace) GetTransactionReceipt(ctx context.Context, txHash common.Hash) (json.RawMessage, error) {
	return n.receiver.GetTransactionReceipt(ctx, txHash)
}

func (n EthNamespace) GetTransactionByBlockHashAndIndex(ctx context.Context, blockHash common.Hash, index rpc.DecimalOrHex) (json.RawMessage, error) {
	return n.receiver.GetTransactionByBlockHashAndIndex(ctx, blockHash, index)
}

func (n EthNamespace) GetTransactionByBlockNumberAndIndex(ctx context.Context, blockNumber rpc.BlockNumber, index rpc.DecimalOrHex) (json.RawMessage, error) {
	return n.receiver.GetTransactionByBlockNumberAndIndex(ctx, blockNumber, index)
}

func (n EthNamespace) ChainId(ctx context.Context) (json.RawMessage, error) {
	return n.receiver.ChainId(ctx)
}

func (n EthNamespace) Syncing(ctx context.Context) (json.RawMessage, error) {
	return n.receiver.Syncing(ctx)
}

// debug namespace
func (n DebugNamespace) TraceBlockByHash(ctx context.Context, hash common.Hash, traceConfig *tracers.TraceConfig) (json.RawMessage, error) {
	return n.receiver.TraceBlockByHash(ctx, hash, traceConfig)
}

func (n DebugNamespace) TraceBlockByNumber(ctx context.Context, blockNumber rpc.BlockNumber, traceConfig *tracers.TraceConfig) (json.RawMessage, error) {
	return n.receiver.TraceBlockByNumber(ctx, blockNumber, traceConfig)
}

// net namespace
func (n NetNamespace) Version(ctx context.Context) (json.RawMessage, error) {
	return n.receiver.Version(ctx)
}

func (n NetNamespace) Listening(ctx context.Context) (json.RawMessage, error) {
	return n.receiver.Listening(ctx)
}

// arbtrace namespace
func (n ArbtraceNamespace) Block(ctx context.Context, blockNumber rpc.BlockNumber) (json.RawMessage, error) {
	return n.receiver.Block(ctx, blockNumber)
}

// bor namespace
func (n BorNamespace) GetAuthor(ctx context.Context, blockNumber rpc.BlockNumber) (json.RawMessage, error) {
	return n.receiver.GetAuthor(ctx, blockNumber)
}
