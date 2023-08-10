package handler

import (
	"context"
	"encoding/json"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/eth/tracers"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainnode/internal/clients/blockchain/jsonrpc"
	"github.com/coinbase/chainnode/internal/server/rpc"
)

type (
	errorInterceptor struct {
		next Receiver
	}
)

func WithErrorInterceptor(next Receiver) Receiver {
	return &errorInterceptor{
		next: next,
	}
}

func (i *errorInterceptor) BlockNumber(ctx context.Context) (json.RawMessage, error) {
	return i.intercept(ctx, func(ctx context.Context) (json.RawMessage, error) {
		return i.next.BlockNumber(ctx)
	})
}

func (i *errorInterceptor) GetBlockByNumber(ctx context.Context, blockNumber rpc.BlockNumber, returnFullTx bool) (json.RawMessage, error) {
	return i.intercept(ctx, func(ctx context.Context) (json.RawMessage, error) {
		return i.next.GetBlockByNumber(ctx, blockNumber, returnFullTx)
	})
}

func (i *errorInterceptor) GetBlockByHash(ctx context.Context, blockHash common.Hash, returnFullTx bool) (json.RawMessage, error) {
	return i.intercept(ctx, func(ctx context.Context) (json.RawMessage, error) {
		return i.next.GetBlockByHash(ctx, blockHash, returnFullTx)
	})
}

func (i *errorInterceptor) GetBlockTransactionCountByHash(ctx context.Context, blockHash common.Hash) (json.RawMessage, error) {
	return i.intercept(ctx, func(ctx context.Context) (json.RawMessage, error) {
		return i.next.GetBlockTransactionCountByHash(ctx, blockHash)
	})
}

func (i *errorInterceptor) GetBlockTransactionCountByNumber(ctx context.Context, blockNumber rpc.BlockNumber) (json.RawMessage, error) {
	return i.intercept(ctx, func(ctx context.Context) (json.RawMessage, error) {
		return i.next.GetBlockTransactionCountByNumber(ctx, blockNumber)
	})
}

func (i *errorInterceptor) GetUncleCountByBlockHash(ctx context.Context, blockHash common.Hash) (json.RawMessage, error) {
	return i.intercept(ctx, func(ctx context.Context) (json.RawMessage, error) {
		return i.next.GetUncleCountByBlockHash(ctx, blockHash)
	})
}

func (i *errorInterceptor) GetUncleCountByBlockNumber(ctx context.Context, blockNumber rpc.BlockNumber) (json.RawMessage, error) {
	return i.intercept(ctx, func(ctx context.Context) (json.RawMessage, error) {
		return i.next.GetUncleCountByBlockNumber(ctx, blockNumber)
	})
}

func (i *errorInterceptor) GetLogs(ctx context.Context, criteria FilterCriteria) (json.RawMessage, error) {
	return i.intercept(ctx, func(ctx context.Context) (json.RawMessage, error) {
		return i.next.GetLogs(ctx, criteria)
	})
}

func (i *errorInterceptor) GetTransactionByHash(ctx context.Context, txHash common.Hash) (json.RawMessage, error) {
	return i.intercept(ctx, func(ctx context.Context) (json.RawMessage, error) {
		return i.next.GetTransactionByHash(ctx, txHash)
	})
}

func (i *errorInterceptor) GetTransactionReceipt(ctx context.Context, txHash common.Hash) (json.RawMessage, error) {
	return i.intercept(ctx, func(ctx context.Context) (json.RawMessage, error) {
		return i.next.GetTransactionReceipt(ctx, txHash)
	})
}

func (i *errorInterceptor) GetTransactionByBlockHashAndIndex(ctx context.Context, blockHash common.Hash, index rpc.DecimalOrHex) (json.RawMessage, error) {
	return i.intercept(ctx, func(ctx context.Context) (json.RawMessage, error) {
		return i.next.GetTransactionByBlockHashAndIndex(ctx, blockHash, index)
	})
}

func (i *errorInterceptor) GetTransactionByBlockNumberAndIndex(ctx context.Context, blockNumber rpc.BlockNumber, index rpc.DecimalOrHex) (json.RawMessage, error) {
	return i.intercept(ctx, func(ctx context.Context) (json.RawMessage, error) {
		return i.next.GetTransactionByBlockNumberAndIndex(ctx, blockNumber, index)
	})
}

func (i *errorInterceptor) TraceBlockByHash(ctx context.Context, hash common.Hash, traceConfig *tracers.TraceConfig) (json.RawMessage, error) {
	return i.intercept(ctx, func(ctx context.Context) (json.RawMessage, error) {
		return i.next.TraceBlockByHash(ctx, hash, traceConfig)
	})
}

func (i *errorInterceptor) TraceBlockByNumber(ctx context.Context, blockNumber rpc.BlockNumber, traceConfig *tracers.TraceConfig) (json.RawMessage, error) {
	return i.intercept(ctx, func(ctx context.Context) (json.RawMessage, error) {
		return i.next.TraceBlockByNumber(ctx, blockNumber, traceConfig)
	})
}

func (i *errorInterceptor) Block(ctx context.Context, blockNumber rpc.BlockNumber) (json.RawMessage, error) {
	return i.intercept(ctx, func(ctx context.Context) (json.RawMessage, error) {
		return i.next.Block(ctx, blockNumber)
	})
}

func (i *errorInterceptor) ChainId(ctx context.Context) (json.RawMessage, error) {
	return i.intercept(ctx, func(ctx context.Context) (json.RawMessage, error) {
		return i.next.ChainId(ctx)
	})
}

func (i *errorInterceptor) Version(ctx context.Context) (json.RawMessage, error) {
	return i.intercept(ctx, func(ctx context.Context) (json.RawMessage, error) {
		return i.next.Version(ctx)
	})
}

func (i *errorInterceptor) Syncing(ctx context.Context) (json.RawMessage, error) {
	return i.intercept(ctx, func(ctx context.Context) (json.RawMessage, error) {
		return i.next.Syncing(ctx)
	})
}

func (i *errorInterceptor) Listening(ctx context.Context) (json.RawMessage, error) {
	return i.intercept(ctx, func(ctx context.Context) (json.RawMessage, error) {
		return i.next.Listening(ctx)
	})
}

func (i *errorInterceptor) GetAuthor(ctx context.Context, blockNumber rpc.BlockNumber) (json.RawMessage, error) {
	return i.intercept(ctx, func(ctx context.Context) (json.RawMessage, error) {
		return i.next.GetAuthor(ctx, blockNumber)
	})
}

func (i *errorInterceptor) intercept(ctx context.Context, receiver receiverFn) (json.RawMessage, error) {
	res, err := receiver(ctx)
	return res, i.mapError(err)
}

func (i *errorInterceptor) mapError(err error) error {
	if err == nil {
		return nil
	}

	var rpcErr *jsonrpc.RPCError
	if xerrors.As(err, &rpcErr) {
		// Use the underlying error directly.
		return rpcErr
	}

	if isCanceledError(err) {
		rpcErr = jsonrpc.NewRPCError(errorCodeCanceled, err.Error())
	} else if isNotImplementedError(err) || isNotAllowedError(err) {
		rpcErr = jsonrpc.NewRPCError(errorCodeBadRequest, err.Error())
	} else {
		rpcErr = jsonrpc.NewRPCError(errorCodeInternal, err.Error())
	}

	return rpcErr.WithCause(err)
}
