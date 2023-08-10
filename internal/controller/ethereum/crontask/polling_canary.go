package crontask

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"io/ioutil"
	"math/big"
	"net/http"
	"time"

	gethCommon "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/go-playground/validator/v10"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainnode/internal/api"
	xapi "github.com/coinbase/chainnode/internal/api/ethereum"
	"github.com/coinbase/chainnode/internal/clients/blockchain/jsonrpc"
	"github.com/coinbase/chainnode/internal/config"
	"github.com/coinbase/chainnode/internal/controller/ethereum/handler"
	"github.com/coinbase/chainnode/internal/controller/internal"
	checkpointStorage "github.com/coinbase/chainnode/internal/storage/checkpoint"
	ethereumStorage "github.com/coinbase/chainnode/internal/storage/ethereum"
	"github.com/coinbase/chainnode/internal/utils/finalizer"
	"github.com/coinbase/chainnode/internal/utils/fxparams"
	"github.com/coinbase/chainnode/internal/utils/log"
	"github.com/coinbase/chainnode/internal/utils/syncgroup"

	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
)

type (
	PollingCanaryTaskParams struct {
		fx.In
		fxparams.Params
		JsonrpcClientServer jsonrpc.Client `name:"server"`
		BlockStorage        ethereumStorage.BlockStorage
		TransactionStorgae  ethereumStorage.TransactionStorage
		CheckpointStorage   checkpointStorage.CheckpointStorage
		Config              *config.Config
	}

	pollingCanaryTask struct {
		enabled              bool
		jsonrpcClientServer  jsonrpc.Client
		blockStorage         ethereumStorage.BlockStorage
		transactionStorgae   ethereumStorage.TransactionStorage
		checkpointStorage    checkpointStorage.CheckpointStorage
		tagStable            uint32
		network              common.Network
		irreversibleDistance uint64
		logger               *zap.Logger
		cfg                  *config.Config
		validate             *validator.Validate
		httpClient           *http.Client
	}

	blockLite struct {
		Transactions []transactionLite `json:"transactions"`
		Uncles       []gethCommon.Hash `json:"uncles"`
	}

	blockValidation struct {
		Hash hexutil.Bytes `json:"hash" validate:"required"`
	}

	transactionLite struct {
		Hash string `json:"hash"`
	}

	transactionValidation struct {
		Hash hexutil.Bytes `json:"hash" validate:"required"`
	}

	transactionReceiptValidation struct {
		TransactionHash hexutil.Bytes `json:"transactionHash" validate:"required"`
	}

	logValidation struct {
		BlockHash hexutil.Bytes `json:"blockHash" validate:"required"`
	}

	traceResultValidation struct {
		Result traceValidation `json:"result" validate:"required"`
	}

	traceValidation struct {
		Type  string `json:"type" validate:"required_without=error"`
		Error string `json:"error"`
	}
)

const (
	ethCallTracer = "callTracer"

	httpTimeout = 5 * time.Second

	graphqlRequest = `{ "query": "query { block { number } }" }`
)

var (
	sampleCanaryInput = map[common.Network]struct {
		contractAddress            string
		getBalanceAddress          string
		getCodeAddress             string
		getTransactionCountAddress string
	}{
		common.Network_NETWORK_ETHEREUM_MAINNET: {
			contractAddress:            "0x514910771af9ca656af840dff83e8264ecf986ca",
			getBalanceAddress:          "0x8d97689c9818892b700e27f316cc3e41e17fbeb9",
			getCodeAddress:             "0x7f268357a8c2552623316e2562d90e642bb538e5",
			getTransactionCountAddress: "0xe222489ae12e15713cc1d65dd0ab2f5b18721bfd",
		},
		common.Network_NETWORK_POLYGON_MAINNET: {
			contractAddress:            "0xe5caef4af8780e59df925470b050fb23c43ca68c",
			getBalanceAddress:          "0x2d25c5a60e9948f310a65388dafccfeb8adb8f9e",
			getCodeAddress:             "0xe5caef4af8780e59df925470b050fb23c43ca68c",
			getTransactionCountAddress: "0x3ce07ad298ee2b3aabeA8c8b3f496c3acc51e647",
		},
		common.Network_NETWORK_ETHEREUM_GOERLI: {
			contractAddress:            "0x7d7d7d4D23Af0f9C2DbD25643b86f60A9aB83Afc",
			getBalanceAddress:          "0x3a5606E418Cda21AD8fF43aB38310fb3038037Fa",
			getCodeAddress:             "0x9b12d2A80fad64A5499e70bf74447C352c99fD46",
			getTransactionCountAddress: "0xd3B9Cbea7856AECf4A6F7c3F4E8791F79cBeeD62",
		},
	}
)

func NewPollingCanaryTask(params PollingCanaryTaskParams) (internal.CronTask, error) {
	return &pollingCanaryTask{
		enabled:              !params.Config.Cron.DisablePollingCanary,
		jsonrpcClientServer:  params.JsonrpcClientServer,
		blockStorage:         params.BlockStorage,
		transactionStorgae:   params.TransactionStorgae,
		checkpointStorage:    params.CheckpointStorage,
		tagStable:            params.Config.Tag.Stable,
		network:              params.Config.Chain.Network,
		irreversibleDistance: params.Config.Chain.IrreversibleDistance,
		logger:               log.WithPackage(params.Logger),
		cfg:                  params.Config,
		validate:             validator.New(),
		httpClient: &http.Client{
			Timeout: httpTimeout,
		},
	}, nil
}

func (t *pollingCanaryTask) Name() string {
	return "polling_canary"
}

func (t *pollingCanaryTask) Spec() string {
	return "@every 5s"
}

func (t *pollingCanaryTask) Parallelism() int64 {
	return 1
}

func (t *pollingCanaryTask) Enabled() bool {
	return t.enabled
}

func (t *pollingCanaryTask) DelayStartDuration() time.Duration {
	// delay for 10 minutes in case there is new API being added
	return 10 * time.Minute
}

func (t *pollingCanaryTask) Run(ctx context.Context) error {
	lastChkp, err := t.checkpointStorage.GetCheckpoint(ctx, api.CollectionLatestCheckpoint, t.tagStable)
	if err != nil {
		return xerrors.Errorf("failed to get latest checkpoint: %w", err)
	}

	if lastChkp.Height < t.irreversibleDistance {
		return nil
	}

	// The latest block is subject to chain reorg. Use an earlier block instead.
	height := lastChkp.Height - t.irreversibleDistance
	blockNumber := hexutil.EncodeUint64(height)

	block, err := t.blockStorage.GetBlock(ctx, t.tagStable, height, lastChkp.Sequence)
	if err != nil {
		return xerrors.Errorf("failed to get latest block (lastChkp.Height=%v): %w", height, err)
	}

	blockHash := block.Hash
	var blkLte blockLite
	err = json.Unmarshal(block.Data, &blkLte)
	if err != nil {
		return xerrors.Errorf("failed to unmarshal GetBlock result (blockNumber=%v): %w", blockNumber, err)
	}

	hasTransaction := false
	txHash := ""
	if len(blkLte.Transactions) == 0 {
		t.logger.Info("latest block has no transactions, skipping transaction calls", zap.String("blockNumber", blockNumber))
	} else {
		hasTransaction = true
		txHash = blkLte.Transactions[0].Hash
	}

	t.logger.Info(
		"running polling canary task",
		zap.Uint64("height", height),
		zap.String("blockNumber", blockNumber),
		zap.String("transaction", txHash),
	)

	group, ctx := syncgroup.New(ctx)
	group.Go(func() error {
		contractAddr := sampleCanaryInput[t.network].contractAddress
		if contractAddr == "" {
			return xerrors.Errorf("Unsupported network (network=%v)", t.network.GetName())
		}
		resp, err := t.jsonrpcClientServer.Call(ctx, handler.EthCall, jsonrpc.Params{
			map[string]string{
				"to":   contractAddr,
				"data": "0x70a08231000000000000000000000000f27eee60abacb983251fea941dd7350280a538ba",
			},
			"latest",
		})
		if err != nil {
			return xerrors.Errorf("failed to call EthCall (blockNumber=%v): %w", blockNumber, err)
		}
		if resp.IsNullOrEmpty() {
			return xerrors.Errorf("EthCall response is null or empty (blockNumber=%v)", blockNumber)
		}
		if string(resp.Result) == "0x" {
			return xerrors.Errorf("EthCall result is \"0x\" (blockNumber=%v)", blockNumber)
		}

		return nil
	})

	group.Go(func() error {
		address := sampleCanaryInput[t.network].getBalanceAddress
		if address == "" {
			return xerrors.Errorf("Unsupported network (network=%v)", t.network.GetName())
		}
		resp, err := t.jsonrpcClientServer.Call(ctx, handler.EthGetBalance, jsonrpc.Params{
			address,
			"latest",
		})
		if err != nil {
			return xerrors.Errorf("failed to call EthGetBalance (address=%v): %w", address, err)
		}
		if resp.IsNullOrEmpty() {
			return xerrors.Errorf("EthGetBalance response is null or empty (address=%v)", address)
		}
		var hex hexutil.Big
		if err := json.Unmarshal(resp.Result, &hex); err != nil {
			return xerrors.Errorf("failed to unmarshal EthGetBalance result: %w", err)
		}
		return nil
	})

	group.Go(func() error {
		address := sampleCanaryInput[t.network].getTransactionCountAddress
		if address == "" {
			return xerrors.Errorf("Unsupported network (network=%v)", t.network.GetName())
		}
		resp, err := t.jsonrpcClientServer.Call(ctx, handler.EthGetTransactionCount, jsonrpc.Params{
			address,
			"latest",
		})
		if err != nil {
			return xerrors.Errorf("failed to call EthGetTransactionCount (address=%v): %w", address, err)
		}
		if resp.IsNullOrEmpty() {
			return xerrors.Errorf("EthGetTransactionCount response is null or empty (address=%v)", address)
		}
		var hex hexutil.Uint64
		if err := json.Unmarshal(resp.Result, &hex); err != nil {
			return xerrors.Errorf("failed to unmarshal EthGetTransactionCount result: %w", err)
		}
		if hex == 0 {
			return xerrors.Errorf("EthGetTransactionCount result is \"0x0\" (address=%v)", address)
		}
		return nil
	})

	group.Go(func() error {
		address := sampleCanaryInput[t.network].getCodeAddress
		if address == "" {
			return xerrors.Errorf("Unsupported network (network=%v)", t.network.GetName())
		}
		resp, err := t.jsonrpcClientServer.Call(ctx, handler.EthGetCode, jsonrpc.Params{
			address,
			"latest",
		})
		if err != nil {
			return xerrors.Errorf("failed to call EthGetCode (address=%v): %w", address, err)
		}
		if resp.IsNullOrEmpty() {
			return xerrors.Errorf("EthGetCode response is null or empty (address=%v)", address)
		}
		var hex hexutil.Bytes
		if err := json.Unmarshal(resp.Result, &hex); err != nil {
			return xerrors.Errorf("failed to unmarshal EthGetCode result: %w", err)
		}
		return nil
	})

	group.Go(func() error {
		resp, err := t.jsonrpcClientServer.Call(ctx, handler.EthBlockNumber, nil)
		if err != nil {
			return xerrors.Errorf("failed to call EthBlockNumber (blockNumber=%v): %w", blockNumber, err)
		}
		if resp.IsNullOrEmpty() {
			return xerrors.Errorf("EthBlockNumber response is null or empty (blockNumber=%v)", blockNumber)
		}

		var blockNumberNew hexutil.Uint64
		err = json.Unmarshal(resp.Result, &blockNumberNew)
		if err != nil {
			return xerrors.Errorf("failed to unmarshal EthBlockNumber result (blockNumber=%v, resp=%v): %w", blockNumber, resp, err)
		}
		if uint64(blockNumberNew) < height {
			return xerrors.Errorf("EthBlockNumber response is less than GetCheckpoint (blockNumber=%v, EthBlockNumber=%v): %w", blockNumber, blockNumberNew, err)
		}

		return nil
	})

	// skip if CollectionBlocks is not synchronized
	if t.cfg.IsCollectionSynchronized(xapi.CollectionBlocks) {
		group.Go(func() error {
			resp, err := t.jsonrpcClientServer.Call(ctx, handler.EthGetBlockByNumber, jsonrpc.Params{
				blockNumber,
				true,
			})
			if err != nil {
				return xerrors.Errorf("failed to call EthGetBlockByNumber (blockNumber=%v): %w", blockNumber, err)
			}
			if resp.IsNullOrEmpty() {
				return xerrors.Errorf("EthGetBlockByNumber response is null or empty (blockNumber=%v)", blockNumber)
			}

			var blkVal blockValidation
			err = json.Unmarshal(resp.Result, &blkVal)
			if err != nil {
				return xerrors.Errorf("failed to unmarshal EthGetBlockByNumber result (blockNumber=%v): %w", blockNumber, err)
			}
			if err := t.validate.Struct(&blkVal); err != nil {
				return xerrors.Errorf("failed to validate blkVal from EthGetBlockByNumber: %w", err)
			}

			return nil
		})
	}

	// skip if CollectionBlocksByHash is not synchronized
	if t.cfg.IsCollectionSynchronized(xapi.CollectionBlocksByHash) {
		group.Go(func() error {
			returnFullTxOptions := []bool{true, false}

			for _, option := range returnFullTxOptions {
				resp, err := t.jsonrpcClientServer.Call(ctx, handler.EthGetBlockByHash, jsonrpc.Params{
					blockHash,
					option,
				})
				if err != nil {
					return xerrors.Errorf("failed to call EthGetBlockByHash (blockHash=%v, option=%v): %w", blockHash, option, err)
				}
				if resp.IsNullOrEmpty() {
					return xerrors.Errorf("EthGetBlockByHash response is null or empty (blockHash=%v, option=%v)", blockHash, option)
				}

				var blkVal blockValidation
				err = json.Unmarshal(resp.Result, &blkVal)
				if err != nil {
					return xerrors.Errorf("failed to unmarshal EthGetBlockByHash result (blockHash=%v, option=%v): %w", blockHash, option, err)
				}
				if err := t.validate.Struct(&blkVal); err != nil {
					return xerrors.Errorf("failed to validate blkVal from EthGetBlockByHash: %w", err)
				}
			}

			return nil
		})
	}

	// skip if CollectionBlocks is not synchronized
	if t.cfg.IsCollectionSynchronized(xapi.CollectionBlocks) {
		group.Go(func() error {
			resp, err := t.jsonrpcClientServer.Call(ctx, handler.EthGetBlockTransactionCountByNumber, jsonrpc.Params{
				blockNumber,
			})
			if err != nil {
				return xerrors.Errorf("failed to call EthGetBlockTransactionCountByNumber (blockNumber=%v): %w", blockNumber, err)
			}
			if resp.IsNullOrEmpty() {
				return xerrors.Errorf("EthGetBlockTransactionCountByNumber response is null or empty (blockNumber=%v)", blockNumber)
			}

			var hex hexutil.Uint64
			if err := json.Unmarshal(resp.Result, &hex); err != nil {
				return xerrors.Errorf("failed to unmarshal EthGetBlockTransactionCountByNumber result: %w", err)
			}

			actualNumTransactions := uint64(hex)
			expectedNumTransactions := uint64(len(blkLte.Transactions))
			if actualNumTransactions != expectedNumTransactions {
				return xerrors.Errorf("EthGetBlockTransactionCountByNumber result is (expected=%v) (actual=%v) (blockNumber=%v)", expectedNumTransactions, actualNumTransactions, blockNumber)
			}

			return nil
		})
	}

	// skip if CollectionBlocksByHash is not synchronized
	if t.cfg.IsCollectionSynchronized(xapi.CollectionBlocksByHash) {
		group.Go(func() error {
			resp, err := t.jsonrpcClientServer.Call(ctx, handler.EthGetBlockTransactionCountByHash, jsonrpc.Params{
				blockHash,
			})
			if err != nil {
				return xerrors.Errorf("failed to call EthGetBlockTransactionCountByHash (blockHash=%v): %w", blockHash, err)
			}
			if resp.IsNullOrEmpty() {
				return xerrors.Errorf("EthGetBlockTransactionCountByHash response is null or empty (blockHash=%v)", blockHash)
			}

			var hex hexutil.Uint64
			if err := json.Unmarshal(resp.Result, &hex); err != nil {
				return xerrors.Errorf("failed to unmarshal EthGetBlockTransactionCountByHash result: %w", err)
			}

			actualNumTransactions := uint64(hex)
			expectedNumTransactions := uint64(len(blkLte.Transactions))
			if actualNumTransactions != expectedNumTransactions {
				return xerrors.Errorf("EthGetBlockTransactionCountByHash result is (expected=%v) (actual=%v) (blockHash=%v)", expectedNumTransactions, actualNumTransactions, blockHash)
			}

			return nil
		})
	}

	// skip if CollectionBlocks is not synchronized
	if t.cfg.IsCollectionSynchronized(xapi.CollectionBlocks) {
		group.Go(func() error {
			resp, err := t.jsonrpcClientServer.Call(ctx, handler.EthGetUncleCountByBlockNumber, jsonrpc.Params{
				blockNumber,
			})
			if err != nil {
				return xerrors.Errorf("failed to call EthGetUncleCountByBlockNumber (blockNumber=%v): %w", blockNumber, err)
			}
			if resp.IsNullOrEmpty() {
				return xerrors.Errorf("EthGetUncleCountByBlockNumber response is null or empty (blockNumber=%v)", blockNumber)
			}

			var hex hexutil.Uint64
			if err := json.Unmarshal(resp.Result, &hex); err != nil {
				return xerrors.Errorf("failed to unmarshal EthGetUncleCountByBlockNumber result: %w", err)
			}

			actualNumUncles := uint64(hex)
			expectedNumUncles := uint64(len(blkLte.Uncles))
			if actualNumUncles != expectedNumUncles {
				return xerrors.Errorf("EthGetUncleCountByBlockNumber result is (expected=%v) (actual=%v) (blockNumber=%v)", expectedNumUncles, actualNumUncles, blockNumber)
			}

			return nil
		})
	}

	// skip if CollectionBlocksByHash is not synchronized
	if t.cfg.IsCollectionSynchronized(xapi.CollectionBlocksByHash) {
		group.Go(func() error {
			resp, err := t.jsonrpcClientServer.Call(ctx, handler.EthGetUncleCountByBlockHash, jsonrpc.Params{
				blockHash,
			})
			if err != nil {
				return xerrors.Errorf("failed to call EthGetUncleCountByBlockHash (blockHash=%v): %w", blockHash, err)
			}
			if resp.IsNullOrEmpty() {
				return xerrors.Errorf("EthGetUncleCountByBlockHash response is null or empty (blockHash=%v)", blockHash)
			}

			var hex hexutil.Uint64
			if err := json.Unmarshal(resp.Result, &hex); err != nil {
				return xerrors.Errorf("failed to unmarshal EthGetUncleCountByBlockHash result: %w", err)
			}

			actualNumUncles := uint64(hex)
			expectedNumUncles := uint64(len(blkLte.Uncles))
			if actualNumUncles != expectedNumUncles {
				return xerrors.Errorf("EthGetUncleCountByBlockHash result is (expected=%v) (actual=%v) (blockHash=%v)", expectedNumUncles, actualNumUncles, blockHash)
			}

			return nil
		})
	}

	// skip if CollectionLogs is not synchronized
	if t.cfg.IsCollectionSynchronized(xapi.CollectionLogsV2) {
		group.Go(func() error {
			inputs := []struct {
				mode   string
				params jsonrpc.Params
			}{
				{
					mode:   "block_range",
					params: jsonrpc.Params{map[string]string{"fromBlock": blockNumber, "toBlock": blockNumber}},
				},

				{
					mode:   "block_hash",
					params: jsonrpc.Params{map[string]string{"blockhash": blockHash}},
				},
			}

			for _, input := range inputs {
				resp, err := t.jsonrpcClientServer.Call(ctx, handler.EthGetLogs, input.params)
				if err != nil {
					return xerrors.Errorf("failed to call EthGetLogs (mode=%v) (blockNumber=%v) (blockHash=%v): %w", input.mode, blockNumber, blockHash, err)
				}
				if resp.IsNullOrEmpty() {
					return xerrors.Errorf("EthGetLogs response is null or empty (mode=%v) (blockNumber=%v) (blockHash=%v)", input.mode, blockNumber, blockHash)
				}

				var logVals []logValidation
				err = json.Unmarshal(resp.Result, &logVals)
				if err != nil {
					return xerrors.Errorf("failed to unmarshal EthGetLogs result (mode=%v) (blockNumber=%v) (blockHash=%v): %w", input.mode, blockNumber, blockHash, err)
				}
				for i := range logVals {
					if err = t.validate.Struct(&logVals[i]); err != nil {
						return xerrors.Errorf("failed to validate logVal from EthGetLogs: %w", err)
					}
				}
			}

			return nil
		})
	}

	if hasTransaction && t.cfg.IsCollectionSynchronized(xapi.CollectionTransactions) {
		group.Go(func() error {
			resp, err := t.jsonrpcClientServer.Call(ctx, handler.EthGetTransactionByHash, jsonrpc.Params{
				txHash,
			})
			if err != nil {
				return xerrors.Errorf("failed to call EthGetTransactionByHash (txHash=%v): %w", txHash, err)
			}
			if resp.IsNullOrEmpty() {
				return xerrors.Errorf("EthGetTransactionByHash response is null or empty (txHash=%v)", txHash)
			}
			var txVal transactionValidation
			err = json.Unmarshal(resp.Result, &txVal)
			if err != nil {
				return xerrors.Errorf("failed to unmarshal EthGetTransactionByHash result (txHash=%v): %w", txHash, err)
			}
			if err := t.validate.Struct(&txVal); err != nil {
				return xerrors.Errorf("failed to validate txVal from EthGetTransactionByHash: %w", err)
			}

			return nil
		})
	}

	// skip if CollectionBlocks is not synchronized
	if hasTransaction && t.cfg.IsCollectionSynchronized(xapi.CollectionBlocks) {
		group.Go(func() error {
			randIndex, err := generateRandomTransactionIndex(blkLte)
			if err != nil {
				return xerrors.Errorf("EthGetTransactionByBlockNumberAndIndex: %w", err)
			}

			resp, err := t.jsonrpcClientServer.Call(ctx, handler.EthGetTransactionByBlockNumberAndIndex, jsonrpc.Params{
				blockNumber,
				hexutil.EncodeUint64(randIndex.Uint64()),
			})
			if err != nil {
				return xerrors.Errorf("failed to call EthGetTransactionByBlockNumberAndIndex (blockNumber=%v): %w", blockNumber, err)
			}

			// blocks might not have transactions
			if resp.IsNullOrEmpty() && len(blkLte.Transactions) == 0 {
				return nil
			} else if resp.IsNullOrEmpty() && len(blkLte.Transactions) != 0 {
				return xerrors.Errorf("EthGetTransactionByBlockNumberAndIndex response is null or empty (blockNumber=%v)", blockNumber)
			} else if !resp.IsNullOrEmpty() && len(blkLte.Transactions) == 0 {
				return xerrors.Errorf("Inconsistent result from EthGetTransactionByBlockNumberAndIndex ChainStorage block has no transactions (blockNumber=%v)", blockNumber)
			}

			var txVal transactionLite
			err = json.Unmarshal(resp.Result, &txVal)
			if err != nil {
				return xerrors.Errorf("failed to unmarshal EthGetTransactionByBlockNumberAndIndex result (txHash=%v): %w", txHash, err)
			}

			actualTxHash := txVal.Hash
			expectedTxHash := blkLte.Transactions[randIndex.Int64()].Hash
			if actualTxHash != expectedTxHash {
				return xerrors.Errorf("failed to validate txVal from EthGetTransactionByBlockNumberAndIndex: (expectedTxHash=%v) (actualTxHash=%v) (blockNumber=%v)", expectedTxHash, actualTxHash, blockNumber)
			}

			return nil
		})
	}

	// skip if CollectionBlocksByHash is not synchronized
	if hasTransaction && t.cfg.IsCollectionSynchronized(xapi.CollectionBlocksByHash) {
		group.Go(func() error {
			randIndex, err := generateRandomTransactionIndex(blkLte)
			if err != nil {
				return xerrors.Errorf("EthGetTransactionByBlockHashAndIndex: %w", err)
			}

			resp, err := t.jsonrpcClientServer.Call(ctx, handler.EthGetTransactionByBlockHashAndIndex, jsonrpc.Params{
				blockHash,
				hexutil.EncodeUint64(randIndex.Uint64()),
			})
			if err != nil {
				return xerrors.Errorf("failed to call EthGetTransactionByBlockHashAndIndex (blockHash=%v): %w", blockHash, err)
			}

			// blocks might not have transactions
			if resp.IsNullOrEmpty() && len(blkLte.Transactions) == 0 {
				return nil
			} else if resp.IsNullOrEmpty() && len(blkLte.Transactions) != 0 {
				return xerrors.Errorf("EthGetTransactionByBlockHashAndIndex response is null or empty (blockNumber=%v)", blockNumber)
			} else if !resp.IsNullOrEmpty() && len(blkLte.Transactions) == 0 {
				return xerrors.Errorf("Inconsistent result from EthGetTransactionByBlockHashAndIndex ChainStorage block has no transactions (blockNumber=%v)", blockNumber)
			}

			var txVal transactionLite
			err = json.Unmarshal(resp.Result, &txVal)
			if err != nil {
				return xerrors.Errorf("failed to unmarshal EthGetTransactionByBlockHashAndIndex result (txHash=%v): %w", txHash, err)
			}

			actualTxHash := txVal.Hash
			expectedTxHash := blkLte.Transactions[randIndex.Int64()].Hash
			if actualTxHash != expectedTxHash {
				return xerrors.Errorf("failed to validate txVal from EthGetTransactionByBlockHashAndIndex: (expectedTxHash=%v) (actualTxHash=%v) (blockHash=%v)", expectedTxHash, actualTxHash, blockHash)
			}

			return nil
		})
	}

	if hasTransaction && t.cfg.IsCollectionSynchronized(xapi.CollectionTransactionReceipts) {
		group.Go(func() error {
			resp, err := t.jsonrpcClientServer.Call(ctx, handler.EthGetTransactionReceipt, jsonrpc.Params{
				txHash,
			})
			if err != nil {
				return xerrors.Errorf("failed to call EthGetTransactionReceipt (txHash=%v): %w", txHash, err)
			}
			if resp.IsNullOrEmpty() {
				return xerrors.Errorf("EthGetTransactionReceipt response is null or empty (txHash=%v)", txHash)
			}

			var txRecpVal transactionReceiptValidation
			err = json.Unmarshal(resp.Result, &txRecpVal)
			if err != nil {
				return xerrors.Errorf("failed to unmarshal EthGetTransactionReceipt result (txHash=%v): %w", txHash, err)
			}
			if err := t.validate.Struct(&txRecpVal); err != nil {
				return xerrors.Errorf("failed to validate txRecpVal from EthGetTransactionReceipt: %w", err)
			}

			return nil
		})
	}

	if hasTransaction && t.cfg.IsCollectionSynchronized(xapi.CollectionTracesByHash) {
		group.Go(func() error {
			params := jsonrpc.Params{
				blockHash,
				map[string]string{
					"tracer": ethCallTracer,
				},
			}
			resp, err := t.jsonrpcClientServer.Call(ctx, handler.DebugTraceBlockByHash, params)
			if err != nil {
				return xerrors.Errorf("failed to call DebugTraceBlockByHash (blockHash=%s): %w", blockHash, err)
			}

			if resp.IsNullOrEmpty() {
				return xerrors.Errorf("DebugTraceBlockByHash response is null or empty (blockHash=%v)", blockHash)
			}

			var traceByHashVal []traceResultValidation
			err = json.Unmarshal(resp.Result, &traceByHashVal)
			if err != nil {
				return xerrors.Errorf("failed to unmarshal DebugTraceBlockByHash result (blockHash=%s): %w", blockHash, err)
			}

			for i := range traceByHashVal {
				if err := t.validate.Struct(&traceByHashVal[i]); err != nil {
					return xerrors.Errorf("failed to validate traceByHashVal from DebugTraceBlockByHash (blockHash=%s): %w", blockHash, err)
				}
			}

			return nil
		})
	}

	if hasTransaction && t.cfg.IsCollectionSynchronized(xapi.CollectionTracesByNumber) {
		group.Go(func() error {
			params := jsonrpc.Params{
				blockNumber,
				map[string]string{
					"tracer": ethCallTracer,
				},
			}
			resp, err := t.jsonrpcClientServer.Call(ctx, handler.DebugTraceBlockByNumber, params)
			if err != nil {
				return xerrors.Errorf("failed to call DebugTraceBlockByNumber (blockNumber=%v): %w", blockNumber, err)
			}

			if resp.IsNullOrEmpty() {
				return xerrors.Errorf("DebugTraceBlockByNumber response is null or empty (blockNumber=%v)", blockNumber)
			}

			var traceByNumberVal []traceResultValidation
			err = json.Unmarshal(resp.Result, &traceByNumberVal)
			if err != nil {
				return xerrors.Errorf("failed to unmarshal DebugTraceBlockByNumber result (blockNumber=%v): %w", blockNumber, err)
			}

			for i := range traceByNumberVal {
				if err := t.validate.Struct(&traceByNumberVal[i]); err != nil {
					return xerrors.Errorf("failed to validate traceByNumberVal from DebugTraceBlockByNumber (blockNumber=%s): %w", blockNumber, err)
				}
			}

			return nil
		})
	}

	if t.cfg.IsCollectionSynchronized(xapi.CollectionBlocksExtraDataByNumber) {
		group.Go(func() error {
			params := jsonrpc.Params{
				blockNumber,
			}

			resp, err := t.jsonrpcClientServer.Call(ctx, handler.BorGetAuthor, params)
			if err != nil {
				return xerrors.Errorf("failed to call GetAuthor (blockNumber=%v): %w", blockNumber, err)
			}

			if resp.IsNullOrEmpty() {
				return xerrors.Errorf("GetAuthor response is null or empty (blockNumber=%v)", blockNumber)
			}

			return nil
		})
	}

	for _, cfg := range t.cfg.Controller.ReverseProxy {
		if cfg.Path == "/v1/graphql" {
			group.Go(func() error {
				var response struct {
					Data struct {
						Block struct {
							Number int `validate:"required"`
						}
					}
				}

				url := t.cfg.Chain.Client.ServerAddress + cfg.Path
				if err := t.makeHttpRequest(ctx, url, []byte(graphqlRequest), &response); err != nil {
					return xerrors.Errorf("failed to make graphql request: %w", err)
				}

				t.logger.Info(
					"finished graphql request",
					zap.Int("number", response.Data.Block.Number),
				)
				return nil
			})
		}
	}

	if err := group.Wait(); err != nil {
		return xerrors.Errorf("failed to finish canary task: %w", err)
	}

	return nil
}

func (t *pollingCanaryTask) makeHttpRequest(ctx context.Context, url string, request []byte, response interface{}) error {
	httpRequest, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(request))
	if err != nil {
		return xerrors.Errorf("failed to create request: %w", err)
	}
	httpRequest.Header.Set("Content-Type", "application/json")

	httpResponse, err := t.httpClient.Do(httpRequest)
	if err != nil {
		return xerrors.Errorf("failed to make graphql request: %w", err)
	}
	finalizer := finalizer.WithCloser(httpResponse.Body)
	defer finalizer.Finalize()

	if httpResponse.StatusCode != http.StatusOK {
		return xerrors.Errorf("received http error: %v", httpResponse.StatusCode)
	}

	responseBody, err := ioutil.ReadAll(httpResponse.Body)
	if err != nil {
		return xerrors.Errorf("failed to read response: %w", err)
	}

	if err := json.Unmarshal(responseBody, response); err != nil {
		return xerrors.Errorf("failed to unmarshal response (%v): %w", string(responseBody), err)
	}

	if err := t.validate.Struct(response); err != nil {
		return xerrors.Errorf("invalid response (%v): %w", string(responseBody), err)
	}

	return finalizer.Close()
}

func generateRandomTransactionIndex(lite blockLite) (*big.Int, error) {
	var err error
	randIndex := big.NewInt(0)
	numTransactions := len(lite.Transactions)
	if numTransactions != 0 {
		randIndex, err = rand.Int(rand.Reader, big.NewInt(int64(numTransactions)))
		if err != nil {
			return nil, xerrors.Errorf("failed to generate random transaction index")
		}
	}

	return randIndex, nil
}
