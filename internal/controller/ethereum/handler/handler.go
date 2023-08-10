package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/eth/tracers"
	"github.com/golang/protobuf/proto"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"
	"google.golang.org/grpc/metadata"

	c3common "github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	chainstorageapi "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"

	"github.com/coinbase/chainnode/internal/api"
	"github.com/coinbase/chainnode/internal/api/ethereum"
	"github.com/coinbase/chainnode/internal/clients/blockchain/jsonrpc"
	"github.com/coinbase/chainnode/internal/config"
	"github.com/coinbase/chainnode/internal/controller/internal"
	"github.com/coinbase/chainnode/internal/server/rpc"
	"github.com/coinbase/chainnode/internal/storage"
	ethereumStorage "github.com/coinbase/chainnode/internal/storage/ethereum"
	"github.com/coinbase/chainnode/internal/utils/arbitrumutil"
	"github.com/coinbase/chainnode/internal/utils/constants"
	"github.com/coinbase/chainnode/internal/utils/fxparams"
	"github.com/coinbase/chainnode/internal/utils/log"
	"github.com/coinbase/chainnode/internal/utils/syncgroup"
)

type (
	HandlerParams struct {
		fx.In
		fxparams.Params
		JsonrpcClient                          jsonrpc.Client `name:"proxy"`
		Validator                              Validator
		BlockStorage                           ethereumStorage.BlockStorage
		BlockByHashStorage                     ethereumStorage.BlockByHashStorage
		BlockByHashWithoutFullTxStorageStorage ethereumStorage.BlockByHashWithoutFullTxStorage
		LogStorageV2                           ethereumStorage.LogStorageV2
		TransactionStorage                     ethereumStorage.TransactionStorage
		TransactionReceiptStorage              ethereumStorage.TransactionReceiptStorage
		TraceByHashStorage                     ethereumStorage.TraceByHashStorage
		TraceByNumberStorage                   ethereumStorage.TraceByNumberStorage
		ArbtraceBlockStorage                   ethereumStorage.ArbtraceBlockStorage
		BlockExtraDataByNumberStorage          ethereumStorage.BlockExtraDataByNumberStorage
		Checkpointer                           internal.Checkpointer
	}

	handler struct {
		receiver     Receiver
		checkpointer internal.Checkpointer
		tagStable    uint32
		config       *config.Config
	}

	Receiver interface {
		ProxyReceiver

		// methods from eth namespace
		BlockNumber(ctx context.Context) (json.RawMessage, error)
		GetBlockByHash(ctx context.Context, blockHash common.Hash, returnFullTx bool) (json.RawMessage, error)
		GetBlockByNumber(ctx context.Context, blockNumber rpc.BlockNumber, returnFullTx bool) (json.RawMessage, error)
		GetBlockTransactionCountByHash(ctx context.Context, blockHash common.Hash) (json.RawMessage, error)
		GetBlockTransactionCountByNumber(ctx context.Context, blockNumber rpc.BlockNumber) (json.RawMessage, error)
		GetUncleCountByBlockHash(ctx context.Context, blockHash common.Hash) (json.RawMessage, error)
		GetUncleCountByBlockNumber(ctx context.Context, blockNumber rpc.BlockNumber) (json.RawMessage, error)
		GetLogs(ctx context.Context, criteria FilterCriteria) (json.RawMessage, error)
		GetTransactionByHash(ctx context.Context, txHash common.Hash) (json.RawMessage, error)
		GetTransactionReceipt(ctx context.Context, txHash common.Hash) (json.RawMessage, error)
		GetTransactionByBlockHashAndIndex(ctx context.Context, blockHash common.Hash, index rpc.DecimalOrHex) (json.RawMessage, error)
		GetTransactionByBlockNumberAndIndex(ctx context.Context, blockNumber rpc.BlockNumber, index rpc.DecimalOrHex) (json.RawMessage, error)
		ChainId(ctx context.Context) (json.RawMessage, error)
		Syncing(ctx context.Context) (json.RawMessage, error)

		// methods from debug namespace
		TraceBlockByHash(ctx context.Context, hash common.Hash, traceConfig *tracers.TraceConfig) (json.RawMessage, error)
		TraceBlockByNumber(ctx context.Context, blockNumber rpc.BlockNumber, traceConfig *tracers.TraceConfig) (json.RawMessage, error)

		// methods from net namespace
		Version(ctx context.Context) (json.RawMessage, error)
		Listening(ctx context.Context) (json.RawMessage, error)

		// methods from web3 namespace
		ClientVersion(ctx context.Context) (json.RawMessage, error)

		// methods from arbtrace (Arbitrum only)
		Block(ctx context.Context, blockNumber rpc.BlockNumber) (json.RawMessage, error)

		// methods from bor namespace (Polygon only)
		GetAuthor(ctx context.Context, blockNumber rpc.BlockNumber) (json.RawMessage, error)
	}

	// All public methods will be exposed as RPC endpoints.
	receiver struct {
		jsonrpcClient                   jsonrpc.Client
		blockStorage                    ethereumStorage.BlockStorage
		blockByHashStorage              ethereumStorage.BlockByHashStorage
		blockByHashWithoutFullTxStorage ethereumStorage.BlockByHashWithoutFullTxStorage
		logStorageV2                    ethereumStorage.LogStorageV2
		transactionStorage              ethereumStorage.TransactionStorage
		transactionReceiptStorage       ethereumStorage.TransactionReceiptStorage
		traceByHashStorage              ethereumStorage.TraceByHashStorage
		traceByNumberStorage            ethereumStorage.TraceByNumberStorage
		arbtraceBlockStorage            ethereumStorage.ArbtraceBlockStorage
		blockExtraDataByNumberStorage   ethereumStorage.BlockExtraDataByNumberStorage
		checkpointer                    internal.Checkpointer
		tagStable                       uint32
		config                          *config.Config
		logger                          *zap.Logger
	}

	receiverFn func(ctx context.Context) (json.RawMessage, error)

	requestLite struct {
		Method string `json:"method"`
	}

	transactionLite struct {
		Hash json.RawMessage `json:"hash"`
	}

	logWithAddressAndTopics struct {
		Address string   `json:"address"`
		Topics  []string `json:"topics"`
	}

	blockLiteTransactions struct {
		Transactions []json.RawMessage `json:"transactions"`
	}

	blockLiteUncles struct {
		Uncles []common.Hash `json:"uncles"`
	}
)

const (
	earliestBlockHeight = 0

	getLogsBlockRangeLimit = 1000
	getLogsParallelism     = 200

	errorCodeGeneric      = -32000
	errorCodeNotSupported = -32005
	errorCodeBadRequest   = -32097
	errorCodeCanceled     = -32098
	errorCodeInternal     = -32099
	errorMethodNotFound   = -32601

	ethCallTracer = "callTracer"

	invalidDispatchModeErrString = "unable to dispatch method=%+v: %w"
	legacyBlockErrString         = "legacy block numbers are not supported with debug_"

	// Log every n-th request.
	loggerSamplingRate = 100
)

var (
	_ internal.Handler = (*handler)(nil)
	_ Receiver         = (*receiver)(nil)
)

// All the supported methods are documented here: https://eth.wiki/json-rpc/API
var (
	EthBlockNumber = &jsonrpc.RequestMethod{
		Name:    "eth_blockNumber",
		Timeout: time.Second * 2,
	}

	EthGetBlockByNumber = &jsonrpc.RequestMethod{
		Name:    "eth_getBlockByNumber",
		Timeout: time.Second * 2,
	}

	EthGetLogs = &jsonrpc.RequestMethod{
		Name:    "eth_getLogs",
		Timeout: time.Second * 5,
	}

	EthGetTransactionByHash = &jsonrpc.RequestMethod{
		Name:    "eth_getTransactionByHash",
		Timeout: time.Second * 2,
	}

	EthGetTransactionReceipt = &jsonrpc.RequestMethod{
		Name:    "eth_getTransactionReceipt",
		Timeout: time.Second * 2,
	}

	EthGetTransactionByBlockHashAndIndex = &jsonrpc.RequestMethod{
		Name:    "eth_getTransactionByBlockHashAndIndex",
		Timeout: time.Second * 2,
	}

	EthGetTransactionByBlockNumberAndIndex = &jsonrpc.RequestMethod{
		Name:    "eth_getTransactionByBlockNumberAndIndex",
		Timeout: time.Second * 2,
	}

	EthGetBlockByHash = &jsonrpc.RequestMethod{
		Name:    "eth_getBlockByHash",
		Timeout: time.Second * 2,
	}

	EthGetBlockTransactionCountByHash = &jsonrpc.RequestMethod{
		Name:    "eth_getBlockTransactionCountByHash",
		Timeout: time.Second * 2,
	}

	EthGetBlockTransactionCountByNumber = &jsonrpc.RequestMethod{
		Name:    "eth_getBlockTransactionCountByNumber",
		Timeout: time.Second * 2,
	}

	EthGetUncleCountByBlockHash = &jsonrpc.RequestMethod{
		Name:    "eth_getUncleCountByBlockHash",
		Timeout: time.Second * 2,
	}

	EthGetUncleCountByBlockNumber = &jsonrpc.RequestMethod{
		Name:    "eth_getUncleCountByBlockNumber",
		Timeout: time.Second * 2,
	}

	EthChainId = &jsonrpc.RequestMethod{
		Name:    "eth_chainId",
		Timeout: time.Second,
	}

	EthSyncing = &jsonrpc.RequestMethod{
		Name:    "eth_syncing",
		Timeout: time.Second,
	}

	DebugTraceBlockByHash = &jsonrpc.RequestMethod{
		Name:    "debug_traceBlockByHash",
		Timeout: time.Second * 60,
	}

	// DebugTraceBlockByNumber TODO: increase the timeout if we are calling the node with this method
	DebugTraceBlockByNumber = &jsonrpc.RequestMethod{
		Name:    "debug_traceBlockByNumber",
		Timeout: time.Second * 5,
	}

	NetVersion = &jsonrpc.RequestMethod{
		Name:    "net_version",
		Timeout: time.Second,
	}

	NetListening = &jsonrpc.RequestMethod{
		Name:    "net_listening",
		Timeout: time.Second,
	}

	ArbtraceBlock = &jsonrpc.RequestMethod{
		Name:    "arbtrace_block",
		Timeout: time.Second * 5,
	}

	BorGetAuthor = &jsonrpc.RequestMethod{
		Name:    "bor_getAuthor",
		Timeout: time.Second * 2,
	}
)

func NewHandler(params HandlerParams) internal.Handler {
	scope := params.Metrics
	logger := log.WithPackage(params.Logger)
	checkpointer := params.Checkpointer
	tagStable := params.Config.Tag.Stable
	receiver := Receiver(&receiver{
		jsonrpcClient:                   params.JsonrpcClient,
		blockStorage:                    params.BlockStorage,
		logStorageV2:                    params.LogStorageV2,
		transactionStorage:              params.TransactionStorage,
		transactionReceiptStorage:       params.TransactionReceiptStorage,
		traceByHashStorage:              params.TraceByHashStorage,
		traceByNumberStorage:            params.TraceByNumberStorage,
		arbtraceBlockStorage:            params.ArbtraceBlockStorage,
		blockByHashStorage:              params.BlockByHashStorage,
		blockByHashWithoutFullTxStorage: params.BlockByHashWithoutFullTxStorageStorage,
		blockExtraDataByNumberStorage:   params.BlockExtraDataByNumberStorage,
		checkpointer:                    checkpointer,
		config:                          params.Config,
		tagStable:                       tagStable,
		logger:                          logger,
	})
	receiver = WithErrorInterceptor(receiver)
	receiver = WithInstrumentInterceptor(receiver, scope, logger)
	receiver = params.Validator.WithValidatorInterceptor(receiver)
	return &handler{
		receiver:     receiver,
		checkpointer: checkpointer,
		tagStable:    tagStable,
		config:       params.Config,
	}
}

func (h *handler) Path() string {
	return "/v1"
}

func (h *handler) Receiver() interface{} {
	return h.receiver
}

func (h *handler) Namespaces() map[string]interface{} {
	namespaces := map[string]interface{}{
		NamespaceEth:      NewEthNamespace(h.receiver),
		NamespaceDebug:    NewDebugNamespace(h.receiver),
		NamespaceNet:      NewNetNamespace(h.receiver),
		NamespaceWeb3:     NewWeb3Namespace(h.receiver),
		NamespaceArbtrace: NewArbtraceNamespace(h.receiver),
	}

	if h.config.Blockchain() == c3common.Blockchain_BLOCKCHAIN_POLYGON {
		namespaces[NamespaceBor] = NewBorNamespace(h.receiver)
	}
	return namespaces
}

func (h *handler) PrepareContext(ctx context.Context, request json.RawMessage) (context.Context, error) {
	if getDispatchMode(ctx) == constants.InvalidMode {
		return nil, api.NewServerError(http.StatusBadRequest, api.ErrInvalidHttpHeaderValue)
	}

	// Cache the checkpoint for validation purposes, so that we don't need to overload the DDB to query the latest checkpoint.
	shouldCacheCheckpoint := false
	if !isBatch(request) {
		var requestLite requestLite
		if err := json.Unmarshal(request, &requestLite); err != nil {
			return nil, api.NewServerError(http.StatusBadRequest, xerrors.Errorf("failed to unmarshal request: %w", err))
		}

		if !proxyMethods[requestLite.Method] {
			shouldCacheCheckpoint = true
		}
	} else {
		var batchRequests []requestLite
		if err := json.Unmarshal(request, &batchRequests); err != nil {
			return nil, api.NewServerError(http.StatusBadRequest, xerrors.Errorf("failed to unmarshal batch requests: %w", err))
		}

		// Cache the checkpoint if any of the requests is not a proxy method.
		for _, r := range batchRequests {
			if !proxyMethods[r.Method] {
				shouldCacheCheckpoint = true
				break
			}
		}
	}

	if !shouldCacheCheckpoint {
		return ctx, nil
	}

	return h.cacheCheckpoint(ctx)
}

func (h *handler) cacheCheckpoint(ctx context.Context) (context.Context, error) {
	// Cache the checkpoint in the context to improve throughput.
	latestCheckpoint, err := h.checkpointer.Get(ctx, api.CollectionLatestCheckpoint, h.tagStable)
	if err != nil {
		return nil, h.mapError(xerrors.Errorf("failed to get latest checkpoint: %w", err))
	}

	ctx = context.WithValue(ctx, constants.ContextKeyLatestCheckpoint, latestCheckpoint)
	return ctx, nil
}

func (h *handler) mapError(err error) error {
	if err == nil {
		return nil
	}

	if isCanceledError(err) {
		return api.NewServerError(api.StatusCanceled, err)
	}

	return api.NewServerError(http.StatusInternalServerError, err)
}

// BlockNumber implements https://eth.wiki/json-rpc/API#eth_blocknumber
func (r *receiver) BlockNumber(ctx context.Context) (json.RawMessage, error) {
	lastChkp, err := r.getLatestCheckpoint(ctx)
	if err != nil {
		return nil, xerrors.Errorf("failed to get latest checkpoint: %w", err)
	}

	result, err := json.Marshal(hexutil.EncodeUint64(lastChkp.Height))
	if err != nil {
		return nil, xerrors.Errorf("failed to marshal lastChkp.Height: %w", err)
	}

	return result, nil
}

// GetBlockByHash implements https://eth.wiki/json-rpc/API#eth_getblockbyhash
func (r *receiver) GetBlockByHash(ctx context.Context, blockHash common.Hash, returnFullTx bool) (json.RawMessage, error) {
	lastChkp, err := r.getLatestCheckpoint(ctx)
	if err != nil {
		return nil, xerrors.Errorf("failed to get latest checkpoint: %w", err)
	}

	var block *ethereum.Block
	if returnFullTx {
		block, err = r.blockByHashStorage.GetBlockByHash(ctx, r.tagStable, blockHash.String(), lastChkp.Sequence)
	} else {
		block, err = r.blockByHashWithoutFullTxStorage.GetBlockByHashWithoutFullTx(ctx, r.tagStable, blockHash.String(), lastChkp.Sequence)
	}

	if err != nil {
		if xerrors.Is(err, storage.ErrItemNotFound) {
			// align with geth output: return null for block not found
			return nil, nil
		}
		return nil, xerrors.Errorf("failed to get block by hash: %w", err)
	}

	return block.Data, nil
}

// GetBlockByNumber implements https://eth.wiki/json-rpc/API#eth_getblockbynumber
func (r *receiver) GetBlockByNumber(ctx context.Context, blockNumber rpc.BlockNumber, returnFullTx bool) (json.RawMessage, error) {
	shouldProxy, err := shouldProxyMethod(blockNumber)
	if err != nil {
		return nil, err
	} else if shouldProxy {
		return r.proxy(ctx, EthGetBlockByNumber, jsonrpc.Params{blockNumber, returnFullTx})
	}

	lastChkp, err := r.getLatestCheckpoint(ctx)
	if err != nil {
		return nil, xerrors.Errorf("failed to get latest checkpoint: %w", err)
	}

	if blockNumber == rpc.LatestBlockNumber {
		blockNumber = rpc.BlockNumber(lastChkp.Height)
	}

	block, err := r.blockStorage.GetBlock(ctx, r.tagStable, uint64(blockNumber), lastChkp.Sequence)
	if err != nil {
		if xerrors.Is(err, storage.ErrItemNotFound) {
			// Align with geth output: return null for block not found
			return nil, nil
		}
		return nil, xerrors.Errorf("failed to get block: %w", err)
	}

	if returnFullTx {
		return block.Data, nil
	}

	var blockData map[string]json.RawMessage
	err = json.Unmarshal(block.Data, &blockData)
	if err != nil {
		return nil, xerrors.Errorf("failed to unmarshal block.data: %w", err)
	}

	if _, ok := blockData["transactions"]; !ok || len(blockData["transactions"]) == 0 {
		return block.Data, nil
	}

	var txs []transactionLite
	if err = json.Unmarshal(blockData["transactions"], &txs); err != nil {
		return nil, xerrors.Errorf("failed to unmarshal blockData[\"transactions\"]: %w", err)
	}

	txHashes := make([]json.RawMessage, len(txs))
	for i, tx := range txs {
		txHashes[i] = tx.Hash
	}

	txHashesMarshal, err := json.Marshal(txHashes)
	if err != nil {
		return nil, xerrors.Errorf("failed to marshal txHashes: %w", err)
	}
	blockData["transactions"] = txHashesMarshal

	result, err := json.Marshal(blockData)
	if err != nil {
		return nil, xerrors.Errorf("failed to marshal blockData: %w", err)
	}

	return result, nil
}

// GetBlockTransactionCountByHash implements https://eth.wiki/json-rpc/API#eth_getblocktransactioncountbyhash
func (r *receiver) GetBlockTransactionCountByHash(ctx context.Context, blockHash common.Hash) (json.RawMessage, error) {
	lastChkp, err := r.getLatestCheckpoint(ctx)
	if err != nil {
		return nil, xerrors.Errorf("failed to get latest checkpoint: %w", err)
	}

	block, err := r.blockByHashWithoutFullTxStorage.GetBlockByHashWithoutFullTx(ctx, r.tagStable, blockHash.String(), lastChkp.Sequence)

	if err != nil {
		if xerrors.Is(err, storage.ErrItemNotFound) {
			// align with geth output: return null for block not found
			return nil, nil
		}
		return nil, xerrors.Errorf("failed to get block by hash: %w", err)
	}

	return getBlockTransactionCount(block)
}

// GetBlockTransactionCountByNumber implements https://eth.wiki/json-rpc/API#eth_getblocktransactioncountbynumber
func (r *receiver) GetBlockTransactionCountByNumber(ctx context.Context, blockNumber rpc.BlockNumber) (json.RawMessage, error) {
	shouldProxy, err := shouldProxyMethod(blockNumber)
	if err != nil {
		return nil, err
	} else if shouldProxy {
		return r.proxy(ctx, EthGetBlockTransactionCountByNumber, jsonrpc.Params{blockNumber})
	}

	lastChkp, err := r.getLatestCheckpoint(ctx)
	if err != nil {
		return nil, xerrors.Errorf("failed to get latest checkpoint: %w", err)
	}

	if blockNumber == rpc.LatestBlockNumber {
		blockNumber = rpc.BlockNumber(lastChkp.Height)
	}

	block, err := r.blockStorage.GetBlock(ctx, r.tagStable, uint64(blockNumber), lastChkp.Sequence)
	if err != nil {
		if xerrors.Is(err, storage.ErrItemNotFound) {
			// Align with geth output: return null for block not found
			return nil, nil
		}
		return nil, xerrors.Errorf("failed to get block: %w", err)
	}

	return getBlockTransactionCount(block)
}

// GetUncleCountByBlockHash implements https://eth.wiki/json-rpc/API#eth_getunclecountbyblockhash
func (r *receiver) GetUncleCountByBlockHash(ctx context.Context, blockHash common.Hash) (json.RawMessage, error) {
	lastChkp, err := r.getLatestCheckpoint(ctx)
	if err != nil {
		return nil, xerrors.Errorf("failed to get latest checkpoint: %w", err)
	}

	block, err := r.blockByHashWithoutFullTxStorage.GetBlockByHashWithoutFullTx(ctx, r.tagStable, blockHash.String(), lastChkp.Sequence)

	if err != nil {
		if xerrors.Is(err, storage.ErrItemNotFound) {
			// align with geth output: return null for block not found
			return nil, nil
		}
		return nil, xerrors.Errorf("failed to get block by hash: %w", err)
	}

	return getBlockUncleCount(block)
}

// GetUncleCountByBlockNumber implements https://eth.wiki/json-rpc/API#eth_getunclecountbyblocknumber
func (r *receiver) GetUncleCountByBlockNumber(ctx context.Context, blockNumber rpc.BlockNumber) (json.RawMessage, error) {
	shouldProxy, err := shouldProxyMethod(blockNumber)
	if err != nil {
		return nil, err
	} else if shouldProxy {
		return r.proxy(ctx, EthGetUncleCountByBlockNumber, jsonrpc.Params{blockNumber})
	}

	lastChkp, err := r.getLatestCheckpoint(ctx)
	if err != nil {
		return nil, xerrors.Errorf("failed to get latest checkpoint: %w", err)
	}

	if blockNumber == rpc.LatestBlockNumber {
		blockNumber = rpc.BlockNumber(lastChkp.Height)
	}

	block, err := r.blockStorage.GetBlock(ctx, r.tagStable, uint64(blockNumber), lastChkp.Sequence)
	if err != nil {
		if xerrors.Is(err, storage.ErrItemNotFound) {
			// Align with geth output: return null for block not found
			return nil, nil
		}
		return nil, xerrors.Errorf("failed to get block: %w", err)
	}

	return getBlockUncleCount(block)
}

// GetLogs implements https://eth.wiki/json-rpc/API#eth_getlogs
func (r *receiver) GetLogs(ctx context.Context, crit FilterCriteria) (json.RawMessage, error) {
	if r.config.Network() == c3common.Network_NETWORK_ETHEREUM_MAINNET {
		// block abnormal traffic from this address
		for _, addr := range crit.Addresses {
			if addr == common.HexToAddress("0x6639cdb3ea7a48b0ad95b47bec78023c6f706160") {
				return []byte("[]"), nil
			}
		}
	}

	lastChkp, err := r.getLatestCheckpoint(ctx)
	if err != nil {
		return nil, xerrors.Errorf("failed to get latest checkpoint: %w", err)
	}

	begin := lastChkp.Height
	end := lastChkp.Height
	if crit.BlockHash != nil {
		block, err := r.blockByHashWithoutFullTxStorage.GetBlockByHashWithoutFullTx(ctx, r.tagStable, crit.BlockHash.String(), lastChkp.Sequence)
		if err != nil {
			if xerrors.Is(err, storage.ErrItemNotFound) {
				return nil, jsonrpc.NewRPCError(errorCodeBadRequest, "unknown block").WithCause(err)
			}
			return nil, xerrors.Errorf("failed to get block by hash: %w", err)
		}
		begin = block.Height
		end = block.Height
	}
	if crit.FromBlock != nil {
		begin, err = r.parseBlockNumber(crit.FromBlock.Int64(), lastChkp.Height)
		if err != nil {
			return nil, xerrors.Errorf("failed to parse FromBlock: %w", err)
		}
	}
	if crit.ToBlock != nil {
		end, err = r.parseBlockNumber(crit.ToBlock.Int64(), lastChkp.Height)
		if err != nil {
			return nil, xerrors.Errorf("failed to parse ToBlock: %w", err)
		}
	}

	// make sure we are not querying the blocks beyond global checkpoint
	if end > lastChkp.Height {
		end = lastChkp.Height
	}

	if end < begin {
		return []byte("[]"), nil
	}
	if end-begin+1 > getLogsBlockRangeLimit {
		return nil, jsonrpc.NewRPCError(errorCodeNotSupported, fmt.Sprintf("please limit the query to at most %v blocks", getLogsBlockRangeLimit))
	}

	// if addresses or topics are not empty, need to fetch bloom in order to rule out irrelevant logs
	bloomFilterRequired := len(crit.Addresses) != 0 || len(crit.Topics) != 0
	logsLite := make([]*ethereum.LogsLite, end-begin+1)
	if bloomFilterRequired {
		logsLite, err = r.logStorageV2.GetLogsLiteByBlockRange(ctx, r.tagStable, begin, end+1, lastChkp.Sequence)
		if err != nil {
			if xerrors.Is(err, storage.ErrItemNotFound) {
				return []byte("[]"), nil
			}
			return nil, xerrors.Errorf("failed to get logsLite: %w", err)
		}

		if len(logsLite) != int(end-begin+1) {
			return nil, xerrors.Errorf("number_of_logsLite != number_of_logs (%d != %d)", len(logsLite), end-begin+1)
		}
	}

	// For each block, get logs and apply filters, and append to logs
	group, ctx := syncgroup.New(ctx, syncgroup.WithThrottling(getLogsParallelism))
	logsAll := make([][]json.RawMessage, end-begin+1)
	for blockNum := begin; blockNum <= end; blockNum++ {
		currBlockNum := blockNum
		group.Go(func() error {
			i := currBlockNum - begin
			if bloomFilterRequired {
				included, err := r.bloomFilterLogs(logsLite[i], crit, currBlockNum)
				if err != nil {
					return xerrors.Errorf("failed to apply bloom filter: %w", err)
				}

				if !included {
					return nil
				}
			}

			logs, err := r.logStorageV2.GetLogsV2(ctx, r.tagStable, currBlockNum, lastChkp.Sequence)
			if err != nil {
				if xerrors.Is(err, storage.ErrItemNotFound) {
					return nil
				}
				return xerrors.Errorf("failed to get logs: %w", err)
			}

			// liteLogs is used for applying filters, kept logs are taken from fullLogs
			var fullLogs []json.RawMessage
			err = json.Unmarshal(logs.Data, &fullLogs)
			if err != nil {
				return xerrors.Errorf("failed to unmarshal logs.Data to fullLogs: %w", err)
			}
			var liteLogs []logWithAddressAndTopics
			err = json.Unmarshal(logs.Data, &liteLogs)
			if err != nil {
				return xerrors.Errorf("failed to unmarshal logs.Data to liteLogs: %w", err)
			}

			logsKept, err := r.applyFilters(fullLogs, liteLogs, crit)
			if err != nil {
				return xerrors.Errorf("failed to apply filters: %w", err)
			}

			logsAll[i] = logsKept
			return nil
		})
	}

	if err := group.Wait(); err != nil {
		return nil, xerrors.Errorf("error in goroutine: %w", err)
	}

	logsFlat := make([]json.RawMessage, 0)
	for _, logs := range logsAll {
		logsFlat = append(logsFlat, logs...)
	}

	result, err := json.Marshal(logsFlat)
	if err != nil {
		return nil, xerrors.Errorf("failed to marshal logs: %w", err)
	}

	return result, nil
}

// bloomFilterLogs applies the bloom filter and returns
// true if the block is possibly in the storage;
// or false if the block is definitely not in the storage.
func (r *receiver) bloomFilterLogs(logsLite *ethereum.LogsLite, crit FilterCriteria, blockNum uint64) (bool, error) {
	if logsLite == nil {
		// logsLite is queried from GSI, which is eventual consistent only.
		// Therefore, we may run into the case where logsLite is not yet available in the GSI for the recent blocks
		// In such case, we can safely return true to indicate it is possible to include such block for further processing
		r.logger.Warn("logsLite was not found for block", zap.Uint64("height", blockNum))
		return true, nil
	}

	bloom := logsLite.LogsBloom
	var filter types.Bloom
	if err := filter.UnmarshalText([]byte(bloom)); err != nil {
		return false, xerrors.Errorf("failed to unmarshal bloom filter from logs: %w", err)
	}

	return bloomFilter(filter, crit.Addresses, crit.Topics), nil
}

// applyFilters applies Addresses and Topics filters on logs
// Trying to match filterLogs: https://github.com/ethereum/go-ethereum/blob/0cb4d65f8d4f2503c23abe7867d52309d352c7fa/eth/filters/filter.go#L286
func (r *receiver) applyFilters(fullLogs []json.RawMessage, liteLogs []logWithAddressAndTopics, crit FilterCriteria) ([]json.RawMessage, error) {
	keptLogs := make([]json.RawMessage, 0)
Logs:
	for i, liteLog := range liteLogs {
		// Apply Addresses filters
		keep := false
		if len(crit.Addresses) == 0 {
			keep = true
		} else if len(liteLog.Address) > 0 {
			logAddr := common.HexToAddress(liteLog.Address)
			for _, addr := range crit.Addresses {
				if logAddr == addr {
					keep = true
				}
			}
		}
		if !keep {
			continue
		}

		// Apply Topics Filters
		if len(crit.Topics) > len(liteLog.Topics) {
			continue
		}
		for i, sub := range crit.Topics {
			keep = false
			if len(sub) == 0 {
				keep = true
			} else {
				logTopic := common.HexToHash(liteLog.Topics[i])
				for _, topic := range sub {
					if logTopic == topic {
						keep = true
					}
				}
			}

			if !keep {
				continue Logs
			}
		}

		keptLogs = append(keptLogs, fullLogs[i])
	}

	return keptLogs, nil
}

// GetTransactionByHash implements https://eth.wiki/json-rpc/API#eth_getTransactionByHash
func (r *receiver) GetTransactionByHash(ctx context.Context, txHash common.Hash) (json.RawMessage, error) {
	lastChkp, err := r.getLatestCheckpoint(ctx)
	if err != nil {
		return nil, xerrors.Errorf("failed to get latest checkpoint: %w", err)
	}

	tx, err := r.transactionStorage.GetTransactionByHash(ctx, r.tagStable, txHash.String(), lastChkp.Sequence)
	if err != nil {
		if xerrors.Is(err, storage.ErrItemNotFound) {
			return nil, nil
		}
		return nil, xerrors.Errorf("failed to get transaction by hash: %w", err)
	}

	result := tx.Data
	return result, nil
}

// GetTransactionReceipt implements https://eth.wiki/json-rpc/API#eth_getTransactionReceipt
func (r *receiver) GetTransactionReceipt(ctx context.Context, txHash common.Hash) (json.RawMessage, error) {
	lastChkp, err := r.getLatestCheckpoint(ctx)
	if err != nil {
		return nil, xerrors.Errorf("failed to get latest checkpoint: %w", err)
	}

	tx, err := r.transactionReceiptStorage.GetTransactionReceipt(ctx, r.tagStable, txHash.String(), lastChkp.Sequence)
	if err != nil {
		if xerrors.Is(err, storage.ErrItemNotFound) {
			return nil, nil
		}
		return nil, xerrors.Errorf("failed to get transaction by hash: %w", err)
	}

	result := tx.Data
	return result, nil
}

// GetTransactionByBlockHashAndIndex implements https://eth.wiki/json-rpc/API#eth_gettransactionbyblockhashandindex
func (r *receiver) GetTransactionByBlockHashAndIndex(ctx context.Context, blockHash common.Hash, index rpc.DecimalOrHex) (json.RawMessage, error) {
	lastChkp, err := r.getLatestCheckpoint(ctx)
	if err != nil {
		return nil, xerrors.Errorf("failed to get latest checkpoint: %w", err)
	}

	block, err := r.blockByHashStorage.GetBlockByHash(ctx, r.tagStable, blockHash.String(), lastChkp.Sequence)
	if err != nil {
		if xerrors.Is(err, storage.ErrItemNotFound) {
			// align with geth output: return null for block not found
			return nil, nil
		}
		return nil, xerrors.Errorf("failed to get block by hash: %w", err)
	}

	return getBlockTransactionByIndex(block, index)
}

// GetTransactionByBlockNumberAndIndex implements https://eth.wiki/json-rpc/API#eth_gettransactionbyblocknumberandindex
func (r *receiver) GetTransactionByBlockNumberAndIndex(ctx context.Context, blockNumber rpc.BlockNumber, index rpc.DecimalOrHex) (json.RawMessage, error) {
	shouldProxy, err := shouldProxyMethod(blockNumber)
	if err != nil {
		return nil, err
	} else if shouldProxy {
		return r.proxy(ctx, EthGetTransactionByBlockNumberAndIndex, jsonrpc.Params{blockNumber, hexutil.EncodeUint64(uint64(index))})
	}

	lastChkp, err := r.getLatestCheckpoint(ctx)
	if err != nil {
		return nil, xerrors.Errorf("failed to get latest checkpoint: %w", err)
	}

	if blockNumber == rpc.LatestBlockNumber {
		blockNumber = rpc.BlockNumber(lastChkp.Height)
	}

	block, err := r.blockStorage.GetBlock(ctx, r.tagStable, uint64(blockNumber), lastChkp.Sequence)
	if err != nil {
		if xerrors.Is(err, storage.ErrItemNotFound) {
			// Align with geth output: return null for block not found
			return nil, nil
		}
		return nil, xerrors.Errorf("failed to get block: %w", err)
	}

	return getBlockTransactionByIndex(block, index)
}

// TraceBlockByHash implements https://github.com/ethereum/go-ethereum/blob/fb3a081c7e534248437595d42b085a1a7221202b/eth/tracers/api.go#L435
func (r *receiver) TraceBlockByHash(ctx context.Context, hash common.Hash, traceConfig *tracers.TraceConfig) (json.RawMessage, error) {
	// TODO: return a special error for genesis block
	err := r.validateTraceConfig(traceConfig)
	if err != nil {
		return nil, xerrors.Errorf("failed to validate traceConfig: %w", err)
	}

	lastChkp, err := r.getLatestCheckpoint(ctx)
	if err != nil {
		return nil, xerrors.Errorf("failed to get latest checkpoint: %w", err)
	}

	trace, err := r.traceByHashStorage.GetTraceByHash(ctx, r.tagStable, hash.String(), lastChkp.Sequence)
	if err != nil {
		if xerrors.Is(err, storage.ErrItemNotFound) {
			if err := r.processTraceNotFound(ctx, hash.String(), lastChkp.Sequence); err != nil {
				return nil, err
			}
			return nil, jsonrpc.NewRPCError(errorCodeBadRequest, fmt.Sprintf("block %s not found", hash)).WithCause(err)
		}
		return nil, xerrors.Errorf("failed to get trace by block hash: %w", err)
	}

	result := trace.Data
	return result, nil
}

// TraceBlockByNumber implements https://github.com/ethereum/go-ethereum/blob/fb3a081c7e534248437595d42b085a1a7221202b/eth/tracers/api.go#L445
func (r *receiver) TraceBlockByNumber(ctx context.Context, blockNumber rpc.BlockNumber, traceConfig *tracers.TraceConfig) (json.RawMessage, error) {
	// TODO: add support for tagged blockNumber ("pending")
	// rpc.BlockNumber unmarshals tagged blocknumber as 0 ("earliest"), -1 ("latest"), and -2 ("pending")
	if blockNumber < 0 && blockNumber != rpc.LatestBlockNumber {
		return nil, xerrors.Errorf("tagged blockNumber=%+v is not implemented: %w", blockNumber, api.ErrNotImplemented)
	}

	if blockNumber == 0 {
		return nil, xerrors.New("genesis is not traceable")
	}

	err := r.validateTraceConfig(traceConfig)
	if err != nil {
		return nil, xerrors.Errorf("failed to validate traceConfig: %w", err)
	}

	lastChkp, err := r.getLatestCheckpoint(ctx)
	if err != nil {
		return nil, xerrors.Errorf("failed to get latest checkpoint: %w", err)
	}

	if blockNumber == rpc.LatestBlockNumber {
		blockNumber = rpc.BlockNumber(lastChkp.Height)
	}

	if arbitrumutil.IsArbitrumAndBeforeNITROUpgrade(r.config.Blockchain(), r.config.Network(), uint64(blockNumber)) {
		return nil, jsonrpc.NewRPCError(errorCodeGeneric, legacyBlockErrString)
	}

	trace, err := r.traceByNumberStorage.GetTraceByNumber(ctx, r.tagStable, uint64(blockNumber), lastChkp.Sequence)
	if err != nil {
		if xerrors.Is(err, storage.ErrItemNotFound) {
			return nil, jsonrpc.NewRPCError(errorCodeBadRequest, fmt.Sprintf("block %v not found", blockNumber)).WithCause(err)
		}
		return nil, xerrors.Errorf("failed to get trace by block number: %w", err)
	}

	result := trace.Data
	return result, nil
}

func (r *receiver) Block(ctx context.Context, blockNumber rpc.BlockNumber) (json.RawMessage, error) {
	if !arbitrumutil.IsArbitrum(r.config.Blockchain(), r.config.Network()) {
		return nil, jsonrpc.NewRPCError(errorMethodNotFound, "the method arbtrace_block does not exist/is not available")
	}

	// rpc.BlockNumber unmarshals tagged blocknumber as 0 ("earliest"), -1 ("latest"), and -2 ("pending")
	if blockNumber < 0 && blockNumber != rpc.LatestBlockNumber {
		return nil, xerrors.Errorf("tagged blockNumber=%+v is not implemented: %w", blockNumber, api.ErrNotImplemented)
	}

	lastChkp, err := r.getLatestCheckpoint(ctx)
	if err != nil {
		return nil, xerrors.Errorf("failed to get latest checkpoint: %w", err)
	}

	if blockNumber == rpc.LatestBlockNumber {
		blockNumber = rpc.BlockNumber(lastChkp.Height)
	}

	if !arbitrumutil.IsBeforeNITROUpgrade(uint64(blockNumber)) {
		return []byte("[]"), nil
	}

	trace, err := r.arbtraceBlockStorage.GetArbtraceBlock(ctx, r.tagStable, uint64(blockNumber), lastChkp.Sequence)
	if err != nil {
		if xerrors.Is(err, storage.ErrItemNotFound) {
			return []byte("[]"), nil
		}
		return nil, xerrors.Errorf("failed to get arbtrace by block number: %w", err)
	}

	result := trace.Data
	return result, nil
}

// ChainId implements https://docs.alchemy.com/alchemy/apis/ethereum/eth-chainid
func (r *receiver) ChainId(ctx context.Context) (json.RawMessage, error) {
	chainID, err := getChainID(r.config.Network())
	if err != nil {
		return nil, xerrors.Errorf("failed to handle chainId: %w", err)
	}

	result, err := json.Marshal(hexutil.Uint(chainID))
	if err != nil {
		return nil, xerrors.Errorf("failed to marshal chain id %v: %w", chainID, err)
	}

	return result, nil
}

// Version implements https://eth.wiki/json-rpc/API#net_version
func (r *receiver) Version(ctx context.Context) (json.RawMessage, error) {
	chainID, err := getChainID(r.config.Network())
	if err != nil {
		return nil, xerrors.Errorf("failed to handle version: %w", err)
	}

	result, err := json.Marshal(strconv.Itoa(chainID))
	if err != nil {
		return nil, xerrors.Errorf("failed to marshal chain id %v: %w", chainID, err)
	}

	return result, nil
}

// Syncing implements https://ethereum.org/en/developers/docs/apis/json-rpc/#eth_syncing
func (r *receiver) Syncing(ctx context.Context) (json.RawMessage, error) {
	// given that as a "node" ChainNode is not syncing with other peer, always return false.
	// reference: https://github.com/ethereum/go-ethereum/blob/89b138cf2fe5c988eea8f2e74d042fc092849f8e/internal/ethapi/api.go#L116-L122
	result, err := json.Marshal(false)
	if err != nil {
		return nil, xerrors.Errorf("failed to marshal for syncing: %w", err)
	}

	return result, nil
}

// Listening implements https://ethereum.org/en/developers/docs/apis/json-rpc/#net_listening
func (r *receiver) Listening(ctx context.Context) (json.RawMessage, error) {
	// Always listening for network connections.
	// reference: https://github.com/ethereum/go-ethereum/blob/89b138cf2fe5c988eea8f2e74d042fc092849f8e/internal/ethapi/api.go#L1999-L2002
	result, err := json.Marshal(true)
	if err != nil {
		return nil, xerrors.Errorf("failed to marshal for listening: %w", err)
	}

	return result, nil
}

// GetAuthor implements https://www.quicknode.com/docs/polygon/bor_getAuthor
func (r *receiver) GetAuthor(ctx context.Context, blockNumber rpc.BlockNumber) (json.RawMessage, error) {
	lastChkp, err := r.getLatestCheckpoint(ctx)
	if err != nil {
		return nil, xerrors.Errorf("failed to get latest checkpoint: %w", err)
	}

	if blockNumber == rpc.LatestBlockNumber {
		blockNumber = rpc.BlockNumber(lastChkp.Height)
	}

	data, err := r.blockExtraDataByNumberStorage.GetBlockExtraDataByNumber(ctx, r.tagStable, uint64(blockNumber), lastChkp.Sequence)
	if err != nil {
		if xerrors.Is(err, storage.ErrItemNotFound) {
			return nil, jsonrpc.NewRPCError(errorCodeGeneric, "unknown block")
		}
		return nil, xerrors.Errorf("failed to get block: %w", err)
	}

	if len(data.Data) == 0 {
		return nil, jsonrpc.NewRPCError(errorCodeGeneric, "unknown author")
	}

	var extraData chainstorageapi.PolygonExtraData
	err = proto.Unmarshal(data.Data, &extraData)
	if err != nil {
		return nil, xerrors.Errorf("failed to unmarshal extra data: %w", err)
	}

	return extraData.Author, nil
}

func (r *receiver) processTraceNotFound(ctx context.Context, hash string, lastChkp api.Sequence) error {
	if arbitrumutil.IsArbitrum(r.config.Blockchain(), r.config.Network()) {
		// If trace is not found and the current config is Arbitrum, then it's likely that the block height is before NITRO upgrade
		block, err := r.blockByHashStorage.GetBlockByHash(ctx, r.tagStable, hash, lastChkp)
		if err != nil {
			if xerrors.Is(err, storage.ErrItemNotFound) {
				return jsonrpc.NewRPCError(errorCodeBadRequest, fmt.Sprintf("block %s not found", hash)).WithCause(err)
			}
			return xerrors.Errorf("failed to get trace by block hash: %w", err)
		}
		if arbitrumutil.IsBeforeNITROUpgrade(block.Height) {
			return jsonrpc.NewRPCError(errorCodeGeneric, legacyBlockErrString)
		}
	}
	return nil
}

func (r *receiver) proxy(ctx context.Context, method *jsonrpc.RequestMethod, params jsonrpc.Params) (json.RawMessage, error) {
	if getDispatchMode(ctx) == constants.NativeOnlyMode {
		return nil, xerrors.Errorf(invalidDispatchModeErrString, method.Name, api.ErrNotAllowed)
	}

	resp, err := r.jsonrpcClient.Call(ctx, method, params)
	if err != nil {
		return nil, xerrors.Errorf("failed to make proxy request %+v: %w", method, err)
	}

	return resp.Result, nil
}

func (r *receiver) getLatestCheckpoint(ctx context.Context) (*api.Checkpoint, error) {
	checkpoint, ok := ctx.Value(constants.ContextKeyLatestCheckpoint).(*api.Checkpoint)
	if ok {
		r.logger.Debug("using cached checkpoint", zap.Reflect("checkpoint", checkpoint))
		return checkpoint, nil
	}

	return r.checkpointer.Get(ctx, api.CollectionLatestCheckpoint, r.tagStable)
}

func (r *receiver) parseBlockNumber(blkNum int64, latestBlkNum uint64) (uint64, error) {
	switch blkNum {
	case rpc.PendingBlockNumber.Int64():
		return 0, xerrors.Errorf("blocknumber: \"pending\" is not implemented: %w", api.ErrNotImplemented)
	case rpc.LatestBlockNumber.Int64():
		return latestBlkNum, nil
	case rpc.EarliestBlockNumber.Int64():
		return earliestBlockHeight, nil
	default:
		return uint64(blkNum), nil
	}
}

func (r *receiver) validateTraceConfig(traceConfig *tracers.TraceConfig) error {
	if traceConfig == nil || traceConfig.Tracer == nil {
		return xerrors.Errorf("tracer is empty, note: ChainNode only accepts `callTracer`: %w", api.ErrNotImplemented)
	}

	if tracer := *traceConfig.Tracer; tracer != ethCallTracer {
		if r.config.Blockchain() == c3common.Blockchain_BLOCKCHAIN_POLYGON {
			r.logger.Warn("invalid tracer", zap.String("tracer", tracer))
		} else {
			return xerrors.Errorf("only accepts `callTracer` as the tracer: %w", api.ErrNotImplemented)
		}
	}

	if traceConfig.TracerConfig != nil {
		return xerrors.Errorf("tracerConfig is not supported: %w", api.ErrNotImplemented)
	}

	return nil
}

func getDispatchMode(ctx context.Context) constants.DispatchMode {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return constants.DynamicMode
	}

	routingModes := md.Get(constants.RoutingModeHttpHeaderName)
	if len(routingModes) == 0 {
		return constants.DynamicMode
	}

	switch routingModes[0] {
	case string(constants.NativeOnlyMode):
		return constants.NativeOnlyMode
	default:
		return constants.InvalidMode
	}
}
