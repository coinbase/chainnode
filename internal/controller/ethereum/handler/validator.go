package handler

import (
	"context"
	"encoding/json"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/eth/tracers"
	"github.com/uber-go/tally/v4"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainnode/internal/api"
	"github.com/coinbase/chainnode/internal/clients/blockchain/jsonrpc"
	"github.com/coinbase/chainnode/internal/config"
	"github.com/coinbase/chainnode/internal/server/rpc"
	"github.com/coinbase/chainnode/internal/utils/constants"
	"github.com/coinbase/chainnode/internal/utils/fxparams"
	"github.com/coinbase/chainnode/internal/utils/log"
	"github.com/coinbase/chainnode/internal/utils/picker"
	"github.com/coinbase/chainnode/internal/utils/taskpool"

	c3common "github.com/coinbase/chainstorage/protos/coinbase/c3/common"
)

type (
	ValidatorParams struct {
		fx.In
		fxparams.Params
		TaskPool   taskpool.TaskPool
		NodeClient jsonrpc.Client   `name:"validator"`
		Result     *ValidatorResult `optional:"true"` // Injected by tests.
	}

	Validator interface {
		WithValidatorInterceptor(receiver Receiver) Receiver
	}

	ValidatorResult struct {
		Done chan struct{}
	}

	validator struct {
		config           *config.Config
		methodConfigs    map[string]config.MethodConfig
		logger           *zap.Logger
		scope            tally.Scope
		taskPool         taskpool.TaskPool
		nodeClient       jsonrpc.Client
		shadowPercentage int
		result           *ValidatorResult
	}

	receiverValidator struct {
		receiver                                     Receiver
		config                                       *config.Config
		logger                                       *zap.Logger
		blockNumberValidator                         *methodValidator
		getBlockByHashValidator                      *methodValidator
		getBlockByNumberValidator                    *methodValidator
		getBlockTransactionCountByHashValidator      *methodValidator
		getBlockTransactionCountByNumberValidator    *methodValidator
		getUncleCountByBlockHashValidator            *methodValidator
		getUncleCountByBlockNumberValidator          *methodValidator
		getLogsValidator                             *methodValidator
		getTransactionByHashValidator                *methodValidator
		getTransactionReceiptValidator               *methodValidator
		getTransactionByBlockHashAndIndexValidator   *methodValidator
		getTransactionByBlockNumberAndIndexValidator *methodValidator
		traceBlockByHashValidator                    *methodValidator
		arbtraceBlockValidator                       *methodValidator
		getAuthorValidator                           *methodValidator
	}

	methodValidator struct {
		picker            picker.Picker
		method            *jsonrpc.RequestMethod
		taskPool          taskpool.TaskPool
		nodeClient        jsonrpc.Client
		config            *config.Config
		logger            *zap.Logger
		matchedCounter    tally.Counter
		unmatchedCounter  tally.Counter
		skippedCounter    tally.Counter
		errorCounter      tally.Counter
		latencyDeltaTimer tally.Timer
		latencyDeltaGauge tally.Gauge
		filter            filterFn
		preprocessor      preprocessorFn
		result            *ValidatorResult
	}

	validatorFn    func(ctx context.Context) (interface{}, error)
	filterFn       func(cfg *config.Config, primaryOutput interface{}, shadowOutput interface{}) bool
	preprocessorFn func(cfg *config.Config, input interface{}) (interface{}, error)

	validatorMode int
)

const (
	validatorModeUnknown validatorMode = iota
	validatorModeShadow
	validatorModePassThrough
)

const (
	validatorScopeName = "validator"
	methodTag          = "method"
	resultTypeTag      = "result_type"
	parityCounter      = "parity"
	latencyDeltaTimer  = "latency_delta"
	latencyDeltaGauge  = "latency_delta_percentage"
	validatorTaskName  = "validator"

	severityTag = "severity"
	sev1        = "sev1"
	sev2        = "sev2"
	sev3        = "sev3"
)

var (
	_ Validator = (*validator)(nil)
	_ Receiver  = (*receiverValidator)(nil)

	// if a method does not exist in methodSeverityMap, it will default to sev3
	methodSeverityMap = map[*jsonrpc.RequestMethod]string{
		EthGetBlockByHash:                      sev1,
		EthGetBlockByNumber:                    sev1,
		EthGetLogs:                             sev1,
		EthGetTransactionByHash:                sev1,
		EthGetTransactionReceipt:               sev1,
		EthGetTransactionByBlockHashAndIndex:   sev1,
		EthGetTransactionByBlockNumberAndIndex: sev1,
		EthGetUncleCountByBlockHash:            sev1,
		EthGetUncleCountByBlockNumber:          sev1,
		DebugTraceBlockByHash:                  sev1,
		BorGetAuthor:                           sev1,
		EthBlockNumber:                         sev2,
		EthGetBlockTransactionCountByHash:      sev3,
		EthGetBlockTransactionCountByNumber:    sev3,
	}
)

func NewValidator(params ValidatorParams) (Validator, error) {
	shadowPercentage := params.Config.Controller.Handler.ShadowPercentage
	if shadowPercentage < 0 || shadowPercentage > 100 {
		return nil, xerrors.Errorf("invalid shadow percentage: %v", shadowPercentage)
	}

	return &validator{
		config:           params.Config,
		methodConfigs:    make(map[string]config.MethodConfig),
		logger:           log.WithPackage(params.Logger),
		scope:            params.Metrics.SubScope(validatorScopeName),
		nodeClient:       params.NodeClient,
		taskPool:         params.TaskPool,
		shadowPercentage: shadowPercentage,
		result:           params.Result,
	}, nil
}

func (v *validator) WithValidatorInterceptor(receiver Receiver) Receiver {
	if v.shadowPercentage == 0 {
		// If shadowing is disabled, return the original receiver.
		return receiver
	}

	for _, methodConfig := range v.config.Controller.Handler.MethodConfigs {
		v.methodConfigs[methodConfig.MethodName] = methodConfig
	}

	return &receiverValidator{
		receiver:                                receiver,
		config:                                  v.config,
		logger:                                  v.logger,
		blockNumberValidator:                    v.newMethodValidator(EthBlockNumber, blockNumberFilter, defaultPreprocessor),
		getBlockByHashValidator:                 v.newMethodValidator(EthGetBlockByHash, blockFilter, blockPreprocessor),
		getBlockByNumberValidator:               v.newMethodValidator(EthGetBlockByNumber, blockFilter, blockPreprocessor),
		getBlockTransactionCountByHashValidator: v.newMethodValidator(EthGetBlockTransactionCountByHash, nullFilter, defaultPreprocessor),
		getBlockTransactionCountByNumberValidator:    v.newMethodValidator(EthGetBlockTransactionCountByNumber, nullFilter, defaultPreprocessor),
		getUncleCountByBlockHashValidator:            v.newMethodValidator(EthGetUncleCountByBlockHash, nullFilter, defaultPreprocessor),
		getUncleCountByBlockNumberValidator:          v.newMethodValidator(EthGetUncleCountByBlockNumber, nullFilter, defaultPreprocessor),
		getLogsValidator:                             v.newMethodValidator(EthGetLogs, logsFilter, defaultPreprocessor),
		getTransactionByHashValidator:                v.newMethodValidator(EthGetTransactionByHash, transactionFilter, transactionByHashPreprocessor),
		getTransactionReceiptValidator:               v.newMethodValidator(EthGetTransactionReceipt, transactionReceiptFilter, transactionReceiptPreprocessor),
		getTransactionByBlockHashAndIndexValidator:   v.newMethodValidator(EthGetTransactionByBlockHashAndIndex, transactionFilter, transactionByHashPreprocessor),
		getTransactionByBlockNumberAndIndexValidator: v.newMethodValidator(EthGetTransactionByBlockNumberAndIndex, transactionFilter, transactionByHashPreprocessor),
		traceBlockByHashValidator:                    v.newMethodValidator(DebugTraceBlockByHash, traceBlockFilter, traceBlockByHashPreprocessor),
		arbtraceBlockValidator:                       v.newMethodValidator(ArbtraceBlock, traceBlockFilter, defaultPreprocessor),
		getAuthorValidator:                           v.newMethodValidator(BorGetAuthor, nullFilter, defaultPreprocessor),
	}
}

func (v *validator) newMethodValidator(method *jsonrpc.RequestMethod, filter filterFn, preprocessor preprocessorFn) *methodValidator {
	methodName := method.Name
	shadowPercentage := v.shadowPercentage
	if methodConfig, ok := v.methodConfigs[methodName]; ok {
		shadowPercentage = methodConfig.ShadowPercentage
	}

	var choices []*picker.Choice
	if shadowPercentage > 0 {
		choices = append(choices, &picker.Choice{
			Item:   validatorModeShadow,
			Weight: shadowPercentage,
		})
	}
	if shadowPercentage < 100 {
		choices = append(choices, &picker.Choice{
			Item:   validatorModePassThrough,
			Weight: 100 - shadowPercentage,
		})
	}
	picker := picker.New(choices)

	severity := GetMethodSeverity(method)
	return &methodValidator{
		picker:            picker,
		method:            method,
		nodeClient:        v.nodeClient,
		taskPool:          v.taskPool,
		config:            v.config,
		logger:            v.logger.With(zap.String(methodTag, methodName)),
		matchedCounter:    v.scope.Tagged(map[string]string{methodTag: methodName, severityTag: severity, resultTypeTag: "matched"}).Counter(parityCounter),
		unmatchedCounter:  v.scope.Tagged(map[string]string{methodTag: methodName, severityTag: severity, resultTypeTag: "unmatched"}).Counter(parityCounter),
		skippedCounter:    v.scope.Tagged(map[string]string{methodTag: methodName, severityTag: severity, resultTypeTag: "skipped"}).Counter(parityCounter),
		errorCounter:      v.scope.Tagged(map[string]string{methodTag: methodName, severityTag: severity, resultTypeTag: "error"}).Counter(parityCounter),
		latencyDeltaTimer: v.scope.Tagged(map[string]string{methodTag: methodName}).Timer(latencyDeltaTimer),
		latencyDeltaGauge: v.scope.Tagged(map[string]string{methodTag: methodName}).Gauge(latencyDeltaGauge),
		filter:            filter,
		preprocessor:      preprocessor,
		result:            v.result,
	}
}

func (v *receiverValidator) BlockNumber(ctx context.Context) (json.RawMessage, error) {
	mv := v.blockNumberValidator

	primaryFn := func(ctx context.Context) (interface{}, error) {
		return v.receiver.BlockNumber(ctx)
	}

	shadowFn := func(ctx context.Context) (interface{}, error) {
		return mv.callNode(ctx, nil)
	}

	result, err := mv.validate(ctx, primaryFn, shadowFn)
	if err != nil {
		return nil, err
	}

	return result.(json.RawMessage), nil
}

func (v *receiverValidator) GetBlockByHash(ctx context.Context, blockHash common.Hash, returnFullTx bool) (json.RawMessage, error) {
	mv := v.getBlockByHashValidator.withLogger(
		zap.Reflect("params", jsonrpc.Params{blockHash, returnFullTx}),
	)

	primaryFn := func(ctx context.Context) (interface{}, error) {
		return v.receiver.GetBlockByHash(ctx, blockHash, returnFullTx)
	}

	shadowFn := func(ctx context.Context) (interface{}, error) {
		return mv.callNode(ctx, jsonrpc.Params{
			blockHash,
			returnFullTx,
		})
	}

	result, err := mv.validate(ctx, primaryFn, shadowFn)
	if err != nil {
		return nil, err
	}

	return result.(json.RawMessage), nil
}

func (v *receiverValidator) GetBlockByNumber(ctx context.Context, blockNumber rpc.BlockNumber, returnFullTx bool) (json.RawMessage, error) {
	mv := v.getBlockByNumberValidator.withLogger(
		zap.Reflect("params", jsonrpc.Params{blockNumber, returnFullTx}),
	)

	primaryFn := func(ctx context.Context) (interface{}, error) {
		return v.receiver.GetBlockByNumber(ctx, blockNumber, returnFullTx)
	}

	shadowFn := func(ctx context.Context) (interface{}, error) {
		return mv.callNode(ctx, jsonrpc.Params{
			blockNumber,
			returnFullTx,
		})
	}

	// Skip validation when fetching block with `latest` or `pending` tag
	if blockNumber == rpc.LatestBlockNumber || blockNumber == rpc.PendingBlockNumber {
		primaryResult, err := primaryFn(ctx)
		if err != nil {
			return nil, err
		}
		return primaryResult.(json.RawMessage), nil
	}

	result, err := mv.validate(ctx, primaryFn, shadowFn)
	if err != nil {
		return nil, err
	}

	return result.(json.RawMessage), nil
}

func (v *receiverValidator) GetBlockTransactionCountByHash(ctx context.Context, blockHash common.Hash) (json.RawMessage, error) {
	mv := v.getBlockTransactionCountByHashValidator.withLogger(
		zap.Reflect("params", jsonrpc.Params{blockHash}),
	)

	primaryFn := func(ctx context.Context) (interface{}, error) {
		return v.receiver.GetBlockTransactionCountByHash(ctx, blockHash)
	}

	shadowFn := func(ctx context.Context) (interface{}, error) {
		return mv.callNode(ctx, jsonrpc.Params{
			blockHash,
		})
	}

	result, err := mv.validate(ctx, primaryFn, shadowFn)
	if err != nil {
		return nil, err
	}

	return result.(json.RawMessage), nil
}

func (v *receiverValidator) GetBlockTransactionCountByNumber(ctx context.Context, blockNumber rpc.BlockNumber) (json.RawMessage, error) {
	mv := v.getBlockTransactionCountByNumberValidator.withLogger(
		zap.Reflect("params", jsonrpc.Params{blockNumber}),
	)

	primaryFn := func(ctx context.Context) (interface{}, error) {
		return v.receiver.GetBlockTransactionCountByNumber(ctx, blockNumber)
	}

	shadowFn := func(ctx context.Context) (interface{}, error) {
		return mv.callNode(ctx, jsonrpc.Params{
			blockNumber,
		})
	}

	// Skip validation when fetching block with `latest` or `pending` tag
	if blockNumber == rpc.LatestBlockNumber || blockNumber == rpc.PendingBlockNumber {
		primaryResult, err := primaryFn(ctx)
		if err != nil {
			return nil, err
		}
		return primaryResult.(json.RawMessage), nil
	}

	result, err := mv.validate(ctx, primaryFn, shadowFn)
	if err != nil {
		return nil, err
	}

	return result.(json.RawMessage), nil
}

func (v *receiverValidator) GetUncleCountByBlockHash(ctx context.Context, blockHash common.Hash) (json.RawMessage, error) {
	mv := v.getUncleCountByBlockHashValidator.withLogger(
		zap.Reflect("params", jsonrpc.Params{blockHash}),
	)

	primaryFn := func(ctx context.Context) (interface{}, error) {
		return v.receiver.GetUncleCountByBlockHash(ctx, blockHash)
	}

	shadowFn := func(ctx context.Context) (interface{}, error) {
		return mv.callNode(ctx, jsonrpc.Params{
			blockHash,
		})
	}

	result, err := mv.validate(ctx, primaryFn, shadowFn)
	if err != nil {
		return nil, err
	}

	return result.(json.RawMessage), nil
}

func (v *receiverValidator) GetUncleCountByBlockNumber(ctx context.Context, blockNumber rpc.BlockNumber) (json.RawMessage, error) {
	mv := v.getUncleCountByBlockNumberValidator.withLogger(
		zap.Reflect("params", jsonrpc.Params{blockNumber}),
	)

	primaryFn := func(ctx context.Context) (interface{}, error) {
		return v.receiver.GetUncleCountByBlockNumber(ctx, blockNumber)
	}

	shadowFn := func(ctx context.Context) (interface{}, error) {
		return mv.callNode(ctx, jsonrpc.Params{
			blockNumber,
		})
	}

	// Skip validation when fetching block with `latest` or `pending` tag
	if blockNumber == rpc.LatestBlockNumber || blockNumber == rpc.PendingBlockNumber {
		primaryResult, err := primaryFn(ctx)
		if err != nil {
			return nil, err
		}
		return primaryResult.(json.RawMessage), nil
	}

	result, err := mv.validate(ctx, primaryFn, shadowFn)
	if err != nil {
		return nil, err
	}

	return result.(json.RawMessage), nil
}

func (v *receiverValidator) GetLogs(ctx context.Context, criteria FilterCriteria) (json.RawMessage, error) {
	mv := v.getLogsValidator.withLogger(
		zap.Reflect("params", jsonrpc.Params{criteria}),
	)

	primaryFn := func(ctx context.Context) (interface{}, error) {
		return v.receiver.GetLogs(ctx, criteria)
	}

	shadowFn := func(ctx context.Context) (interface{}, error) {
		return mv.callNode(ctx, jsonrpc.Params{
			criteria,
		})
	}

	// https://github.com/ethereum/go-ethereum/blob/b196ad1c165ecd6c9edaca520e7161a58e50eb06/eth/filters/api.go#L326
	if criteria.BlockHash == nil && (criteria.FromBlock == nil || criteria.ToBlock == nil || criteria.FromBlock.Int64() < 0 || criteria.ToBlock.Int64() < 0) {
		primaryResult, err := primaryFn(ctx)
		if err != nil {
			return nil, err
		}
		return primaryResult.(json.RawMessage), nil
	}

	result, err := mv.validate(ctx, primaryFn, shadowFn)
	if err != nil {
		return nil, err
	}

	return result.(json.RawMessage), nil
}

func (v *receiverValidator) GetTransactionByHash(ctx context.Context, txHash common.Hash) (json.RawMessage, error) {
	mv := v.getTransactionByHashValidator.withLogger(
		zap.Reflect("params", jsonrpc.Params{txHash}),
	)

	primaryFn := func(ctx context.Context) (interface{}, error) {
		return v.receiver.GetTransactionByHash(ctx, txHash)
	}

	shadowFn := func(ctx context.Context) (interface{}, error) {
		return mv.callNode(ctx, jsonrpc.Params{
			txHash,
		})
	}

	result, err := mv.validate(ctx, primaryFn, shadowFn)
	if err != nil {
		return nil, err
	}

	return result.(json.RawMessage), nil
}

func (v *receiverValidator) GetTransactionReceipt(ctx context.Context, txHash common.Hash) (json.RawMessage, error) {
	mv := v.getTransactionReceiptValidator.withLogger(
		zap.Reflect("params", jsonrpc.Params{txHash}),
	)

	primaryFn := func(ctx context.Context) (interface{}, error) {
		return v.receiver.GetTransactionReceipt(ctx, txHash)
	}

	shadowFn := func(ctx context.Context) (interface{}, error) {
		return mv.callNode(ctx, jsonrpc.Params{
			txHash,
		})
	}

	result, err := mv.validate(ctx, primaryFn, shadowFn)
	if err != nil {
		return nil, err
	}

	return result.(json.RawMessage), nil
}

func (v *receiverValidator) GetTransactionByBlockHashAndIndex(ctx context.Context, blockHash common.Hash, index rpc.DecimalOrHex) (json.RawMessage, error) {
	mv := v.getTransactionByBlockHashAndIndexValidator.withLogger(
		zap.Reflect("params", jsonrpc.Params{blockHash, index}),
	)

	primaryFn := func(ctx context.Context) (interface{}, error) {
		return v.receiver.GetTransactionByBlockHashAndIndex(ctx, blockHash, index)
	}

	shadowFn := func(ctx context.Context) (interface{}, error) {
		return mv.callNode(ctx, jsonrpc.Params{
			blockHash,
			hexutil.EncodeUint64(uint64(index)),
		})
	}

	result, err := mv.validate(ctx, primaryFn, shadowFn)
	if err != nil {
		return nil, err
	}

	return result.(json.RawMessage), nil
}

func (v *receiverValidator) GetTransactionByBlockNumberAndIndex(ctx context.Context, blockNumber rpc.BlockNumber, index rpc.DecimalOrHex) (json.RawMessage, error) {
	mv := v.getTransactionByBlockNumberAndIndexValidator.withLogger(
		zap.Reflect("params", jsonrpc.Params{blockNumber, index}),
	)

	primaryFn := func(ctx context.Context) (interface{}, error) {
		return v.receiver.GetTransactionByBlockNumberAndIndex(ctx, blockNumber, index)
	}

	shadowFn := func(ctx context.Context) (interface{}, error) {
		return mv.callNode(ctx, jsonrpc.Params{
			blockNumber,
			hexutil.EncodeUint64(uint64(index)),
		})
	}

	// Skip validation when fetching block with `latest` or `pending` tag
	if blockNumber == rpc.LatestBlockNumber || blockNumber == rpc.PendingBlockNumber {
		primaryResult, err := primaryFn(ctx)
		if err != nil {
			return nil, err
		}
		return primaryResult.(json.RawMessage), nil
	}

	result, err := mv.validate(ctx, primaryFn, shadowFn)
	if err != nil {
		return nil, err
	}

	return result.(json.RawMessage), nil
}

func (v *receiverValidator) TraceBlockByHash(ctx context.Context, hash common.Hash, traceConfig *tracers.TraceConfig) (json.RawMessage, error) {
	mv := v.traceBlockByHashValidator.withLogger(
		zap.Reflect("params", jsonrpc.Params{hash, traceConfig}),
	)

	primaryFn := func(ctx context.Context) (interface{}, error) {
		return v.receiver.TraceBlockByHash(ctx, hash, traceConfig)
	}

	shadowFn := func(ctx context.Context) (interface{}, error) {
		if v.config.Blockchain() == c3common.Blockchain_BLOCKCHAIN_POLYGON {
			return mv.callNode(
				ctx,
				jsonrpc.Params{hash, map[string]string{
					"tracer": ethCallTracer,
				}},
			)
		} else {
			return mv.callNode(ctx, jsonrpc.Params{hash, traceConfig})
		}
	}

	result, err := mv.validate(ctx, primaryFn, shadowFn)
	if err != nil {
		return nil, err
	}

	return result.(json.RawMessage), nil
}

func (v *receiverValidator) TraceBlockByNumber(ctx context.Context, blockNumber rpc.BlockNumber, traceConfig *tracers.TraceConfig) (json.RawMessage, error) {
	// validator endpoint does not support traceBlockByNumber. Pass through validation for now
	return v.receiver.TraceBlockByNumber(ctx, blockNumber, traceConfig)
}

func (v *receiverValidator) Block(ctx context.Context, blockNumber rpc.BlockNumber) (json.RawMessage, error) {
	mv := v.arbtraceBlockValidator.withLogger(
		zap.Reflect("params", jsonrpc.Params{blockNumber}),
	)

	primaryFn := func(ctx context.Context) (interface{}, error) {
		return v.receiver.Block(ctx, blockNumber)
	}

	shadowFn := func(ctx context.Context) (interface{}, error) {
		return mv.callNode(ctx, jsonrpc.Params{blockNumber})
	}

	result, err := mv.validate(ctx, primaryFn, shadowFn)
	if err != nil {
		return nil, err
	}

	return result.(json.RawMessage), nil
}

func (v *receiverValidator) ChainId(ctx context.Context) (json.RawMessage, error) {
	// This API simply returns a hardcoded number. No need to shadow.
	return v.receiver.ChainId(ctx)
}

func (v *receiverValidator) Version(ctx context.Context) (json.RawMessage, error) {
	// This API simply returns a hardcoded number. No need to shadow.
	return v.receiver.Version(ctx)
}

func (v *receiverValidator) Syncing(ctx context.Context) (json.RawMessage, error) {
	// no need to compare results with shadow client because shadow node may be in a different syncing status
	return v.receiver.Syncing(ctx)
}

func (v *receiverValidator) Listening(ctx context.Context) (json.RawMessage, error) {
	// no need to compare results because it is supposed to return one constant value
	return v.receiver.Listening(ctx)
}

func (v *receiverValidator) GetAuthor(ctx context.Context, blockNumber rpc.BlockNumber) (json.RawMessage, error) {
	mv := v.getAuthorValidator.withLogger(
		zap.Reflect("params", jsonrpc.Params{blockNumber}),
	)

	primaryFn := func(ctx context.Context) (interface{}, error) {
		return v.receiver.GetAuthor(ctx, blockNumber)
	}

	shadowFn := func(ctx context.Context) (interface{}, error) {
		return mv.callNode(ctx, jsonrpc.Params{blockNumber})
	}

	var primaryResult interface{}
	var err error
	if v.isDistanceReversible(ctx, blockNumber) {
		// skip validation if the request block number is within the irreversible distance
		v.getAuthorValidator.skippedCounter.Inc(1)
		primaryResult, err = primaryFn(ctx)
	} else {
		primaryResult, err = mv.validate(ctx, primaryFn, shadowFn)
	}

	if err != nil {
		return nil, err
	}

	return primaryResult.(json.RawMessage), nil
}

// isDistanceReversible returns true if the block number is the latest block or requested block number diff with current checkpoint is less than irreversible distance.
// This is to avoid validating some false positive cases where the block is not finalized during chain reorg.
func (v *receiverValidator) isDistanceReversible(ctx context.Context, blockNumber rpc.BlockNumber) bool {
	// Skip validation when fetching block with `latest` tag
	if blockNumber == rpc.LatestBlockNumber {
		return true
	}

	// Get cached latest checkpoint from context to avoid fetching it from DB
	checkpoint, ok := ctx.Value(constants.ContextKeyLatestCheckpoint).(*api.Checkpoint)
	if !ok {
		v.logger.Warn("failed to get cached latest checkpoint from context")
		// If failed to get cached latest checkpoint, we still validate anyway.
		return false
	}

	// Skip validation if block number is greater than latest checkpoint
	if checkpoint.Height < uint64(blockNumber) {
		return true
	}

	return checkpoint.Height-uint64(blockNumber) < v.config.Chain.IrreversibleDistance-1
}

func (v *methodValidator) withLogger(fields ...zap.Field) *methodValidator {
	dup := *v
	dup.logger = dup.logger.With(fields...)
	return &dup
}

func (v *methodValidator) callNode(ctx context.Context, params jsonrpc.Params) (json.RawMessage, error) {
	response, err := v.nodeClient.Call(ctx, v.method, params)
	if err != nil {
		return nil, xerrors.Errorf("failed to call node: %w", err)
	}

	return response.Result, nil
}

func (v *methodValidator) getMode() validatorMode {
	return v.picker.Next().(validatorMode)
}

// validate compares the results between primary (ChainNode) and shadow.
func (v *methodValidator) validate(ctx context.Context, primaryFn validatorFn, shadowFn validatorFn) (interface{}, error) {
	if v.getMode() == validatorModePassThrough {
		return primaryFn(ctx)
	}

	primaryStartTime := time.Now()
	primaryOutput, err := primaryFn(ctx)
	if err != nil {
		return nil, err
	}
	primaryDuration := time.Since(primaryStartTime)

	if err := v.taskPool.Submit(validatorTaskName, func(ctx context.Context) error {
		if v.result != nil {
			defer func() {
				close(v.result.Done)
			}()
		}

		shadowStartTime := time.Now()
		shadowOutput, err := shadowFn(ctx)
		if err != nil {
			v.errorCounter.Inc(1)
			return xerrors.Errorf("failed to call shadow function: %w", err)
		}
		shadowDuration := time.Since(shadowStartTime)

		// Positive values mean that ChainNode is faster than shadow.
		latencyDelta := shadowDuration - primaryDuration
		v.latencyDeltaTimer.Record(latencyDelta)

		if primaryDuration > 0 {
			latencyDeltaPercentage := float64(latencyDelta) * 100 / float64(primaryDuration)
			v.latencyDeltaGauge.Update(latencyDeltaPercentage)
		}

		if err := v.compareResults(primaryOutput, shadowOutput); err != nil {
			v.errorCounter.Inc(1)
			return xerrors.Errorf("failed to compare results: %w", err)
		}

		return nil
	}); err != nil {
		// When the pool is full, the task is intentionally dropped; otherwise, shadow node may be overloaded.
		if !xerrors.Is(err, taskpool.ErrFull) {
			v.logger.Warn("failed to submit validator task", zap.Error(err))
		}
	}

	return primaryOutput, nil
}

func (v *methodValidator) compareResults(primaryOutput interface{}, shadowOutput interface{}) error {
	var err error

	primaryOutput, err = v.preprocessor(v.config, primaryOutput)
	if err != nil {
		return xerrors.Errorf("failed to preprocess fields from primaryOutput: %w", err)
	}

	shadowOutput, err = v.preprocessor(v.config, shadowOutput)
	if err != nil {
		return xerrors.Errorf("failed to preprocess fields from shadowOutput: %w", err)
	}

	if v.filter(v.config, primaryOutput, shadowOutput) {
		// The comparison is skipped when the outputs satisfy the "filter" conditions.
		v.skippedCounter.Inc(1)
		v.logger.Debug("validation is skipped")
		return nil
	}

	primaryResult, err := formatJSON(primaryOutput)
	if err != nil {
		return xerrors.Errorf("failed to format my output: %w", err)
	}

	shadowResult, err := formatJSON(shadowOutput)
	if err != nil {
		return xerrors.Errorf("failed to format their output: %w", err)
	}

	if primaryResult == shadowResult {
		v.matchedCounter.Inc(1)
		v.logger.Debug("results are the same")
	} else {
		v.unmatchedCounter.Inc(1)
		const maxResultLength = 6000
		v.logger.Warn(
			"results are different",
			zap.String("primary", shortenString(primaryResult, maxResultLength)),
			zap.String("shadow", shortenString(shadowResult, maxResultLength)),
		)
	}
	return nil
}

func GetMethodSeverity(method *jsonrpc.RequestMethod) string {
	if severity, ok := methodSeverityMap[method]; ok {
		return severity
	}
	return sev3
}
