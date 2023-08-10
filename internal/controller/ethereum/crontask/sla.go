package crontask

import (
	"context"
	"math"
	"time"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/uber-go/tally/v4"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	chainstorageapi "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"

	"github.com/coinbase/chainnode/internal/api"
	"github.com/coinbase/chainnode/internal/clients/blockchain/jsonrpc"
	"github.com/coinbase/chainnode/internal/clients/chainstorage"
	"github.com/coinbase/chainnode/internal/config"
	"github.com/coinbase/chainnode/internal/controller/ethereum/handler"
	"github.com/coinbase/chainnode/internal/controller/internal"
	"github.com/coinbase/chainnode/internal/storage"
	checkpointStorage "github.com/coinbase/chainnode/internal/storage/checkpoint"
	"github.com/coinbase/chainnode/internal/utils/fxparams"
	"github.com/coinbase/chainnode/internal/utils/log"
	"github.com/coinbase/chainnode/internal/utils/syncgroup"
	"github.com/coinbase/chainnode/internal/utils/timeutil"
)

const (
	slaScope  = "sla"
	slaMetric = "sla"

	outOfSLAMsg = "out_of_sla"
	expectedTag = "expected"
	actualTag   = "actual"

	severityTag = "severity"
	sev1        = "sev1"
	sev2        = "sev2"
	sev3        = "sev3"

	resultTypeTag     = "result_type"
	resultTypeSuccess = "success"
	resultTypeError   = "error"

	slaTypeTag                      = "sla_type"
	blockHeightDeltaMetric          = "block_height_delta"
	blockTimeDeltaMetric            = "block_time_delta"
	timeSinceLastBlockMetric        = "time_since_last_block"
	validatorBlockHeightDeltaMetric = "validator_block_height_delta"

	slaTypeLoggerTag = "slaType"
)

var (
	errSkipped = xerrors.New("skipped")
)

type (
	SLATaskParams struct {
		fx.In
		fxparams.Params
		CheckpointStorage  checkpointStorage.CheckpointStorage
		ChainStorageClient chainstorage.Client
		Config             *config.Config
		ClientValidator    jsonrpc.Client `name:"validator"`
	}

	slaTask struct {
		enabled            bool
		spec               string
		checkpointStorage  checkpointStorage.CheckpointStorage
		chainStorageClient chainstorage.Client
		cfg                *config.Config
		logger             *zap.Logger
		metrics            *slaMetrics
		clientValidator    jsonrpc.Client
	}

	slaMetrics struct {
		blockHeightDelta                   tally.Gauge
		blockHeightDeltaWithinSLA          tally.Counter
		blockHeightDeltaOutOfSLA           tally.Counter
		blockTimeDelta                     tally.Gauge
		blockTimeDeltaWithinSLA            tally.Counter
		blockTimeDeltaOutOfSLA             tally.Counter
		timeSinceLastBlock                 tally.Gauge
		timeSinceLastBlockWithinSLA        tally.Counter
		timeSinceLastBlockOutOfSLA         tally.Counter
		validatorBlockHeightDelta          tally.Gauge
		validatorBlockHeightDeltaWithinSLA tally.Counter
		validatorBlockHeightDeltaOutOfSLA  tally.Counter
	}
)

func NewSLATask(params SLATaskParams) (internal.CronTask, error) {
	spec := "@every 5s"
	if params.Config.Env() == config.EnvProduction {
		spec = "@every 1s"
	}
	return &slaTask{
		enabled:            !params.Config.Cron.DisableSLA,
		checkpointStorage:  params.CheckpointStorage,
		chainStorageClient: params.ChainStorageClient,
		cfg:                params.Config,
		logger:             log.WithPackage(params.Logger),
		metrics:            newSLAMetrics(params.Metrics),
		spec:               spec,
		clientValidator:    params.ClientValidator,
	}, nil
}

func newSLAMetrics(rootScope tally.Scope) *slaMetrics {
	scope := rootScope.SubScope(slaScope)

	newSLACounter := func(sla string, severity string, resultType string) tally.Counter {
		return rootScope.Tagged(map[string]string{
			slaTypeTag:    sla,
			severityTag:   severity,
			resultTypeTag: resultType,
		}).Counter(slaMetric)
	}

	return &slaMetrics{
		blockHeightDelta:                   scope.Gauge(blockHeightDeltaMetric),
		blockHeightDeltaWithinSLA:          newSLACounter(blockHeightDeltaMetric, sev1, resultTypeSuccess),
		blockHeightDeltaOutOfSLA:           newSLACounter(blockHeightDeltaMetric, sev1, resultTypeError),
		blockTimeDelta:                     scope.Gauge(blockTimeDeltaMetric),
		blockTimeDeltaWithinSLA:            newSLACounter(blockTimeDeltaMetric, sev1, resultTypeSuccess),
		blockTimeDeltaOutOfSLA:             newSLACounter(blockTimeDeltaMetric, sev1, resultTypeError),
		timeSinceLastBlock:                 scope.Gauge(timeSinceLastBlockMetric),
		timeSinceLastBlockWithinSLA:        newSLACounter(timeSinceLastBlockMetric, sev1, resultTypeSuccess),
		timeSinceLastBlockOutOfSLA:         newSLACounter(timeSinceLastBlockMetric, sev1, resultTypeError),
		validatorBlockHeightDelta:          scope.Gauge(validatorBlockHeightDeltaMetric),
		validatorBlockHeightDeltaWithinSLA: newSLACounter(validatorBlockHeightDeltaMetric, sev2, resultTypeSuccess),
		validatorBlockHeightDeltaOutOfSLA:  newSLACounter(validatorBlockHeightDeltaMetric, sev2, resultTypeError),
	}
}

func (t *slaTask) Name() string {
	return "sla"
}

func (t *slaTask) Spec() string {
	return t.spec
}

func (t *slaTask) Parallelism() int64 {
	return 1
}

func (t *slaTask) Enabled() bool {
	return t.enabled
}

func (t *slaTask) DelayStartDuration() time.Duration {
	return 0
}

func (t *slaTask) Run(ctx context.Context) error {
	now := time.Now()
	sla := t.cfg.SLA
	tagStable := t.cfg.Tag.Stable
	eventTag := t.cfg.Workflows.Coordinator.EventTag
	group, ctx := syncgroup.New(ctx)

	var lastChkp *api.Checkpoint
	group.Go(func() error {
		checkpoint, err := t.checkpointStorage.GetCheckpoint(ctx, api.CollectionLatestCheckpoint, tagStable)
		if err != nil {
			if xerrors.Is(err, storage.ErrItemNotFound) {
				t.logger.Info("no checkpoint founded")
				return errSkipped
			}
			return xerrors.Errorf("failed to get latest checkpoint: %w", err)
		}
		lastChkp = checkpoint
		return nil
	})

	var csLatestBlockEvent *chainstorageapi.BlockchainEvent
	group.Go(func() error {
		events, err := t.chainStorageClient.GetChainEvents(ctx, &chainstorageapi.GetChainEventsRequest{
			InitialPositionInStream: chainstorage.InitialPositionLatest,
			MaxNumEvents:            1,
			EventTag:                eventTag,
		})
		if err != nil {
			return xerrors.Errorf("failed to get latest blockchain event: %w", err)
		}
		if len(events) != 1 {
			return xerrors.Errorf("got unexpected number of events: %v", len(events))
		}

		csLatestBlockEvent = events[0]
		if csLatestBlockEvent.Type != chainstorageapi.BlockchainEvent_BLOCK_ADDED {
			t.logger.Info("skipping sla task",
				zap.Int64("eventSequence", csLatestBlockEvent.SequenceNum),
				zap.String("eventType", csLatestBlockEvent.Type.String()),
			)
			return errSkipped
		}
		return nil
	})

	var validatorHeight uint64
	group.Go(func() error {
		response, err := t.clientValidator.Call(ctx, handler.EthBlockNumber, nil)
		// Swallow the errors from validator; otherwise, we will lose the critical sla metrics when the validator node is down.
		if err != nil {
			return nil
		}

		var result string
		if err := response.Unmarshal(&result); err != nil {
			return xerrors.Errorf("failed to unmarshal result: %w", err)
		}

		height, err := hexutil.DecodeUint64(result)
		if err != nil {
			return xerrors.Errorf("failed to decode height: %w", err)
		}
		validatorHeight = height

		return nil
	})

	if err := group.Wait(); err != nil {
		if xerrors.Is(err, errSkipped) {
			return nil
		}
		return xerrors.Errorf("failed to finish sla task: %w", err)
	}

	// Measure SLA by comparing the latest block in chainnode and chainstorage
	// Because each chain has different SLA expectations,
	// the SLA is loaded from the config and a corresponding counter is emitted if it is out of SLA.
	var blockHeightDelta uint64
	var blockTimeDelta time.Duration
	var timeSinceLastBlock time.Duration
	var validatorBlockHeightDelta int64

	csLatestBlock := csLatestBlockEvent.GetBlock()
	if csLatestBlock == nil {
		return xerrors.Errorf("block of chainstorage event %v is nil", csLatestBlockEvent.SequenceNum)
	}

	blockHeightDelta = uint64(math.Max(0, float64(csLatestBlock.Height)-float64(lastChkp.Height)))
	t.metrics.blockHeightDelta.Update(float64(blockHeightDelta))
	if blockHeightDelta < sla.BlockHeightDelta {
		t.metrics.blockHeightDeltaWithinSLA.Inc(1)
	} else {
		t.metrics.blockHeightDeltaOutOfSLA.Inc(1)
		t.logger.Warn(
			outOfSLAMsg,
			zap.Uint64("chainnodeHeight", lastChkp.Height),
			zap.Uint64("chainstorageHeight", csLatestBlock.Height),
			zap.String(slaTypeLoggerTag, blockHeightDeltaMetric),
			zap.Uint64(expectedTag, sla.BlockHeightDelta),
			zap.Uint64(actualTag, blockHeightDelta),
		)
	}

	if validatorHeight == 0 {
		// If the validator height is 0, it means that there is error returned by the validator node. We will skip this check.
		validatorBlockHeightDelta = 0
	} else {
		validatorBlockHeightDelta = int64(float64(validatorHeight) - float64(lastChkp.Height))
	}
	t.metrics.validatorBlockHeightDelta.Update(float64(validatorBlockHeightDelta))
	if validatorBlockHeightDelta < int64(sla.ValidatorBlockHeightDelta) {
		t.metrics.validatorBlockHeightDeltaWithinSLA.Inc(1)
	} else {
		t.metrics.validatorBlockHeightDeltaOutOfSLA.Inc(1)
		t.logger.Warn(
			outOfSLAMsg,
			zap.Uint64("chainNodeHeight", lastChkp.Height),
			zap.Uint64("validatorHeight", validatorHeight),
			zap.String(slaTypeLoggerTag, validatorBlockHeightDeltaMetric),
			zap.Uint64(expectedTag, sla.ValidatorBlockHeightDelta),
			zap.Int64(actualTag, validatorBlockHeightDelta),
		)
	}

	if !lastChkp.LastBlockTime.IsZero() {
		timeSinceLastBlock = now.Sub(lastChkp.LastBlockTime)
		t.metrics.timeSinceLastBlock.Update(timeSinceLastBlock.Seconds())
		if timeSinceLastBlock < sla.TimeSinceLastBlock {
			t.metrics.timeSinceLastBlockWithinSLA.Inc(1)
		} else {
			t.metrics.timeSinceLastBlockOutOfSLA.Inc(1)
			t.logger.Warn(
				outOfSLAMsg,
				zap.Uint64("chainnodeHeight", lastChkp.Height),
				zap.Uint64("chainstorageHeight", csLatestBlock.Height),
				zap.String(slaTypeLoggerTag, timeSinceLastBlockMetric),
				zap.String(expectedTag, sla.TimeSinceLastBlock.String()),
				zap.String(actualTag, timeSinceLastBlock.String()),
			)
		}

		csLatestBlockTime := timeutil.TimestampToTime(csLatestBlock.Timestamp)
		if !csLatestBlockTime.IsZero() {
			blockTimeDelta = csLatestBlockTime.Sub(lastChkp.LastBlockTime)
			if blockTimeDelta < 0 {
				blockTimeDelta = 0
			}
			t.metrics.blockTimeDelta.Update(blockTimeDelta.Seconds())
			if blockTimeDelta < sla.BlockTimeDelta {
				t.metrics.blockTimeDeltaWithinSLA.Inc(1)
			} else {
				t.metrics.blockTimeDeltaOutOfSLA.Inc(1)
				t.logger.Warn(
					outOfSLAMsg,
					zap.Uint64("chainnodeHeight", lastChkp.Height),
					zap.Uint64("chainstorageHeight", csLatestBlock.Height),
					zap.String(slaTypeLoggerTag, blockTimeDeltaMetric),
					zap.String(expectedTag, sla.BlockTimeDelta.String()),
					zap.String(actualTag, blockTimeDelta.String()),
				)
			}
		}
	}

	t.logger.Info(
		"finished sla task",
		zap.Uint64("blockHeightDelta", blockHeightDelta),
		zap.String("blockTimeDelta", blockTimeDelta.String()),
		zap.String("timeSinceLastBlock", timeSinceLastBlock.String()),
	)

	return nil
}
