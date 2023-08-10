package crontask

import (
	"context"
	"time"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/uber-go/tally/v4"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainnode/internal/clients/blockchain/jsonrpc"
	"github.com/coinbase/chainnode/internal/config"
	"github.com/coinbase/chainnode/internal/controller/ethereum/handler"
	"github.com/coinbase/chainnode/internal/controller/internal"
	"github.com/coinbase/chainnode/internal/utils/fxparams"
	"github.com/coinbase/chainnode/internal/utils/log"
)

type (
	ShadowTaskParams struct {
		fx.In
		fxparams.Params
		Client jsonrpc.Client `name:"proxy"`
		Config *config.Config
	}

	shadowTask struct {
		enabled bool
		spec    string
		logger  *zap.Logger
		client  jsonrpc.Client
		metrics *shadowTaskMetrics
	}

	shadowTaskMetrics struct {
		height             tally.Gauge
		timeSinceLastBlock tally.Gauge
	}
)

const (
	shadowTaskScopeName     = "shadow"
	heightGauge             = "height"
	timeSinceLastBlockGauge = "time_since_last_block"
)

func NewShadowTask(params ShadowTaskParams) internal.CronTask {
	spec := "@every 3s"
	if params.Config.Env() == config.EnvProduction {
		spec = "@every 1s"
	}

	return &shadowTask{
		enabled: !params.Config.Cron.DisableShadow,
		logger:  log.WithPackage(params.Logger),
		client:  params.Client,
		metrics: newShadowTaskMetrics(params.Metrics.SubScope(subScope)),
		spec:    spec,
	}
}

func newShadowTaskMetrics(scope tally.Scope) *shadowTaskMetrics {
	scope = scope.SubScope(shadowTaskScopeName)
	return &shadowTaskMetrics{
		height:             scope.Gauge(heightGauge),
		timeSinceLastBlock: scope.Gauge(timeSinceLastBlockGauge),
	}
}

func (t *shadowTask) Name() string {
	return "shadow"
}

func (t *shadowTask) Spec() string {
	return t.spec
}

func (t *shadowTask) Parallelism() int64 {
	return 1
}

func (t *shadowTask) Enabled() bool {
	return t.enabled
}

func (t *shadowTask) DelayStartDuration() time.Duration {
	return 0
}

func (t *shadowTask) Run(ctx context.Context) error {
	resp, err := t.client.Call(ctx, handler.EthBlockNumber, nil)
	if err != nil {
		return xerrors.Errorf("failed to get block number: %w", err)
	}

	var blockNumber hexutil.Uint64
	if err := resp.Unmarshal(&blockNumber); err != nil {
		return xerrors.Errorf("failed to unmarshal block number: %w", err)
	}

	resp, err = t.client.Call(ctx, handler.EthGetBlockByNumber, jsonrpc.Params{blockNumber, false})
	if err != nil {
		return xerrors.Errorf("failed to get block: %w", err)
	}

	var block struct {
		Number    hexutil.Uint64 `json:"number"`
		BlockTime hexutil.Uint64 `json:"timestamp"`
	}
	if err := resp.Unmarshal(&block); err != nil {
		return xerrors.Errorf("failed to unmarshal block: %w", err)
	}

	// if there is a chain reorg, or the request is routed to a node that does not have the block yet, then it will return an empty result
	if block.Number == 0 {
		return nil
	}

	if block.Number != blockNumber || block.BlockTime == 0 {
		return xerrors.Errorf("invalid block: %+v", block)
	}

	t.metrics.height.Update(float64(block.Number))
	blockTime := time.Unix(int64(block.BlockTime), 0)
	timeSinceLastBlock := time.Since(blockTime)
	t.metrics.timeSinceLastBlock.Update(timeSinceLastBlock.Seconds())
	t.logger.Info(
		"finished shadow task",
		zap.Uint64("height", uint64(block.Number)),
		zap.String("timeSinceLastBlock", timeSinceLastBlock.String()),
	)

	return nil
}
