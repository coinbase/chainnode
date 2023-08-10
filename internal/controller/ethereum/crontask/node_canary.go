package crontask

import (
	"context"
	"time"

	"github.com/uber-go/tally/v4"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainnode/internal/clients/blockchain/endpoints"
	"github.com/coinbase/chainnode/internal/clients/blockchain/jsonrpc"
	"github.com/coinbase/chainnode/internal/controller/ethereum/handler"
	"github.com/coinbase/chainnode/internal/controller/internal"
	"github.com/coinbase/chainnode/internal/utils/fxparams"
	"github.com/coinbase/chainnode/internal/utils/instrument"
	"github.com/coinbase/chainnode/internal/utils/log"
	"github.com/coinbase/chainnode/internal/utils/syncgroup"
)

type (
	NodeCanaryTaskParams struct {
		fx.In
		fxparams.Params

		ClientProxy     jsonrpc.Client `name:"proxy"`
		ClientValidator jsonrpc.Client `name:"validator"`
		FailoverManager endpoints.FailoverManager
	}

	nodeCanaryTask struct {
		enabled bool
		logger  *zap.Logger
		metrics *nodeCanaryTaskMetrics

		proxy           jsonrpc.Client
		validator       jsonrpc.Client
		failoverManager endpoints.FailoverManager
	}

	nodeCanaryTaskMetrics struct {
		proxyPrimaryHealthCheck       instrument.Call
		proxySecondaryHealthCheck     instrument.Call
		validatorPrimaryHealthCheck   instrument.Call
		validatorSecondaryHealthCheck instrument.Call
	}
)

const (
	nodeCanaryTaskScopeName = "node_canary"

	healthCheckMsg  = "node.health_check"
	healthCheckType = "health_check_type"

	proxyPrimaryHealthCheckMetric      = "proxy_primary_health_check"
	proxySecondaryHealthCheckMetric    = "proxy_secondary_health_check"
	validatorPrimaryHeathCheckMetric   = "validator_primary_health_check"
	validatorSecondaryHeathCheckMetric = "validator_secondary_health_check"
)

var (
	ErrSkipped = xerrors.New("skipped")
)

func NewNodeCanaryTask(params NodeCanaryTaskParams) internal.CronTask {
	logger := log.WithPackage(params.Logger)
	return &nodeCanaryTask{
		enabled:         !params.Config.Cron.DisableNodeCanary,
		logger:          logger,
		metrics:         newNodeCanaryTaskMetrics(params.Metrics, logger),
		proxy:           params.ClientProxy,
		validator:       params.ClientValidator,
		failoverManager: params.FailoverManager,
	}
}

func newNodeCanaryTaskMetrics(scope tally.Scope, logger *zap.Logger) *nodeCanaryTaskMetrics {
	scope = scope.SubScope(nodeCanaryTaskScopeName)
	newHealthCheckCall := func(metricName string) instrument.Call {
		return instrument.NewCall(
			scope,
			metricName,
			instrument.WithLogger(logger.With(zap.String(healthCheckType, metricName)), healthCheckMsg),
		)
	}
	return &nodeCanaryTaskMetrics{
		proxyPrimaryHealthCheck:       newHealthCheckCall(proxyPrimaryHealthCheckMetric),
		proxySecondaryHealthCheck:     newHealthCheckCall(proxySecondaryHealthCheckMetric),
		validatorPrimaryHealthCheck:   newHealthCheckCall(validatorPrimaryHeathCheckMetric),
		validatorSecondaryHealthCheck: newHealthCheckCall(validatorSecondaryHeathCheckMetric),
	}
}

func (t *nodeCanaryTask) Name() string {
	return "node_canary"
}

func (t *nodeCanaryTask) Spec() string {
	return "@every 20s"
}

func (t *nodeCanaryTask) Parallelism() int64 {
	return 1
}

func (t *nodeCanaryTask) Enabled() bool {
	return t.enabled
}

func (t *nodeCanaryTask) DelayStartDuration() time.Duration {
	return 0
}

func (t *nodeCanaryTask) Run(ctx context.Context) error {
	group, ctx := syncgroup.New(ctx)

	// Note that failoverCtx is set to nil if failover is unavailable.
	failoverCtx, err := t.failoverManager.WithFailoverContext(ctx)
	if err != nil {
		if !xerrors.Is(err, endpoints.ErrFailoverUnavailable) {
			return xerrors.Errorf("failed to create failover context: %w", err)
		}
	}

	group.Go(func() error {
		_ = t.metrics.proxyPrimaryHealthCheck.Instrument(
			ctx,
			func(ctx context.Context) error {
				resp, err := t.proxy.Call(ctx, handler.EthBlockNumber, nil)
				if err != nil {
					return xerrors.Errorf("failed to call EthBlockNumber: %w", err)
				}
				if resp.IsNullOrEmpty() {
					return xerrors.New("EthBlockNumber response is null or empty")
				}
				return nil
			},
		)
		return nil
	})

	group.Go(func() error {
		_ = t.metrics.validatorPrimaryHealthCheck.Instrument(
			ctx,
			func(ctx context.Context) error {
				resp, err := t.validator.Call(ctx, handler.EthBlockNumber, nil)
				if err != nil {
					return xerrors.Errorf("failed to call EthBlockNumber: %w", err)
				}
				if resp.IsNullOrEmpty() {
					return xerrors.New("EthBlockNumber response is null or empty")
				}
				return nil
			},
		)
		return nil
	})

	if failoverCtx != nil {
		group.Go(func() error {
			_ = t.metrics.proxySecondaryHealthCheck.Instrument(
				ctx,
				func(ctx context.Context) error {
					resp, err := t.proxy.Call(failoverCtx, handler.EthBlockNumber, nil)
					if err != nil {
						return xerrors.Errorf("failed to call EthBlockNumber with failover context: %w", err)
					}
					if resp.IsNullOrEmpty() {
						return xerrors.New("EthBlockNumber response is null or empty")
					}
					return nil
				},
			)
			return nil
		})

		group.Go(func() error {
			_ = t.metrics.validatorSecondaryHealthCheck.Instrument(
				ctx,
				func(ctx context.Context) error {
					resp, err := t.validator.Call(failoverCtx, handler.EthBlockNumber, nil)
					if err != nil {
						return xerrors.Errorf("failed to call EthBlockNumber with failover context: %w", err)
					}
					if resp.IsNullOrEmpty() {
						return xerrors.New("EthBlockNumber response is null or empty")
					}
					return nil
				},
			)
			return nil
		})
	}

	if err := group.Wait(); err != nil {
		if xerrors.Is(err, ErrSkipped) {
			return nil
		}

		return xerrors.Errorf("failed to finish node canary task: %w", err)
	}

	t.logger.Info("finished node canary task")
	return nil
}
