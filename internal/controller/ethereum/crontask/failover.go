package crontask

import (
	"context"
	"time"

	"github.com/uber-go/tally/v4"
	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/coinbase/chainnode/internal/clients/blockchain/endpoints"
	"github.com/coinbase/chainnode/internal/config"
	"github.com/coinbase/chainnode/internal/controller/internal"
	"github.com/coinbase/chainnode/internal/utils/fxparams"
	"github.com/coinbase/chainnode/internal/utils/log"
)

type (
	FailoverTaskParams struct {
		fx.In
		fxparams.Params
		EndpointProvider endpoints.EndpointProvider `name:"proxy"`
		Config           *config.Config
	}

	failoverTask struct {
		enabled          bool
		logger           *zap.Logger
		endpointProvider endpoints.EndpointProvider
		metrics          *failoverTaskMetrics
	}

	failoverTaskMetrics struct {
		enabled tally.Gauge
	}
)

const (
	failoverTaskScopeName = "failover"
	failoverEnabledGauge  = "enabled"
)

func NewFailoverTask(params FailoverTaskParams) internal.CronTask {
	return &failoverTask{
		enabled:          !params.Config.Cron.DisableFailover,
		logger:           log.WithPackage(params.Logger),
		endpointProvider: params.EndpointProvider,
		metrics:          newFailoverTaskMetrics(params.Metrics.SubScope(subScope)),
	}
}

func newFailoverTaskMetrics(scope tally.Scope) *failoverTaskMetrics {
	scope = scope.SubScope(failoverTaskScopeName)
	return &failoverTaskMetrics{
		enabled: scope.Gauge(failoverEnabledGauge),
	}
}

func (t *failoverTask) Name() string {
	return "failover"
}

func (t *failoverTask) Spec() string {
	return "@every 60s"
}

func (t *failoverTask) Parallelism() int64 {
	return 1
}

func (t *failoverTask) Enabled() bool {
	return t.enabled
}

func (t *failoverTask) DelayStartDuration() time.Duration {
	return 0
}

func (t *failoverTask) Run(ctx context.Context) error {
	failoverEnabled := t.endpointProvider.FailoverEnabled(ctx)
	if failoverEnabled {
		t.metrics.enabled.Update(1)
	} else {
		t.metrics.enabled.Update(0)
	}

	t.logger.Info(
		"finished failover task",
		zap.Bool("failoverEnabled", failoverEnabled),
	)
	return nil
}
