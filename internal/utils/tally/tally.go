package tally

import (
	"context"
	"time"

	"github.com/uber-go/tally/v4"
	"go.uber.org/fx"

	"github.com/coinbase/chainnode/internal/config"
	"github.com/coinbase/chainnode/internal/utils/constants"
)

type (
	MetricParams struct {
		fx.In
		Lifecycle fx.Lifecycle
		Config    *config.Config
		Reporter  tally.StatsReporter `optional:"true"`
	}
)

const (
	reportingInterval = time.Second
)

func NewRootScope(params MetricParams) tally.Scope {
	// XXX: Inject your own reporter here.
	reporter := params.Reporter
	if reporter == nil {
		reporter = tally.NullStatsReporter
	}

	opts := tally.ScopeOptions{
		Prefix:   constants.ServiceName,
		Reporter: reporter,
		Tags:     params.Config.GetCommonTags(),
	}
	scope, closer := tally.NewRootScope(opts, reportingInterval)
	params.Lifecycle.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			return closer.Close()
		},
	})

	return scope
}
