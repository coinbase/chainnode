package tracer

import (
	"testing"

	"github.com/opentracing/opentracing-go"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"

	"github.com/coinbase/chainnode/internal/config"
	"github.com/coinbase/chainnode/internal/utils/testutil"

	"github.com/coinbase/chainstorage/sdk/services"
)

func TestNewTracer(t *testing.T) {
	require := testutil.Require(t)

	manager := services.NewManager()

	var tr opentracing.Tracer
	app := fxtest.New(
		t,
		Module,
		config.Module,
		fx.Provide(func() services.SystemManager { return manager }),
		fx.Populate(&tr),
	)
	defer app.RequireStop()
	require.NotNil(tr)
}
