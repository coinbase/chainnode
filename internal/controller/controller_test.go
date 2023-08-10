package controller

import (
	"testing"

	"go.temporal.io/sdk/testsuite"
	"go.uber.org/fx"

	"github.com/coinbase/chainnode/internal/cadence"
	"github.com/coinbase/chainnode/internal/clients"
	"github.com/coinbase/chainnode/internal/config"
	"github.com/coinbase/chainnode/internal/storage"
	"github.com/coinbase/chainnode/internal/utils/testapp"
	"github.com/coinbase/chainnode/internal/utils/testutil"
)

type (
	ControllerTestSuite struct {
		testsuite.WorkflowTestSuite
		t *testing.T
	}
)

func (s ControllerTestSuite) T() *testing.T {
	return s.t
}

func TestNewController(t *testing.T) {
	testapp.TestAllConfigs(t, func(t *testing.T, cfg *config.Config) {
		require := testutil.Require(t)

		ts := &ControllerTestSuite{t: t}
		env := cadence.NewTestEnv(ts)
		var controller Controller
		testapp.New(
			t,
			testapp.WithConfig(cfg),
			Module,
			clients.Module,
			storage.Module,
			cadence.WithTestEnv(env),
			storage.WithEmptyStorage(),
			fx.Populate(&controller),
		)
		require.NotNil(controller)
	})
}
