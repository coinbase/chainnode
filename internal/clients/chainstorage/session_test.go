package chainstorage

import (
	"testing"

	"go.uber.org/fx"

	"github.com/coinbase/chainnode/internal/config"
	"github.com/coinbase/chainnode/internal/utils/testapp"
	"github.com/coinbase/chainnode/internal/utils/testutil"
)

func TestNewSession(t *testing.T) {
	testapp.TestAllConfigs(t, func(t *testing.T, cfg *config.Config) {
		require := testutil.Require(t)

		var deps struct {
			fx.In
			Client Client
			Parser Parser
		}
		app := testapp.New(
			t,
			testapp.WithConfig(cfg),
			Module,
			fx.Populate(&deps),
		)
		defer app.Close()

		require.NotNil(deps.Client)
		require.NotNil(deps.Parser)
	})
}
