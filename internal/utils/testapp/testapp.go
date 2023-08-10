package testapp

import (
	"fmt"
	"testing"

	"github.com/uber-go/tally/v4"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"
	"go.uber.org/zap"

	"github.com/coinbase/chainnode/internal/aws"
	"github.com/coinbase/chainnode/internal/cadence"
	"github.com/coinbase/chainnode/internal/config"
	"github.com/coinbase/chainnode/internal/utils/constants"
	"github.com/coinbase/chainnode/internal/utils/fxparams"
	"github.com/coinbase/chainnode/internal/utils/log"
	"github.com/coinbase/chainnode/internal/utils/taskpool"
	"github.com/coinbase/chainnode/internal/utils/testutil"
	"github.com/coinbase/chainnode/internal/utils/tracer"

	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"

	"github.com/coinbase/chainstorage/sdk/services"
)

type (
	TestApp interface {
		Close()
		Logger() *zap.Logger
		Config() *config.Config
	}

	TestFn func(t *testing.T, cfg *config.Config)

	TestConfig struct {
		Namespace   string
		ConfigNames []string
	}

	testAppImpl struct {
		app    *fxtest.App
		logger *zap.Logger
		config *config.Config
	}

	localOnlyOption struct {
		fx.Option
	}

	customManager struct {
		manager services.SystemManager
	}

	managerParams struct {
		fx.In
		CustomManager *customManager `optional:"true"`
	}
)

var (
	// define the full list of ConfigNames that ChainNode supports
	testConfigs = []TestConfig{
		{
			Namespace: "chainnode",
			ConfigNames: []string{
				"ethereum-mainnet",
				"ethereum-goerli",
				"polygon-mainnet",
			},
		},
	}
)

func New(t testing.TB, opts ...fx.Option) TestApp {
	logger, err := log.NewDevelopment()
	if err != nil {
		panic(err)
	}

	var cfg *config.Config
	opts = append(
		opts,
		aws.Module,
		cadence.Module,
		config.Module,
		fxparams.Module,
		taskpool.Module,
		tracer.Module,
		fx.NopLogger,
		fx.Provide(newManager),
		fx.Provide(func() testing.TB { return t }),
		fx.Provide(func() *zap.Logger { return logger }),
		fx.Provide(func() tally.Scope { return tally.NewTestScope(constants.ServiceName, nil) }),
		fx.Populate(&cfg),
	)

	app := fxtest.New(t, opts...)
	app.RequireStart()
	return &testAppImpl{
		app:    app,
		logger: logger,
		config: cfg,
	}
}

// WithConfig overrides the default config.
func WithConfig(cfg *config.Config) fx.Option {
	return config.WithCustomConfig(cfg)
}

// WithIntegration runs the test only if $TEST_TYPE is integration.
func WithIntegration() fx.Option {
	return &localOnlyOption{
		Option: fx.Invoke(func(tb testing.TB, cfg *config.Config, logger *zap.Logger) {
			if !cfg.IsIntegrationTest() {
				logger.Warn("skipping integration test", zap.String("test", tb.Name()))
				tb.Skip()
			}
		}),
	}
}

// WithFunctional runs the test only if $TEST_TYPE is functional.
func WithFunctional() fx.Option {
	return &localOnlyOption{
		Option: fx.Invoke(func(tb testing.TB, cfg *config.Config, logger *zap.Logger) {
			if !cfg.IsFunctionalTest() {
				logger.Warn("skipping functional test", zap.String("test", tb.Name()))
				tb.Skip()
			}

			if cfg.IsCI() && cfg.Blockchain() == common.Blockchain_BLOCKCHAIN_ARBITRUM {
				logger.Warn("skipping functional test for arbitrum", zap.String("test", tb.Name()))
				tb.Skip()
			}
		}),
	}
}

// WithManager injects a real system manager; otherwise, a mock system manager is provided.
func WithManager(manager services.SystemManager) fx.Option {
	return fx.Provide(func() *customManager {
		return &customManager{manager: manager}
	})
}

func newManager(params managerParams) services.SystemManager {
	if params.CustomManager != nil {
		return params.CustomManager.manager
	}

	return services.NewMockSystemManager()
}

func (a *testAppImpl) Close() {
	a.app.RequireStop()
}

func (a *testAppImpl) Logger() *zap.Logger {
	return a.logger
}

func (a *testAppImpl) Config() *config.Config {
	return a.config
}

var EnvsToTest = []config.Env{
	config.EnvLocal,
	config.EnvDevelopment,
	config.EnvProduction,
}

// configNameToEnvsMap key: the config name, value: the list of AWS accounts where the config name was created
var configNameToEnvsMap = map[string][]config.Env{
	"ethereum-mainnet": EnvsToTest,
	"ethereum-goerli":  EnvsToTest,
	"polygon-mainnet":  EnvsToTest,
}

func TestAllConfigs(t *testing.T, fn TestFn) {
	for _, testConfig := range testConfigs {
		namespace := testConfig.Namespace
		for _, configName := range testConfig.ConfigNames {
			name := fmt.Sprintf("%v/%v", namespace, configName)
			t.Run(name, func(t *testing.T) {
				for _, env := range configNameToEnvsMap[configName] {
					t.Run(string(env), func(t *testing.T) {
						require := testutil.Require(t)
						blockchain, network, err := config.ParseConfigName(configName)
						require.NoError(err)

						cfg, err := config.New(
							config.WithEnvironment(env),
							config.WithBlockchain(blockchain),
							config.WithNetwork(network),
						)
						require.NoError(err)
						require.Equal(env, cfg.Env())
						require.Equal(blockchain, cfg.Blockchain())
						require.Equal(network, cfg.Network())

						fn(t, cfg)
					})
				}
			})
		}
	}
}
