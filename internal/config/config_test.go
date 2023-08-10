package config_test

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/coinbase/chainnode/internal/api"
	xapi "github.com/coinbase/chainnode/internal/api/ethereum"
	"github.com/coinbase/chainnode/internal/config"
	"github.com/coinbase/chainnode/internal/utils/testapp"
	"github.com/coinbase/chainnode/internal/utils/testutil"

	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
)

func TestConfigLoad_Override(t *testing.T) {
	require := testutil.Require(t)

	cfg, err := config.New(
		config.WithNetwork(common.Network_NETWORK_ETHEREUM_MAINNET),
	)
	require.NoError(err)
	require.Equal(cfg.Chain.Blockchain, common.Blockchain_BLOCKCHAIN_ETHEREUM)
	require.Equal(cfg.Chain.Network, common.Network_NETWORK_ETHEREUM_MAINNET)
}

func TestConfig(t *testing.T) {
	testapp.TestAllConfigs(t, func(t *testing.T, cfg *config.Config) {
		require := testutil.Require(t)
		require.True(cfg.AWS.IsLocalStack)
		require.True(cfg.AWS.IsResetLocal)

		require.NotEmpty(cfg.Cadence.Address)
		require.NotEmpty(cfg.Cadence.Domain)

		require.Equal(fmt.Sprintf("chainnode-%v", cfg.ConfigName), cfg.Cadence.Domain)
		require.NotEmpty(cfg.Cadence.RetentionPeriod)

		require.Equal("us-east-1", cfg.AWS.Region)
		require.Equal(fmt.Sprintf("chainnode-collections-%s-%s", cfg.ConfigName, cfg.ShortEnv()), cfg.AWS.Bucket)
		if (cfg.Env() == config.EnvDevelopment || cfg.Env() == config.EnvLocal) && cfg.ConfigName == "polygon-mainnet" {
			require.Equal("chainnode-collection-polygon-mainnet-v2", cfg.AWS.DynamoDB.CollectionTable)
		} else {
			require.Equal(fmt.Sprintf("chainnode-collection-%s", cfg.ConfigName), cfg.AWS.DynamoDB.CollectionTable)
		}

		require.Equal(fmt.Sprintf("chainnode-logs-%s", cfg.ConfigName), cfg.AWS.DynamoDB.LogsTable)
		require.Equal(fmt.Sprintf("chainnode-logs-by-block-range-%s", cfg.ConfigName), cfg.AWS.DynamoDB.LogsByBlockRangeIndexName)
		require.Equal(uint64(500), cfg.AWS.DynamoDB.LogsTablePartitionSize) // Cannot change the value
		require.Equal(396*1024, cfg.AWS.DynamoDB.MaxDataSize)

		require.Equal(int(cfg.Chain.IrreversibleDistance), cfg.Controller.Handler.MaxBlockNumberDiff)
		require.Equal(2, len(cfg.Cron.ExpectedWorkflows))
		require.True(strings.HasPrefix(cfg.Cron.ExpectedWorkflows[0], "coordinator"))
		require.Contains(cfg.Cron.ExpectedWorkflows, fmt.Sprintf("ingestor/blocks/%d", cfg.Tag.Stable))

		for _, child := range xapi.ChildCollections {
			for _, ingestor := range cfg.Workflows.Coordinator.Ingestors {
				// child collection should not have an ingestor config
				require.NotEqual(child, api.Collection(ingestor.Collection))
			}
		}

		for _, ingestor := range cfg.Workflows.Coordinator.Ingestors {
			if xapi.CollectionBlocksExtraDataByNumber == api.Collection(ingestor.Collection) {
				// only polygon allows indexing BlockExtraData
				require.Equal(common.Blockchain_BLOCKCHAIN_POLYGON, cfg.Blockchain())
			}
		}
	})
}

func TestParseConfigName(t *testing.T) {
	tests := []struct {
		configName string
		blockchain common.Blockchain
		network    common.Network
	}{
		{
			configName: "ethereum_mainnet",
			blockchain: common.Blockchain_BLOCKCHAIN_ETHEREUM,
			network:    common.Network_NETWORK_ETHEREUM_MAINNET,
		},
		{
			configName: "ethereum_goerli",
			blockchain: common.Blockchain_BLOCKCHAIN_ETHEREUM,
			network:    common.Network_NETWORK_ETHEREUM_GOERLI,
		},
		{
			configName: "bsc_mainnet",
			blockchain: common.Blockchain_BLOCKCHAIN_BSC,
			network:    common.Network_NETWORK_BSC_MAINNET,
		},
		{
			configName: "bsc_testnet",
			blockchain: common.Blockchain_BLOCKCHAIN_BSC,
			network:    common.Network_NETWORK_BSC_TESTNET,
		},
		{
			configName: "ethereum-mainnet",
			blockchain: common.Blockchain_BLOCKCHAIN_ETHEREUM,
			network:    common.Network_NETWORK_ETHEREUM_MAINNET,
		},
		{
			configName: "polygon-mainnet",
			blockchain: common.Blockchain_BLOCKCHAIN_POLYGON,
			network:    common.Network_NETWORK_POLYGON_MAINNET,
		},
	}
	for _, test := range tests {
		t.Run(test.configName, func(t *testing.T) {
			require := testutil.Require(t)

			actualBlockchain, actualNetwork, err := config.ParseConfigName(test.configName)
			require.NoError(err)
			require.Equal(test.blockchain, actualBlockchain)
			require.Equal(test.network, actualNetwork)
		})
	}
}

func TestConfigOverridingByEnvSettings(t *testing.T) {
	testapp.TestAllConfigs(t, func(t *testing.T, cfg *config.Config) {
		require := testutil.Require(t)

		expectedPrimaryEndpoints := []config.Endpoint{
			{
				Name:     "testCluster1",
				Url:      "testUrl1",
				User:     "testUser1",
				Password: "testPassword1",
				Weight:   1,
			},
			{
				Name:     "testCluster2",
				Url:      "testUrl2",
				User:     "testUser2",
				Password: "testPassword2",
				Weight:   2,
			},
		}

		jsonRpcPrimaryEndpoint := `
		{
			"endpoints": [
				{
					"name": "testCluster1",
					"url": "testUrl1",
					"user": "testUser1",
					"password": "testPassword1",
					"weight": 1
				},
				{
					"name": "testCluster2",
					"url": "testUrl2",
					"user": "testUser2",
					"password": "testPassword2",
					"weight": 2
				}
			]
		}
		`

		expectedValidatorEndpoints := []config.Endpoint{
			{
				Name:     "testCluster3",
				Url:      "testUrl3",
				User:     "testUser3",
				Password: "testPassword3",
				Weight:   1,
			},
			{
				Name:     "testCluster4",
				Url:      "testUrl4",
				User:     "testUser4",
				Password: "testPassword4",
				Weight:   2,
			},
		}

		jsonRpcValidatorEndpoint := `
		{
			"endpoints": [
				{
					"name": "testCluster3",
					"url": "testUrl3",
					"user": "testUser3",
					"password": "testPassword3",
					"weight": 1
				},
				{
					"name": "testCluster4",
					"url": "testUrl4",
					"user": "testUser4",
					"password": "testPassword4",
					"weight": 2
				}
			]
		}
		`
		err := os.Setenv("CHAINNODE_CHAIN_CLIENT_PRIMARY", jsonRpcPrimaryEndpoint)
		require.NoError(err)
		defer os.Unsetenv("CHAINNODE_CHAIN_CLIENT_PRIMARY")
		err = os.Setenv("CHAINNODE_CHAIN_CLIENT_VALIDATOR", jsonRpcValidatorEndpoint)
		require.NoError(err)
		defer os.Unsetenv("CHAINNODE_CHAIN_CLIENT_VALIDATOR")

		// Reload config using the env var.
		cfg, err = config.New(
			config.WithEnvironment(cfg.Env()),
			config.WithBlockchain(cfg.Blockchain()),
			config.WithNetwork(cfg.Network()),
		)
		require.NoError(err)
		require.Equal(expectedPrimaryEndpoints, cfg.Chain.Client.Primary.Endpoints,
			"config yml is likely broken since environment variable "+
				"'CHAINNODE_CHAIN_CLIENT_PRIMARY' no longer overrides the config values")
		require.Equal(expectedValidatorEndpoints, cfg.Chain.Client.Validator.Endpoints,
			"config yml is likely broken since environment variable "+
				"'CHAINNODE_CHAIN_CLIENT_VALIDATOR' no longer overrides the config values")
	})
}

func TestConfigOverridingByEnvSettings_UseFailover(t *testing.T) {
	testapp.TestAllConfigs(t, func(t *testing.T, cfg *config.Config) {
		require := testutil.Require(t)

		expectedEndpoints := []config.Endpoint{
			{
				Name:     "testCluster1",
				Url:      "testUrl1",
				User:     "testUser1",
				Password: "testPassword1",
				Weight:   1,
			},
		}

		expectedEndpointsFailover := []config.Endpoint{
			{
				Name:     "testCluster2",
				Url:      "testUrl2",
				User:     "testUser2",
				Password: "testPassword2",
				Weight:   2,
			},
		}

		jsonRpcEndpoint := `
		{
			"endpoints": [
				{
					"name": "testCluster1",
					"url": "testUrl1",
					"user": "testUser1",
					"password": "testPassword1",
					"weight": 1
				}
			],
			"endpoints_failover": [
				{
					"name": "testCluster2",
					"url": "testUrl2",
					"user": "testUser2",
					"password": "testPassword2",
					"weight": 2
				}
			],
			"use_failover": true
		}
		`
		err := os.Setenv("CHAINNODE_CHAIN_CLIENT_PRIMARY", jsonRpcEndpoint)
		require.NoError(err)
		defer os.Unsetenv("CHAINNODE_CHAIN_CLIENT_PRIMARY")

		// Reload config using the env var.
		cfg, err = config.New(
			config.WithEnvironment(cfg.Env()),
			config.WithBlockchain(cfg.Blockchain()),
			config.WithNetwork(cfg.Network()),
		)
		require.NoError(err)
		require.Equal(expectedEndpoints, cfg.Chain.Client.Primary.Endpoints)
		require.Equal(expectedEndpointsFailover, cfg.Chain.Client.Primary.EndpointsFailover)
		require.True(cfg.Chain.Client.Primary.UseFailover)
	})
}

func TestDeriveClientConfig(t *testing.T) {
	require := testutil.Require(t)

	expected := []struct {
		ServerName    string
		ServerAddress string
	}{
		{
			"chainnode-ethereum-ethereum-mainnet-local",
			"http://localhost:8000",
		},
		{
			"chainnode-ethereum-ethereum-goerli-local",
			"http://localhost:8000",
		},
		{
			"chainnode-polygon-polygon-mainnet-local",
			"http://localhost:8000",
		},
	}
	i := 0

	networksMap := map[common.Blockchain][]common.Network{
		common.Blockchain_BLOCKCHAIN_ETHEREUM: {
			common.Network_NETWORK_ETHEREUM_MAINNET,
			common.Network_NETWORK_ETHEREUM_GOERLI,
		},
		common.Blockchain_BLOCKCHAIN_POLYGON: {
			common.Network_NETWORK_POLYGON_MAINNET,
		},
	}

	envsMap := map[common.Blockchain][]config.Env{
		common.Blockchain_BLOCKCHAIN_ETHEREUM: {
			config.EnvLocal,
		},
		common.Blockchain_BLOCKCHAIN_POLYGON: {
			config.EnvLocal,
		},
	}

	for _, blockchain := range []common.Blockchain{
		common.Blockchain_BLOCKCHAIN_ETHEREUM,
		common.Blockchain_BLOCKCHAIN_POLYGON,
	} {
		envs, ok := envsMap[blockchain]
		require.True(ok)
		for _, env := range envs {
			networks, ok := networksMap[blockchain]
			require.True(ok)
			for _, network := range networks {
				cfg, err := config.New(
					config.WithEnvironment(env),
					config.WithBlockchain(blockchain),
					config.WithNetwork(network),
				)
				require.NoError(err)
				cfg.Chain.Client.DeriveConfig(cfg)
				require.Equal(expected[i].ServerName, cfg.Chain.Client.ServerName)
				require.Equal(expected[i].ServerAddress, cfg.Chain.Client.ServerAddress)
				require.Equal(config.ServerHandle, cfg.Chain.Client.ServerHandle)
				i++
			}
		}
	}
}

func TestIsCollectionSynchronized(t *testing.T) {
	require := testutil.Require(t)

	cfg, err := config.New()
	require.NoError(err)
	cfg.Workflows.Coordinator.Ingestors = []config.IngestorConfig{
		{
			Collection:   "foo",
			Synchronized: true,
		},
		{
			Collection:   "bar",
			Synchronized: false,
		},
	}

	require.True(cfg.IsCollectionSynchronized("foo"))
	require.False(cfg.IsCollectionSynchronized("bar"))
	require.False(cfg.IsCollectionSynchronized("foo1"))
}

func TestGetCommonTags(t *testing.T) {
	require := testutil.Require(t)

	cfg, err := config.New()
	require.NoError(err)
	require.Equal(map[string]string{
		"blockchain": "ethereum",
		"network":    "ethereum-mainnet",
		"tier":       "1",
	}, cfg.GetCommonTags())
}
