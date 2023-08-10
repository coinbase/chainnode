package integration

import (
	"context"
	"encoding/json"
	"math"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/testsuite"
	"go.uber.org/fx"
	"go.uber.org/zap"

	"github.com/coinbase/chainnode/internal/api"
	xapi "github.com/coinbase/chainnode/internal/api/ethereum"
	"github.com/coinbase/chainnode/internal/cadence"
	"github.com/coinbase/chainnode/internal/clients"
	"github.com/coinbase/chainnode/internal/config"
	"github.com/coinbase/chainnode/internal/controller"
	"github.com/coinbase/chainnode/internal/storage"
	"github.com/coinbase/chainnode/internal/utils/testapp"
	"github.com/coinbase/chainnode/internal/utils/testutil"
	"github.com/coinbase/chainnode/internal/workflow"
)

type (
	CoordinatorTestSuite struct {
		suite.Suite
		testsuite.WorkflowTestSuite
		env          *cadence.TestEnv
		coordinator  *workflow.Coordinator
		checkpointer controller.Checkpointer
		storage      storage.EthereumStorage
		app          testapp.TestApp
	}

	blockLite struct {
		Number       hexutil.Uint64 `json:"number"`
		Transactions []struct {
			Hash string `json:"hash"`
		} `json:"transactions"`
	}
)

const (
	idleTimeout = 5 * time.Minute
)

func TestIntegrationCoordinatorTestSuite(t *testing.T) {
	suite.Run(t, new(CoordinatorTestSuite))
}

func (s *CoordinatorTestSuite) SetupTest() {
	require := s.Require()

	cfg, err := config.New()
	require.NoError(err)

	coordinatorCfg := &cfg.Workflows.Coordinator
	coordinatorCfg.InterruptTimeout = time.Duration(math.MaxInt64)
	coordinatorCfg.Ingestors = make([]config.IngestorConfig, len(xapi.ParentCollections))
	for i, collection := range xapi.ParentCollections {
		coordinatorCfg.Ingestors[i] = config.IngestorConfig{
			Collection:   collection.String(),
			Synchronized: true,
		}
	}

	ingestorCfg := &cfg.Workflows.Ingestor
	ingestorCfg.BatchSize = 4
	ingestorCfg.MiniBatchSize = 2
	ingestorCfg.Parallelism = 2

	s.env = cadence.NewTestEnv(s)
	s.env.SetTestTimeout(idleTimeout)

	var deps struct {
		fx.In
		Coordinator  *workflow.Coordinator
		Checkpointer controller.Checkpointer
		Storage      storage.EthereumStorage
	}
	s.app = testapp.New(
		s.T(),
		testapp.WithFunctional(),
		testapp.WithConfig(cfg),
		cadence.WithTestEnv(s.env),
		clients.Module,
		controller.Module,
		storage.Module,
		workflow.Module,
		fx.Populate(&deps),
	)
	s.coordinator = deps.Coordinator
	s.checkpointer = deps.Checkpointer
	s.storage = deps.Storage
}

func (s *CoordinatorTestSuite) TearDownTest() {
	if s.app != nil {
		s.app.Close()
	}
}

func (s *CoordinatorTestSuite) TestStartFromScratch() {
	const (
		maxEvents = int64(8)
		tag       = uint32(2)
		eventTag  = uint32(1)
	)

	require := testutil.Require(s.T())

	ctx := context.Background()
	logger := s.app.Logger()

	_, err := s.coordinator.Execute(ctx, &workflow.CoordinatorRequest{
		Tag:       tag,
		EventTag:  eventTag,
		MaxEvents: maxEvents,
		Sequence:  1000000,
	})
	require.NoError(err)

	for _, collection := range xapi.ParentCollections {
		checkpoint, err := s.checkpointer.Get(ctx, collection, tag)
		require.NoError(err)

		logger.Info(
			"local checkpoint",
			zap.String("collection", collection.String()),
			zap.Reflect("checkpoint", checkpoint),
		)
		require.Equal(collection, checkpoint.Collection)
		require.LessOrEqual(maxEvents, int64(checkpoint.Sequence))
	}

	earliestCheckpoint, err := s.checkpointer.Get(ctx, api.CollectionEarliestCheckpoint, tag)
	require.NoError(err)
	logger.Info(
		"earliest checkpoint",
		zap.Reflect("checkpoint", earliestCheckpoint),
	)
	require.Equal(api.CollectionEarliestCheckpoint, earliestCheckpoint.Collection)
	require.Equal(int64(1), int64(earliestCheckpoint.Sequence))
	require.LessOrEqual(uint64(0), earliestCheckpoint.Height)

	latestCheckpoint, err := s.checkpointer.Get(ctx, api.CollectionLatestCheckpoint, tag)
	require.NoError(err)
	logger.Info(
		"latest checkpoint",
		zap.Reflect("checkpoint", latestCheckpoint),
	)
	require.Equal(api.CollectionLatestCheckpoint, latestCheckpoint.Collection)
	require.Less(uint64(0), latestCheckpoint.Height)
	require.LessOrEqual(maxEvents, int64(latestCheckpoint.Sequence))

	height := latestCheckpoint.Height
	sequence := latestCheckpoint.Sequence

	block, err := s.storage.GetBlock(ctx, tag, height, sequence)
	require.NoError(err)

	var blockHeader blockLite
	err = json.Unmarshal(block.Data, &blockHeader)
	require.NoError(err)
	require.Equal(height, uint64(blockHeader.Number))
	require.NotEmpty(blockHeader.Transactions)
	txHash := blockHeader.Transactions[0].Hash

	_, err = s.storage.GetLogsV2(ctx, tag, height, sequence)
	require.NoError(err)

	_, err = s.storage.GetTransactionByHash(ctx, tag, txHash, sequence)
	require.NoError(err)

	_, err = s.storage.GetTransactionReceipt(ctx, tag, txHash, sequence)
	require.NoError(err)

	_, err = s.storage.GetTraceByHash(ctx, tag, block.Hash, sequence)
	require.NoError(err)

	_, err = s.storage.GetTraceByNumber(ctx, tag, height, sequence)
	require.NoError(err)

	_, err = s.storage.GetBlockByHash(ctx, tag, block.Hash, sequence)
	require.NoError(err)

	_, err = s.storage.GetBlockByHashWithoutFullTx(ctx, tag, block.Hash, sequence)
	require.NoError(err)

	_, err = s.storage.GetBlockExtraDataByNumber(ctx, tag, height, sequence)
	require.NoError(err)
}
