package integration

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"

	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	chainstorageapi "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"

	"github.com/coinbase/chainnode/internal/api"
	"github.com/coinbase/chainnode/internal/clients/chainstorage"
	"github.com/coinbase/chainnode/internal/utils/testapp"
	"github.com/coinbase/chainnode/internal/utils/testutil"
)

type ChainStorageTestSuite struct {
	suite.Suite
	app    testapp.TestApp
	client chainstorage.Client
	parser chainstorage.Parser
}

const (
	testMinHeight    = uint64(14_000_000)
	testSequence     = api.Sequence(10_000)
	testMaxNumEvents = 5
)

func TestIntegrationChainStorageTestSuite(t *testing.T) {
	suite.Run(t, new(ChainStorageTestSuite))
}

func (s *ChainStorageTestSuite) SetupTest() {
	s.app = testapp.New(
		s.T(),
		testapp.WithFunctional(),
		chainstorage.Module,
		fx.Populate(&s.client),
		fx.Populate(&s.parser),
	)
}

func (s *ChainStorageTestSuite) TearDownTest() {
	if s.app != nil {
		s.app.Close()
	}
}

func (s *ChainStorageTestSuite) TestGetLatestBlock() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	height, err := s.client.GetLatestBlock(ctx)
	require.NoError(err)
	require.Less(testMinHeight, height)
}

func (s *ChainStorageTestSuite) TestGetBlock() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	events, err := s.client.GetChainEvents(ctx, &chainstorageapi.GetChainEventsRequest{
		SequenceNum:  testSequence.AsInt64(),
		MaxNumEvents: testMaxNumEvents,
		EventTag:     s.app.Config().Workflows.Coordinator.EventTag,
	})
	require.NoError(err)
	require.Equal(testMaxNumEvents, len(events))
	for _, event := range events {
		blockID := event.Block
		rawBlock, err := s.client.GetBlockWithTag(ctx, blockID.Tag, blockID.Height, blockID.Hash)
		require.NoError(err)

		nativeBlock, err := s.parser.ParseNativeBlock(ctx, rawBlock)
		require.NoError(err)
		require.Equal(common.Blockchain_BLOCKCHAIN_ETHEREUM, nativeBlock.Blockchain)
		require.Equal(common.Network_NETWORK_ETHEREUM_MAINNET, nativeBlock.Network)
		require.Equal(blockID.Tag, nativeBlock.Tag)
		require.Equal(blockID.Height, nativeBlock.Height)
		require.Equal(blockID.Hash, nativeBlock.Hash)
		require.NotNil(nativeBlock.GetEthereum())
	}
}
