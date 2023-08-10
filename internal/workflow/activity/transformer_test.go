package activity

import (
	"context"
	"sync"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/testsuite"
	"go.uber.org/fx"

	chainstorageapi "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"

	"github.com/coinbase/chainnode/internal/api"
	xapi "github.com/coinbase/chainnode/internal/api/ethereum"
	"github.com/coinbase/chainnode/internal/cadence"
	"github.com/coinbase/chainnode/internal/clients/chainstorage"
	chainstoragemocks "github.com/coinbase/chainnode/internal/clients/chainstorage/mocks"
	"github.com/coinbase/chainnode/internal/controller"
	controllermocks "github.com/coinbase/chainnode/internal/controller/mocks"
	"github.com/coinbase/chainnode/internal/storage"
	"github.com/coinbase/chainnode/internal/utils/testapp"
	"github.com/coinbase/chainnode/internal/utils/testutil"
)

type TransformerTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
	env                *cadence.TestEnv
	ctrl               *gomock.Controller
	chainStorageClient *chainstoragemocks.MockClient
	controller         *controllermocks.MockController
	indexer            *controllermocks.MockIndexer
	transformer        *Transformer
	app                testapp.TestApp
}

func TestTransformerTestSuite(t *testing.T) {
	suite.Run(t, new(TransformerTestSuite))
}

func (s *TransformerTestSuite) SetupTest() {
	s.env = cadence.NewTestActivityEnv(s)
	s.ctrl = gomock.NewController(s.T())
	s.chainStorageClient = chainstoragemocks.NewMockClient(s.ctrl)
	s.controller = controllermocks.NewMockController(s.ctrl)
	s.indexer = controllermocks.NewMockIndexer(s.ctrl)
	s.app = testapp.New(
		s.T(),
		Module,
		cadence.WithTestEnv(s.env),
		storage.Module,
		storage.WithEmptyStorage(),
		fx.Provide(func() chainstorage.Client { return s.chainStorageClient }),
		fx.Provide(func() controller.Controller { return s.controller }),
		fx.Populate(&s.transformer),
	)
}

func (s *TransformerTestSuite) TearDownTest() {
	s.app.Close()
	s.ctrl.Finish()
	s.env.AssertExpectations(s.T())
}

func (s *TransformerTestSuite) TestTransformer() {
	const (
		tag        = uint32(2)
		blockTag   = uint32(1)
		eventTag   = uint32(3)
		sequence   = api.Sequence(10100)
		numEvents  = 10
		collection = xapi.CollectionBlocks
	)

	require := testutil.Require(s.T())

	events := testutil.MakeBlockEvents(sequence, numEvents, blockTag, eventTag)
	startHeight := events[0].Block.Height
	endHeight := events[numEvents-1].Block.Height
	blocks := testutil.MakeBlocksFromStartHeight(startHeight, numEvents, blockTag)
	blocksByHeight := make(map[uint64]*api.Block, len(blocks))
	indexed := sync.Map{}
	for _, block := range blocks {
		blocksByHeight[block.Metadata.Height] = block
	}
	s.chainStorageClient.EXPECT().GetBlockWithTag(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, tag uint32, height uint64, hash string) (*api.Block, error) {
			require.Equal(blockTag, tag)
			block, ok := blocksByHeight[height]
			require.True(ok)
			require.Equal(block.Metadata.Hash, hash)
			return block, nil
		}).AnyTimes()
	s.controller.EXPECT().Indexer(collection).Return(s.indexer, nil).AnyTimes()
	s.indexer.EXPECT().Index(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, tag uint32, event *api.Event, block *api.Block) error {
			_, ok := blocksByHeight[block.Metadata.Height]
			require.True(ok)
			require.Equal(event.Block.Tag, block.Metadata.Tag)
			require.Equal(event.Block.Height, block.Metadata.Height)
			require.Equal(event.Block.Hash, block.Metadata.Hash)
			_, ok = indexed.LoadOrStore(event.Block.Height, struct{}{})
			require.False(ok)
			return nil
		}).AnyTimes()

	ctx := s.env.BackgroundContext()
	_, err := s.transformer.Execute(ctx, &TransformerRequest{
		Tag:        tag,
		Collection: collection,
		Events:     events,
	})
	require.NoError(err)

	for i := startHeight; i < endHeight; i++ {
		_, ok := indexed.Load(uint64(i))
		require.True(ok, "height %v not indexed", i)
	}
}

func (s *TransformerTestSuite) TestTransformerWithParallelism() {
	const (
		tag         = uint32(2)
		blockTag    = uint32(1)
		eventTag    = uint32(3)
		sequence    = api.Sequence(10100)
		numEvents   = 10
		collection  = xapi.CollectionBlocks
		parallelism = 4
	)

	require := testutil.Require(s.T())

	events := testutil.MakeBlockEvents(sequence, numEvents, blockTag, eventTag)
	startHeight := events[0].Block.Height
	endHeight := events[numEvents-1].Block.Height
	blocks := testutil.MakeBlocksFromStartHeight(startHeight, numEvents, blockTag)
	blocksByHeight := make(map[uint64]*api.Block, len(blocks))
	indexed := sync.Map{}
	for _, block := range blocks {
		blocksByHeight[block.Metadata.Height] = block
	}
	s.chainStorageClient.EXPECT().GetBlockWithTag(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, tag uint32, height uint64, hash string) (*api.Block, error) {
			require.Equal(blockTag, tag)
			block, ok := blocksByHeight[height]
			require.True(ok)
			require.Equal(block.Metadata.Hash, hash)
			return block, nil
		}).AnyTimes()
	s.controller.EXPECT().Indexer(collection).Return(s.indexer, nil).AnyTimes()
	s.indexer.EXPECT().Index(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, tag uint32, event *api.Event, block *api.Block) error {
			_, ok := blocksByHeight[block.Metadata.Height]
			require.True(ok)
			require.Equal(event.Block.Tag, block.Metadata.Tag)
			require.Equal(event.Block.Height, block.Metadata.Height)
			require.Equal(event.Block.Hash, block.Metadata.Hash)
			_, ok = indexed.LoadOrStore(event.Block.Height, struct{}{})
			require.False(ok)
			return nil
		}).AnyTimes()

	ctx := s.env.BackgroundContext()
	_, err := s.transformer.Execute(ctx, &TransformerRequest{
		Tag:         tag,
		Collection:  collection,
		Events:      events,
		Parallelism: parallelism,
	})
	require.NoError(err)

	for i := startHeight; i < endHeight; i++ {
		_, ok := indexed.Load(uint64(i))
		require.True(ok, "height %v not indexed", i)
	}
}

func (s *TransformerTestSuite) TestTransformer_NoIndexDelay() {
	const (
		tag        = uint32(2)
		blockTag   = uint32(1)
		eventTag   = uint32(3)
		sequence   = api.Sequence(10100)
		numEvents  = 10
		collection = xapi.CollectionBlocks
	)

	require := testutil.Require(s.T())

	events := testutil.MakeBlockEvents(sequence, numEvents, blockTag, eventTag)
	startHeight := events[0].Block.Height
	endHeight := events[numEvents-1].Block.Height
	blocks := testutil.MakeBlocksFromStartHeight(startHeight, numEvents, blockTag)
	blocksByHeight := make(map[uint64]*api.Block, len(blocks))
	indexed := sync.Map{}
	for _, block := range blocks {
		blocksByHeight[block.Metadata.Height] = block
	}
	s.chainStorageClient.EXPECT().GetBlockWithTag(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, tag uint32, height uint64, hash string) (*api.Block, error) {
			require.Equal(blockTag, tag)
			block, ok := blocksByHeight[height]
			require.True(ok)
			require.Equal(block.Metadata.Hash, hash)
			return block, nil
		}).AnyTimes()
	s.controller.EXPECT().Indexer(collection).Return(s.indexer, nil).AnyTimes()
	s.indexer.EXPECT().Index(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, tag uint32, event *api.Event, block *api.Block) error {
			_, ok := blocksByHeight[block.Metadata.Height]
			require.True(ok)
			require.Equal(event.Block.Tag, block.Metadata.Tag)
			require.Equal(event.Block.Height, block.Metadata.Height)
			require.Equal(event.Block.Hash, block.Metadata.Hash)
			_, ok = indexed.LoadOrStore(event.Block.Height, struct{}{})
			require.False(ok)
			return nil
		}).AnyTimes()

	ctx := s.env.BackgroundContext()
	_, err := s.transformer.Execute(ctx, &TransformerRequest{
		Tag:        tag,
		Collection: collection,
		Events:     events,
	})
	require.NoError(err)

	for i := startHeight; i < endHeight; i++ {
		_, ok := indexed.Load(uint64(i))
		require.True(ok, "height %v not indexed", i)
	}
}

func (s *TransformerTestSuite) TestFilterNoneAddedBlockEvent() {
	const (
		tag        = uint32(2)
		eventTag   = uint32(3)
		blockTag   = uint32(1)
		sequence   = api.Sequence(10100)
		numEvents  = 2
		collection = xapi.CollectionBlocks
	)

	require := testutil.Require(s.T())
	events := testutil.MakeBlockEvents(sequence, numEvents, blockTag, eventTag)
	for _, event := range events {
		event.Type = chainstorageapi.BlockchainEvent_UNKNOWN
	}

	ctx := s.env.BackgroundContext()
	_, err := s.transformer.Execute(ctx, &TransformerRequest{
		Tag:        tag,
		Collection: collection,
		Events:     events,
	})
	require.NoError(err)
}

func (s *TransformerTestSuite) TestFilterBlockSkippedEvent() {
	const (
		tag        = uint32(2)
		eventTag   = uint32(3)
		blockTag   = uint32(1)
		sequence   = api.Sequence(10100)
		numEvents  = 2
		collection = xapi.CollectionBlocks
	)

	require := testutil.Require(s.T())
	events := testutil.MakeBlockEvents(sequence, numEvents, blockTag, eventTag, testutil.WithBlockSkipped())

	ctx := s.env.BackgroundContext()
	_, err := s.transformer.Execute(ctx, &TransformerRequest{
		Tag:        tag,
		Collection: collection,
		Events:     events,
	})
	require.NoError(err)
}
