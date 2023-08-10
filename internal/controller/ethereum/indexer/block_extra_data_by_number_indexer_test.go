package indexer

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"

	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	chainstorageapi "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"

	xapi "github.com/coinbase/chainnode/internal/api/ethereum"
	"github.com/coinbase/chainnode/internal/config"
	"github.com/coinbase/chainnode/internal/controller/internal"
	"github.com/coinbase/chainnode/internal/storage"
	storagemocks "github.com/coinbase/chainnode/internal/storage/mocks"
	"github.com/coinbase/chainnode/internal/utils/fixtures"
	"github.com/coinbase/chainnode/internal/utils/testapp"
	"github.com/coinbase/chainnode/internal/utils/testutil"
)

type BlockExtraDataByNumberIndexerTestSuite struct {
	suite.Suite
	ctrl    *gomock.Controller
	indexer internal.Indexer
	storage *storagemocks.MockEthereumStorage
	app     testapp.TestApp
}

func TestBlockExtraDataByNumberIndexerTestSuite(t *testing.T) {
	suite.Run(t, new(BlockExtraDataByNumberIndexerTestSuite))
}

func (s *BlockExtraDataByNumberIndexerTestSuite) SetupTest() {
	var deps struct {
		fx.In
		Indexer internal.Indexer `name:"ethereum/blockExtraDataByNumber"`
	}

	s.ctrl = gomock.NewController(s.T())
	s.storage = storagemocks.NewMockEthereumStorage(s.ctrl)
	cfg, err := config.New(
		config.WithBlockchain(common.Blockchain_BLOCKCHAIN_POLYGON),
		config.WithNetwork(common.Network_NETWORK_POLYGON_MAINNET),
	)
	s.Require().NoError(err)
	s.app = testapp.New(
		s.T(),
		Module,
		testapp.WithConfig(cfg),
		fx.Provide(func() storage.EthereumStorage { return s.storage }),
		fx.Populate(&deps),
	)

	s.indexer = deps.Indexer
}

func (s *BlockExtraDataByNumberIndexerTestSuite) TearDownTest() {
	s.app.Close()
}

func (s *BlockExtraDataByNumberIndexerTestSuite) TestIndex() {
	require := testutil.Require(s.T())

	event := testutil.MakeBlockEvent(sequence, blockTag, eventTag)
	height := event.Block.Height
	header := fixtures.MustReadJson("controller/ethereum/eth_header.json")

	author := []byte("0x742d13f0b2a19c823bdd362b16305e4704b97a38")
	block := &chainstorageapi.Block{
		Metadata: testutil.MakeBlockMetadata(height, blockTag),
		Blobdata: &chainstorageapi.Block_Ethereum{
			Ethereum: &chainstorageapi.EthereumBlobdata{
				Header: header,
				ExtraData: &chainstorageapi.EthereumBlobdata_Polygon{
					Polygon: &chainstorageapi.PolygonExtraData{
						Author: author,
					},
				},
			},
		},
	}
	hash := block.GetMetadata().Hash

	blockTime := testutil.MustTime("2020-11-24T16:07:21Z")
	s.storage.EXPECT().PersistBlockExtraDataByNumber(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, data *xapi.BlockExtraData) error {
			var extraData chainstorageapi.PolygonExtraData
			err := proto.Unmarshal(data.Data, &extraData)
			require.NoError(err)
			require.Equal(author, extraData.Author)

			require.Equal(hash, data.Hash)
			require.Equal(height, data.Height)
			require.Equal(tag, data.Tag)
			require.Equal(sequence, data.Sequence)
			require.Equal(blockTime, data.BlockTime)
			return nil
		})

	err := s.indexer.Index(context.Background(), tag, event, block)
	require.NoError(err)
}

func (s *BlockExtraDataByNumberIndexerTestSuite) TestIndex_NoExtraData() {
	require := testutil.Require(s.T())

	event := testutil.MakeBlockEvent(sequence, blockTag, eventTag)
	height := event.Block.Height
	header := fixtures.MustReadJson("controller/ethereum/eth_header.json")

	block := &chainstorageapi.Block{
		Metadata: testutil.MakeBlockMetadata(height, blockTag),
		Blobdata: &chainstorageapi.Block_Ethereum{
			Ethereum: &chainstorageapi.EthereumBlobdata{
				Header: header,
			},
		},
	}
	hash := block.GetMetadata().Hash
	blockTime := testutil.MustTime("2020-11-24T16:07:21Z")
	expected := xapi.NewBlockExtraData(tag, height, hash, sequence, blockTime, nil)
	s.storage.EXPECT().PersistBlockExtraDataByNumber(gomock.Any(), expected).Return(nil)

	err := s.indexer.Index(context.Background(), tag, event, block)
	require.NoError(err)
}

func (s *BlockExtraDataByNumberIndexerTestSuite) TestIndex_NoBlockTime() {
	require := testutil.Require(s.T())

	event := testutil.MakeBlockEvent(sequence, blockTag, eventTag)
	height := event.Block.Height
	header := fixtures.MustReadJson("controller/ethereum/eth_header_no_block_time.json")

	author := []byte("0x742d13f0b2a19c823bdd362b16305e4704b97a38")
	block := &chainstorageapi.Block{
		Metadata: testutil.MakeBlockMetadata(height, blockTag),
		Blobdata: &chainstorageapi.Block_Ethereum{
			Ethereum: &chainstorageapi.EthereumBlobdata{
				Header: header,
				ExtraData: &chainstorageapi.EthereumBlobdata_Polygon{
					Polygon: &chainstorageapi.PolygonExtraData{
						Author: author,
					},
				},
			},
		},
	}
	hash := block.GetMetadata().Hash
	s.storage.EXPECT().PersistBlockExtraDataByNumber(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, data *xapi.BlockExtraData) error {
			var extraData chainstorageapi.PolygonExtraData
			err := proto.Unmarshal(data.Data, &extraData)
			require.NoError(err)
			require.Equal(author, extraData.Author)

			require.Equal(hash, data.Hash)
			require.Equal(height, data.Height)
			require.Equal(tag, data.Tag)
			require.Equal(sequence, data.Sequence)
			require.Equal(time.Time{}, data.BlockTime)
			return nil
		})

	err := s.indexer.Index(context.Background(), tag, event, block)
	require.NoError(err)
}

func (s *BlockExtraDataByNumberIndexerTestSuite) TestIndex_InvalidBlockTime() {
	require := testutil.Require(s.T())

	event := testutil.MakeBlockEvent(sequence, blockTag, eventTag)
	height := event.Block.Height
	header := fixtures.MustReadJson("controller/ethereum/eth_header_invalid_block_time.json")

	author := []byte("0x742d13f0b2a19c823bdd362b16305e4704b97a38")
	block := &chainstorageapi.Block{
		Metadata: testutil.MakeBlockMetadata(height, blockTag),
		Blobdata: &chainstorageapi.Block_Ethereum{
			Ethereum: &chainstorageapi.EthereumBlobdata{
				Header: header,
				ExtraData: &chainstorageapi.EthereumBlobdata_Polygon{
					Polygon: &chainstorageapi.PolygonExtraData{
						Author: author,
					},
				},
			},
		},
	}

	err := s.indexer.Index(context.Background(), tag, event, block)
	require.Error(err)
}
