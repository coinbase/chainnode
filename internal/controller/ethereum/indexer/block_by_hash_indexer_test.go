package indexer

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"

	chainstorageapi "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"

	"github.com/coinbase/chainnode/internal/api"
	xapi "github.com/coinbase/chainnode/internal/api/ethereum"
	"github.com/coinbase/chainnode/internal/controller/internal"
	"github.com/coinbase/chainnode/internal/storage"
	storagemocks "github.com/coinbase/chainnode/internal/storage/mocks"
	"github.com/coinbase/chainnode/internal/utils/fixtures"
	"github.com/coinbase/chainnode/internal/utils/testapp"
	"github.com/coinbase/chainnode/internal/utils/testutil"
)

type BlockByHashIndexerTestSuite struct {
	suite.Suite
	ctrl    *gomock.Controller
	indexer internal.Indexer
	storage *storagemocks.MockEthereumStorage
	app     testapp.TestApp
}

const (
	tag      = uint32(2)
	blockTag = uint32(1)
	eventTag = uint32(3)
	sequence = api.Sequence(123)
)

func TestBlockByHashIndexerTestSuite(t *testing.T) {
	suite.Run(t, new(BlockByHashIndexerTestSuite))
}

func (s *BlockByHashIndexerTestSuite) SetupTest() {
	var deps struct {
		fx.In
		Indexer internal.Indexer `name:"ethereum/blockByHash"`
	}

	s.ctrl = gomock.NewController(s.T())
	s.storage = storagemocks.NewMockEthereumStorage(s.ctrl)
	s.app = testapp.New(
		s.T(),
		Module,
		fx.Provide(func() storage.EthereumStorage { return s.storage }),
		fx.Populate(&deps),
	)

	s.indexer = deps.Indexer
}

func (s *BlockByHashIndexerTestSuite) TearDownTest() {
	s.app.Close()
}

func (s *BlockByHashIndexerTestSuite) TestIndex() {
	require := testutil.Require(s.T())

	event := testutil.MakeBlockEvent(sequence, blockTag, eventTag)
	height := event.Block.Height
	header, err := fixtures.ReadJson("controller/ethereum/eth_header.json")
	require.NoError(err)
	block := &chainstorageapi.Block{
		Metadata: testutil.MakeBlockMetadata(height, blockTag),
		Blobdata: &chainstorageapi.Block_Ethereum{
			Ethereum: &chainstorageapi.EthereumBlobdata{
				Header: header,
			},
		},
	}
	hash := block.GetMetadata().Hash
	blockHeader := block.GetEthereum().GetHeader()

	blockTime := testutil.MustTime("2020-11-24T16:07:21Z")
	expectedBlockByHash := xapi.NewBlock(tag, height, hash, sequence, blockTime, blockHeader)
	s.storage.EXPECT().PersistBlockByHash(gomock.Any(), expectedBlockByHash).Return(nil)

	headerWithoutFullTx, err := fixtures.ReadJson("controller/ethereum/eth_header_without_full_tx.json")
	require.NoError(err)
	expectedBlockByHashWithoutFullTx := xapi.NewBlock(tag, height, hash, sequence, blockTime, headerWithoutFullTx)
	s.storage.EXPECT().PersistBlockByHashWithoutFullTx(gomock.Any(), expectedBlockByHashWithoutFullTx).Return(nil)

	err = s.indexer.Index(context.Background(), tag, event, block)
	require.NoError(err)
}

func (s *BlockByHashIndexerTestSuite) TestIndex_NoBlockTime() {
	require := testutil.Require(s.T())

	event := testutil.MakeBlockEvent(sequence, blockTag, eventTag)
	height := event.Block.Height
	header, err := fixtures.ReadJson("controller/ethereum/eth_header_no_block_time.json")
	require.NoError(err)

	block := &chainstorageapi.Block{
		Metadata: testutil.MakeBlockMetadata(height, blockTag),
		Blobdata: &chainstorageapi.Block_Ethereum{
			Ethereum: &chainstorageapi.EthereumBlobdata{
				Header: header,
			},
		},
	}
	hash := block.GetMetadata().Hash
	blockHeader := block.GetEthereum().GetHeader()

	expectedBlockByHash := xapi.NewBlock(tag, height, hash, sequence, time.Time{}, blockHeader)
	s.storage.EXPECT().PersistBlockByHash(gomock.Any(), expectedBlockByHash).Return(nil)

	headerWithoutFullTx, err := fixtures.ReadJson("controller/ethereum/eth_header_no_block_time_without_full_tx.json")
	require.NoError(err)

	expectedBlock := xapi.NewBlock(tag, height, hash, sequence, time.Time{}, headerWithoutFullTx)
	s.storage.EXPECT().PersistBlockByHashWithoutFullTx(gomock.Any(), expectedBlock).Return(nil)

	err = s.indexer.Index(context.Background(), tag, event, block)
	require.NoError(err)
}

func (s *BlockByHashIndexerTestSuite) TestIndex_InvalidBlockTime() {
	require := testutil.Require(s.T())

	event := testutil.MakeBlockEvent(sequence, blockTag, eventTag)
	height := event.Block.Height
	header, err := fixtures.ReadJson("controller/ethereum/eth_header_invalid_block_time.json")
	require.NoError(err)

	block := &chainstorageapi.Block{
		Metadata: testutil.MakeBlockMetadata(height, blockTag),
		Blobdata: &chainstorageapi.Block_Ethereum{
			Ethereum: &chainstorageapi.EthereumBlobdata{
				Header: header,
			},
		},
	}

	err = s.indexer.Index(context.Background(), tag, event, block)
	require.Error(err)
}
