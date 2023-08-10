package indexer

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"

	chainstorageapi "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"

	xapi "github.com/coinbase/chainnode/internal/api/ethereum"
	"github.com/coinbase/chainnode/internal/controller/internal"
	"github.com/coinbase/chainnode/internal/storage"
	storagemocks "github.com/coinbase/chainnode/internal/storage/mocks"
	"github.com/coinbase/chainnode/internal/utils/fixtures"
	"github.com/coinbase/chainnode/internal/utils/testapp"
	"github.com/coinbase/chainnode/internal/utils/testutil"
)

type transactionReceiptIndexerTestSuite struct {
	suite.Suite
	ctrl    *gomock.Controller
	indexer internal.Indexer
	storage *storagemocks.MockEthereumStorage
	app     testapp.TestApp
}

func TestTransactionReceiptIndexerTestSuite(t *testing.T) {
	suite.Run(t, new(transactionReceiptIndexerTestSuite))
}

func (s *transactionReceiptIndexerTestSuite) SetupTest() {
	var deps struct {
		fx.In
		Indexer internal.Indexer `name:"ethereum/transactionReceipt"`
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

func (s *transactionReceiptIndexerTestSuite) TearDownTest() {
	s.app.Close()
}

func (s *transactionReceiptIndexerTestSuite) TestIndex() {
	require := testutil.Require(s.T())

	event := testutil.MakeBlockEvent(sequence, blockTag, eventTag)
	height := event.Block.Height
	header, err := fixtures.ReadJson("controller/ethereum/eth_header.json")
	require.NoError(err)
	receipt1, err := fixtures.ReadJson("controller/ethereum/eth_transactionreceipt_1.json")
	require.NoError(err)
	receipt2, err := fixtures.ReadJson("controller/ethereum/eth_transactionreceipt_2.json")
	require.NoError(err)
	block := &chainstorageapi.Block{
		Metadata: testutil.MakeBlockMetadata(height, blockTag),
		Blobdata: &chainstorageapi.Block_Ethereum{
			Ethereum: &chainstorageapi.EthereumBlobdata{
				Header:              header,
				TransactionReceipts: [][]byte{receipt1, receipt2},
			},
		},
	}

	blockTime := testutil.MustTime("2020-11-24T16:07:21Z")
	expectedReceipts := []*xapi.TransactionReceipt{
		{
			Tag:       tag,
			Hash:      "0xe67071db25331ea3a92a4e28b516c95f2d5b62b68329b70386c19e00807f51d8",
			Sequence:  sequence,
			Data:      receipt1,
			BlockTime: blockTime,
		},
		{
			Tag:       tag,
			Hash:      "0x8aa066cca57271b696873384afbe65d99e4aaa0ddc5ccc2c90da94a45b889fae",
			Sequence:  sequence,
			Data:      receipt2,
			BlockTime: blockTime,
		},
	}

	s.storage.EXPECT().PersistTransactionReceipts(gomock.Any(), expectedReceipts).Return(nil)

	err = s.indexer.Index(context.Background(), tag, event, block)
	require.NoError(err)
}

func (s *transactionReceiptIndexerTestSuite) TestIndex_NoBlockTime() {
	require := testutil.Require(s.T())

	event := testutil.MakeBlockEvent(sequence, blockTag, eventTag)
	height := event.Block.Height
	header, err := fixtures.ReadJson("controller/ethereum/eth_header_no_block_time.json")
	require.NoError(err)
	receipt1, err := fixtures.ReadJson("controller/ethereum/eth_transactionreceipt_1.json")
	require.NoError(err)
	receipt2, err := fixtures.ReadJson("controller/ethereum/eth_transactionreceipt_2.json")
	require.NoError(err)
	block := &chainstorageapi.Block{
		Metadata: testutil.MakeBlockMetadata(height, blockTag),
		Blobdata: &chainstorageapi.Block_Ethereum{
			Ethereum: &chainstorageapi.EthereumBlobdata{
				Header:              header,
				TransactionReceipts: [][]byte{receipt1, receipt2},
			},
		},
	}
	expectedReceipts := []*xapi.TransactionReceipt{
		{
			Tag:      tag,
			Hash:     "0xe67071db25331ea3a92a4e28b516c95f2d5b62b68329b70386c19e00807f51d8",
			Sequence: sequence,
			Data:     receipt1,
		},
		{
			Tag:      tag,
			Hash:     "0x8aa066cca57271b696873384afbe65d99e4aaa0ddc5ccc2c90da94a45b889fae",
			Sequence: sequence,
			Data:     receipt2,
		},
	}

	s.storage.EXPECT().PersistTransactionReceipts(gomock.Any(), expectedReceipts).Return(nil)

	err = s.indexer.Index(context.Background(), tag, event, block)
	require.NoError(err)
}

func (s *transactionReceiptIndexerTestSuite) TestIndex_InvalidBlockTime() {
	require := testutil.Require(s.T())

	event := testutil.MakeBlockEvent(sequence, blockTag, eventTag)
	height := event.Block.Height
	header, err := fixtures.ReadJson("controller/ethereum/eth_header_invalid_block_time.json")
	require.NoError(err)
	receipt1, err := fixtures.ReadJson("controller/ethereum/eth_transactionreceipt_1.json")
	require.NoError(err)
	receipt2, err := fixtures.ReadJson("controller/ethereum/eth_transactionreceipt_2.json")
	require.NoError(err)
	block := &chainstorageapi.Block{
		Metadata: testutil.MakeBlockMetadata(height, blockTag),
		Blobdata: &chainstorageapi.Block_Ethereum{
			Ethereum: &chainstorageapi.EthereumBlobdata{
				Header:              header,
				TransactionReceipts: [][]byte{receipt1, receipt2},
			},
		},
	}

	err = s.indexer.Index(context.Background(), tag, event, block)
	require.Error(err)
}

func (s *transactionReceiptIndexerTestSuite) TestIndex_NoReceipts() {
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

	err = s.indexer.Index(context.Background(), tag, event, block)
	require.NoError(err)
}
