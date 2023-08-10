package indexer

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"

	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	chainstorageapi "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"

	xapi "github.com/coinbase/chainnode/internal/api/ethereum"
	"github.com/coinbase/chainnode/internal/controller/internal"
	"github.com/coinbase/chainnode/internal/storage"
	storagemocks "github.com/coinbase/chainnode/internal/storage/mocks"
	"github.com/coinbase/chainnode/internal/utils/fixtures"
	"github.com/coinbase/chainnode/internal/utils/testapp"
	"github.com/coinbase/chainnode/internal/utils/testutil"
)

type LogIndexerV2TestSuite struct {
	suite.Suite
	ctrl    *gomock.Controller
	indexer internal.Indexer
	storage *storagemocks.MockEthereumStorage
	app     testapp.TestApp
}

const (
	logsBloomFixture = "0xfdf668a334802d0164a3e3cab8f79d6bab99b9800f565fa9dea9138b2192371768c4d8f83681bb0647e07d000807499cca14bcd05d1202c180e0e2a0f2777225f4990de285aa8086d82acd2c5653c46fe8943c0e50e6521a879a5144c14f57125c064c122e730c959509992ac09588e9c648da88a6eac64805fe9132d280772abd048d16428227c6c0d8c57a460c8281e0203f8791e402cdba21c5ea0430a282a2b3a7e593a2392a2523b7961b2fd0a06752631744001311b4a9ad111d20ec7d4c2d4e02892ed5023b12126442a219ac16400e40051900d250a3b7e6adc2e13053393130810561402181040301ca492fdb24320064c43a50c42c31ea259f4820"
)

func TestLogIndexerV2TestSuite(t *testing.T) {
	suite.Run(t, new(LogIndexerV2TestSuite))
}

func (s *LogIndexerV2TestSuite) SetupTest() {
	var deps struct {
		fx.In
		Indexer internal.Indexer `name:"ethereum/logV2"`
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

func (s *LogIndexerV2TestSuite) TearDownTest() {
	s.app.Close()
}

func (s *LogIndexerV2TestSuite) TestIndex() {
	require := testutil.Require(s.T())

	event := testutil.MakeBlockEvent(sequence, blockTag, eventTag)
	height := event.Block.Height
	hash := event.Block.Hash
	receipt, err := fixtures.ReadFile("controller/ethereum/eth_transactionreceipt_1.json")
	require.NoError(err)
	header, err := fixtures.ReadFile("controller/ethereum/eth_header.json")
	require.NoError(err)
	block := &chainstorageapi.Block{
		Metadata: testutil.MakeBlockMetadata(height, blockTag),
		Blobdata: &chainstorageapi.Block_Ethereum{
			Ethereum: &chainstorageapi.EthereumBlobdata{
				Header:              header,
				TransactionReceipts: [][]byte{receipt},
			},
		},
	}

	logsFixture, err := fixtures.ReadJson("controller/ethereum/eth_logs_1.json")
	require.NoError(err)

	blockTime := testutil.MustTime("2020-11-24T16:07:21Z")

	expectedLogs := xapi.NewLogs(tag, height, hash, sequence, logsBloomFixture, logsFixture, blockTime)

	s.storage.EXPECT().PersistLogsV2(gomock.Any(), expectedLogs).Return(nil)

	err = s.indexer.Index(context.Background(), tag, event, block)
	require.NoError(err)
}

func (s *LogIndexerV2TestSuite) TestIndex_NoReceipt() {
	require := testutil.Require(s.T())

	event := testutil.MakeBlockEvent(sequence, blockTag, eventTag)
	height := event.Block.Height
	hash := event.Block.Hash
	header, err := fixtures.ReadFile("controller/ethereum/eth_header.json")
	require.NoError(err)
	block := &chainstorageapi.Block{
		Metadata: testutil.MakeBlockMetadata(height, blockTag),
		Blobdata: &chainstorageapi.Block_Ethereum{
			Ethereum: &chainstorageapi.EthereumBlobdata{
				Header: header,
			},
		},
	}

	blockTime := testutil.MustTime("2020-11-24T16:07:21Z")

	expectedLogs := xapi.NewLogs(tag, height, hash, sequence, logsBloomFixture, []byte("[]"), blockTime)

	s.storage.EXPECT().PersistLogsV2(gomock.Any(), expectedLogs).Return(nil)

	err = s.indexer.Index(context.Background(), tag, event, block)
	require.NoError(err)
}

func (s *LogIndexerV2TestSuite) TestIndex_NoHeader() {
	require := testutil.Require(s.T())

	event := testutil.MakeBlockEvent(sequence, blockTag, eventTag)
	height := event.Block.Height
	block := &chainstorageapi.Block{
		Metadata: testutil.MakeBlockMetadata(height, blockTag),
	}

	err := s.indexer.Index(context.Background(), tag, event, block)
	require.Error(err)
}

func (s *LogIndexerV2TestSuite) TestIndex_NoBlockTime() {
	require := testutil.Require(s.T())

	event := testutil.MakeBlockEvent(sequence, blockTag, eventTag)
	height := event.Block.Height
	metadata := testutil.MakeBlockMetadata(height, tag)
	header, err := fixtures.ReadJson("controller/ethereum/eth_header_no_block_time.json")
	require.NoError(err)
	receipt, err := fixtures.ReadFile("controller/ethereum/eth_transactionreceipt_1.json")
	require.NoError(err)

	block := &chainstorageapi.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_ETHEREUM,
		Network:    common.Network_NETWORK_ETHEREUM_MAINNET,
		Metadata:   metadata,
		Blobdata: &chainstorageapi.Block_Ethereum{
			Ethereum: &chainstorageapi.EthereumBlobdata{
				Header:              header,
				TransactionReceipts: [][]byte{receipt},
			},
		},
	}
	hash := block.GetMetadata().Hash

	logsFixture, err := fixtures.ReadJson("controller/ethereum/eth_logs_1.json")
	require.NoError(err)

	expectedLogs := xapi.NewLogs(tag, height, hash, sequence, logsBloomFixture, logsFixture, time.Time{})
	s.storage.EXPECT().PersistLogsV2(gomock.Any(), expectedLogs).Return(nil)

	err = s.indexer.Index(context.Background(), tag, event, block)
	require.NoError(err)
}

func (s *LogIndexerV2TestSuite) TestIndex_InvalidBlockTime() {
	require := testutil.Require(s.T())

	event := testutil.MakeBlockEvent(sequence, blockTag, eventTag)
	height := event.Block.Height
	metadata := testutil.MakeBlockMetadata(height, tag)
	header, err := fixtures.ReadJson("controller/ethereum/eth_header_invalid_block_time.json")
	require.NoError(err)

	block := &chainstorageapi.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_ETHEREUM,
		Network:    common.Network_NETWORK_ETHEREUM_MAINNET,
		Metadata:   metadata,
		Blobdata: &chainstorageapi.Block_Ethereum{
			Ethereum: &chainstorageapi.EthereumBlobdata{
				Header: header,
			},
		},
	}

	err = s.indexer.Index(context.Background(), tag, event, block)
	require.Error(err)
}
