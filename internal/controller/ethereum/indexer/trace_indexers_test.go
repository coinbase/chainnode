package indexer

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"

	c3common "github.com/coinbase/chainstorage/protos/coinbase/c3/common"
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

type (
	TraceIndexerTestConfig struct {
		Collection       api.Collection
		Blockchain       c3common.Blockchain
		Network          c3common.Network
		TracePath        string
		TraceFixturePath string
		ErrorTracePath   string
		BlockHeight      uint64
	}
)

type TraceIndexersTestSuite struct {
	suite.Suite
	config      TraceIndexerTestConfig
	ctrl        *gomock.Controller
	indexer     internal.Indexer
	storage     *storagemocks.MockEthereumStorage
	persistFunc func(interface{}, interface{}) *gomock.Call
	app         testapp.TestApp
}

func TestTraceByHashIndexerTestSuite(t *testing.T) {
	suite.Run(t, &TraceIndexersTestSuite{
		config: TraceIndexerTestConfig{
			Collection:       xapi.CollectionTracesByHash,
			Blockchain:       c3common.Blockchain_BLOCKCHAIN_ETHEREUM,
			Network:          c3common.Network_NETWORK_ETHEREUM_MAINNET,
			TraceFixturePath: "controller/ethereum/eth_trace.json",
			TracePath:        "controller/ethereum/eth_trace_1.json",
			ErrorTracePath:   "controller/ethereum/eth_trace_error.json",
			BlockHeight:      uint64(30000000),
		},
	})
}

func TestTraceByNumberIndexerTestSuite(t *testing.T) {
	suite.Run(t, &TraceIndexersTestSuite{
		config: TraceIndexerTestConfig{
			Collection:       xapi.CollectionTracesByNumber,
			Blockchain:       c3common.Blockchain_BLOCKCHAIN_ETHEREUM,
			Network:          c3common.Network_NETWORK_ETHEREUM_MAINNET,
			TraceFixturePath: "controller/ethereum/eth_trace.json",
			TracePath:        "controller/ethereum/eth_trace_1.json",
			ErrorTracePath:   "controller/ethereum/eth_trace_error.json",
			BlockHeight:      uint64(30000000),
		},
	})
}

func TestArbtraceBlockIndexerTestSuite(t *testing.T) {
	suite.Run(t, &TraceIndexersTestSuite{
		config: TraceIndexerTestConfig{
			Collection:       xapi.CollectionArbtraceBlock,
			Blockchain:       c3common.Blockchain_BLOCKCHAIN_ARBITRUM,
			Network:          c3common.Network_NETWORK_ARBITRUM_MAINNET,
			TraceFixturePath: "controller/ethereum/arb_trace.json",
			TracePath:        "controller/ethereum/arb_trace_1.json",
			ErrorTracePath:   "controller/ethereum/arb_trace_error.json",
			BlockHeight:      uint64(12000000), //before arbitrum nitro upgrade
		},
	})
}

func (s *TraceIndexersTestSuite) SetupSuite() {
	var deps struct {
		fx.In
		HashIndexer   internal.Indexer `name:"ethereum/traceByHash"`
		NumberIndexer internal.Indexer `name:"ethereum/traceByNumber"`
	}

	s.ctrl = gomock.NewController(s.T())
	s.storage = storagemocks.NewMockEthereumStorage(s.ctrl)
	s.app = testapp.New(
		s.T(),
		Module,
		fx.Provide(func() storage.EthereumStorage { return s.storage }),
		fx.Populate(&deps),
	)

	switch s.config.Collection {
	case xapi.CollectionTracesByHash:
		s.indexer = deps.HashIndexer
		s.persistFunc = s.storage.EXPECT().PersistTraceByHash
	case xapi.CollectionTracesByNumber:
		s.indexer = deps.NumberIndexer
		s.persistFunc = s.storage.EXPECT().PersistTraceByNumber
	case xapi.CollectionArbtraceBlock:
		s.indexer = deps.NumberIndexer
		s.persistFunc = s.storage.EXPECT().PersistArbtraceBlock
	default:
		s.Fail("unknown trace collection")
	}
}

func (s *TraceIndexersTestSuite) TearDownSuite() {
	s.app.Close()
}

func (s *TraceIndexersTestSuite) TestIndex() {
	require := testutil.Require(s.T())

	event := testutil.MakeBlockEvent(sequence, blockTag, eventTag, testutil.WithBlockHeight(s.config.BlockHeight))
	height := event.Block.Height
	hash := event.Block.Hash
	trace, err := fixtures.ReadJson(s.config.TracePath)
	require.NoError(err)
	header, err := fixtures.ReadJson("controller/ethereum/eth_header.json")
	require.NoError(err)
	block := &chainstorageapi.Block{
		Blockchain: s.config.Blockchain,
		Network:    s.config.Network,
		Metadata:   testutil.MakeBlockMetadata(height, blockTag),
		Blobdata: &chainstorageapi.Block_Ethereum{
			Ethereum: &chainstorageapi.EthereumBlobdata{
				Header:            header,
				TransactionTraces: [][]byte{trace},
			},
		},
	}

	traceFixture, err := fixtures.ReadJson(s.config.TraceFixturePath)
	require.NoError(err)

	blockTime := testutil.MustTime("2020-11-24T16:07:21Z")
	expectedTrace := xapi.NewTrace(tag, height, hash, sequence, traceFixture, blockTime)

	s.persistFunc(gomock.Any(), expectedTrace).Return(nil)

	err = s.indexer.Index(context.Background(), tag, event, block)
	require.NoError(err)
}

func (s *TraceIndexersTestSuite) TestIndex_NoTrace() {
	require := testutil.Require(s.T())

	event := testutil.MakeBlockEvent(sequence, blockTag, eventTag, testutil.WithBlockHeight(s.config.BlockHeight))
	height := event.Block.Height
	hash := event.Block.Hash
	header, err := fixtures.ReadJson("controller/ethereum/eth_header.json")
	require.NoError(err)
	block := &chainstorageapi.Block{
		Blockchain: s.config.Blockchain,
		Network:    s.config.Network,
		Metadata:   testutil.MakeBlockMetadata(height, blockTag),
		Blobdata: &chainstorageapi.Block_Ethereum{
			Ethereum: &chainstorageapi.EthereumBlobdata{
				Header: header,
			},
		},
	}

	blockTime := testutil.MustTime("2020-11-24T16:07:21Z")
	expectedTrace := xapi.NewTrace(tag, height, hash, sequence, []byte("[]"), blockTime)

	s.persistFunc(gomock.Any(), expectedTrace).Return(nil)

	err = s.indexer.Index(context.Background(), tag, event, block)
	require.NoError(err)
}

func (s *TraceIndexersTestSuite) TestIndex_NoHeader() {
	require := testutil.Require(s.T())

	event := testutil.MakeBlockEvent(sequence, blockTag, eventTag, testutil.WithBlockHeight(s.config.BlockHeight))
	height := event.Block.Height
	block := &chainstorageapi.Block{
		Blockchain: s.config.Blockchain,
		Network:    s.config.Network,
		Metadata:   testutil.MakeBlockMetadata(height, blockTag),
	}

	err := s.indexer.Index(context.Background(), tag, event, block)
	require.Error(err)
}

func (s *TraceIndexersTestSuite) TestIndex_NoBlockTime() {
	require := testutil.Require(s.T())

	event := testutil.MakeBlockEvent(sequence, blockTag, eventTag, testutil.WithBlockHeight(s.config.BlockHeight))
	height := event.Block.Height
	hash := event.Block.Hash
	trace, err := fixtures.ReadJson(s.config.TracePath)
	require.NoError(err)
	header, err := fixtures.ReadJson("controller/ethereum/eth_header_no_block_time.json")
	require.NoError(err)
	block := &chainstorageapi.Block{
		Blockchain: s.config.Blockchain,
		Network:    s.config.Network,
		Metadata:   testutil.MakeBlockMetadata(height, blockTag),
		Blobdata: &chainstorageapi.Block_Ethereum{
			Ethereum: &chainstorageapi.EthereumBlobdata{
				Header:            header,
				TransactionTraces: [][]byte{trace},
			},
		},
	}

	traceFixture, err := fixtures.ReadJson(s.config.TraceFixturePath)
	require.NoError(err)

	expectedTrace := xapi.NewTrace(tag, height, hash, sequence, traceFixture, time.Time{})
	s.persistFunc(gomock.Any(), expectedTrace).Return(nil)

	err = s.indexer.Index(context.Background(), tag, event, block)
	require.NoError(err)
}

func (s *TraceIndexersTestSuite) TestIndex_InvalidBlockTime() {
	require := testutil.Require(s.T())

	event := testutil.MakeBlockEvent(sequence, blockTag, eventTag, testutil.WithBlockHeight(s.config.BlockHeight))
	height := event.Block.Height
	trace, err := fixtures.ReadJson("controller/ethereum/eth_trace_1.json")
	require.NoError(err)
	header, err := fixtures.ReadJson("controller/ethereum/eth_header_invalid_block_time.json")
	require.NoError(err)
	block := &chainstorageapi.Block{
		Blockchain: s.config.Blockchain,
		Network:    s.config.Network,
		Metadata:   testutil.MakeBlockMetadata(height, blockTag),
		Blobdata: &chainstorageapi.Block_Ethereum{
			Ethereum: &chainstorageapi.EthereumBlobdata{
				Header:            header,
				TransactionTraces: [][]byte{trace},
			},
		},
	}

	err = s.indexer.Index(context.Background(), tag, event, block)
	require.Error(err)
}

func (s *TraceIndexersTestSuite) TestIndex_TraceError() {
	require := testutil.Require(s.T())

	event := testutil.MakeBlockEvent(sequence, blockTag, eventTag, testutil.WithBlockHeight(s.config.BlockHeight))
	height := event.Block.Height
	hash := event.Block.Hash
	trace, err := fixtures.ReadJson("controller/ethereum/eth_trace_error_1.json")
	require.NoError(err)
	header, err := fixtures.ReadJson("controller/ethereum/eth_header.json")
	require.NoError(err)
	block := &chainstorageapi.Block{
		Blockchain: s.config.Blockchain,
		Network:    s.config.Network,
		Metadata:   testutil.MakeBlockMetadata(height, blockTag),
		Blobdata: &chainstorageapi.Block_Ethereum{
			Ethereum: &chainstorageapi.EthereumBlobdata{
				Header:            header,
				TransactionTraces: [][]byte{trace},
			},
		},
	}

	traceFixture, err := fixtures.ReadJson(s.config.ErrorTracePath)
	require.NoError(err)

	blockTime := testutil.MustTime("2020-11-24T16:07:21Z")
	expectedTrace := xapi.NewTrace(tag, height, hash, sequence, traceFixture, blockTime)

	s.persistFunc(gomock.Any(), expectedTrace).Return(nil)

	err = s.indexer.Index(context.Background(), tag, event, block)
	require.NoError(err)
}

func (s *TraceIndexersTestSuite) TestIndex_Arbitrum_SkipTraceByHash_BeforeNitroUpgrade() {
	if s.config.Collection != xapi.CollectionTracesByHash {
		s.T().Skip()
	}
	require := testutil.Require(s.T())

	event := testutil.MakeBlockEvent(sequence, blockTag, eventTag)
	height := event.Block.Height
	trace, err := fixtures.ReadJson(s.config.TracePath)
	require.NoError(err)
	header, err := fixtures.ReadJson("controller/ethereum/eth_header.json")
	require.NoError(err)
	block := &chainstorageapi.Block{
		Blockchain: c3common.Blockchain_BLOCKCHAIN_ARBITRUM,
		Network:    c3common.Network_NETWORK_ARBITRUM_MAINNET,
		Metadata:   testutil.MakeBlockMetadata(height, blockTag),
		Blobdata: &chainstorageapi.Block_Ethereum{
			Ethereum: &chainstorageapi.EthereumBlobdata{
				Header:            header,
				TransactionTraces: [][]byte{trace},
			},
		},
	}

	err = s.indexer.Index(context.Background(), tag, event, block)
	require.NoError(err)
}

func (s *TraceIndexersTestSuite) TestIndex_Ethereum_BelowNitroUpgrade() {
	if s.config.Blockchain != c3common.Blockchain_BLOCKCHAIN_ETHEREUM {
		s.T().Skip()
	}
	require := testutil.Require(s.T())

	event := testutil.MakeBlockEvent(sequence, blockTag, eventTag)
	height := event.Block.Height
	hash := event.Block.Hash
	trace, err := fixtures.ReadJson("controller/ethereum/eth_trace_1.json")
	require.NoError(err)
	header, err := fixtures.ReadJson("controller/ethereum/eth_header.json")
	require.NoError(err)
	block := &chainstorageapi.Block{
		Blockchain: c3common.Blockchain_BLOCKCHAIN_ETHEREUM,
		Network:    c3common.Network_NETWORK_ARBITRUM_MAINNET,
		Metadata:   testutil.MakeBlockMetadata(height, blockTag),
		Blobdata: &chainstorageapi.Block_Ethereum{
			Ethereum: &chainstorageapi.EthereumBlobdata{
				Header:            header,
				TransactionTraces: [][]byte{trace},
			},
		},
	}

	traceFixture, err := fixtures.ReadJson("controller/ethereum/eth_trace.json")
	require.NoError(err)

	blockTime := testutil.MustTime("2020-11-24T16:07:21Z")
	expectedTrace := xapi.NewTrace(tag, height, hash, sequence, traceFixture, blockTime)

	s.persistFunc(gomock.Any(), expectedTrace).Return(nil)

	err = s.indexer.Index(context.Background(), tag, event, block)
	require.NoError(err)
}
