package integration

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/big"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	ethereum "github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/testsuite"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	chainstorageapi "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"

	"github.com/coinbase/chainnode/internal/api"
	xapi "github.com/coinbase/chainnode/internal/api/ethereum"
	"github.com/coinbase/chainnode/internal/cadence"
	"github.com/coinbase/chainnode/internal/clients"
	"github.com/coinbase/chainnode/internal/clients/blockchain/jsonrpc"
	"github.com/coinbase/chainnode/internal/clients/chainstorage"
	"github.com/coinbase/chainnode/internal/config"
	"github.com/coinbase/chainnode/internal/controller"
	"github.com/coinbase/chainnode/internal/controller/ethereum/handler"
	"github.com/coinbase/chainnode/internal/controller/internal"
	"github.com/coinbase/chainnode/internal/server"
	"github.com/coinbase/chainnode/internal/storage"
	"github.com/coinbase/chainnode/internal/utils/constants"
	"github.com/coinbase/chainnode/internal/utils/fixtures"
	"github.com/coinbase/chainnode/internal/utils/jsonutil"
	"github.com/coinbase/chainnode/internal/utils/syncgroup"
	"github.com/coinbase/chainnode/internal/utils/testapp"
	"github.com/coinbase/chainnode/internal/utils/testutil"
)

type (
	// ControllerTestSuite performs the following steps:
	// 1. Pull data from ChainStorage
	// 2. Index the data into storage
	// 3. Call server and validator endpoint
	// 4. Compare the responses
	ControllerTestSuite struct {
		suite.Suite
		testsuite.WorkflowTestSuite
		config             TestConfig
		app                testapp.TestApp
		chainStorageClient chainstorage.Client
		chainStorageParser chainstorage.Parser
		httpClient         *http.Client
		nodeClient         jsonrpc.Client
		checkpointer       internal.Checkpointer
		handler            internal.Handler
		controller         internal.Controller
		server             *httptest.Server
		events             []*api.Event
		blocks             []*api.Block
		tagStable          uint32
	}

	TestConfig struct {
		Blockchain         common.Blockchain
		Network            common.Network
		SequenceNum        int64
		Height             uint64
		TraceFixtures      []string
		TransferEventTopic string
		AddressIndex       uint32
	}
)

const (
	numEvents       = 10
	maxTransactions = 5

	nodeTimeout = 5 * time.Second

	transferEventTopic = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
)

func TestIntegrationControllerTestSuite_EthereumMainnet(t *testing.T) {
	suite.Run(t, &ControllerTestSuite{
		config: TestConfig{
			Blockchain:  common.Blockchain_BLOCKCHAIN_ETHEREUM,
			Network:     common.Network_NETWORK_ETHEREUM_MAINNET,
			SequenceNum: int64(15486500),
			Height:      15486500,
			TraceFixtures: []string{
				"controller/ethereum/eth_mainnet_trace_sequence_15486501.json",
			},
			TransferEventTopic: transferEventTopic,
			AddressIndex:       0,
		},
	})
}

func TestIntegrationControllerTestSuite_PolygonMainnet(t *testing.T) {
	suite.Run(t, &ControllerTestSuite{
		config: TestConfig{
			Blockchain:  common.Blockchain_BLOCKCHAIN_POLYGON,
			Network:     common.Network_NETWORK_POLYGON_MAINNET,
			SequenceNum: int64(27000000),
			Height:      27000000,
			TraceFixtures: []string{
				"controller/ethereum/polygon_mainnet_trace_sequence_27000001.json",
			},
			TransferEventTopic: transferEventTopic,
			AddressIndex:       0,
		},
	})
}

func (s *ControllerTestSuite) SetupSuite() {
	cfg, err := config.New(
		config.WithBlockchain(s.config.Blockchain),
		config.WithNetwork(s.config.Network),
	)
	s.Require().NoError(err)

	// The test verifies the results by itself.
	// Shadowing is enabled at 20% so that the validator code path is run.
	cfg.Controller.Handler.ShadowPercentage = 20

	var deps struct {
		fx.In
		ChainStorageClient chainstorage.Client
		ChainStorageParser chainstorage.Parser
		NodeClient         jsonrpc.Client `name:"proxy"`
		Controller         internal.Controller
		Server             *server.Server
	}
	env := cadence.NewTestEnv(s)
	s.app = testapp.New(
		s.T(),
		testapp.WithFunctional(),
		testapp.WithConfig(cfg),
		cadence.WithTestEnv(env),
		clients.Module,
		server.Module,
		storage.Module,
		controller.Module,
		fx.Populate(&deps),
	)
	s.chainStorageClient = deps.ChainStorageClient
	s.chainStorageParser = deps.ChainStorageParser
	s.httpClient = http.DefaultClient
	s.nodeClient = deps.NodeClient
	s.controller = deps.Controller
	s.checkpointer = deps.Controller.Checkpointer()
	s.handler = deps.Controller.Handler()
	s.server = httptest.NewServer(deps.Server)
	s.tagStable = s.app.Config().Tag.Stable
	s.initData()
}

func (s *ControllerTestSuite) TearDownSuite() {
	if s.app != nil {
		s.server.Close()
		s.app.Close()
	}
}

func (s *ControllerTestSuite) TestBlocks() {
	require := testutil.Require(s.T())

	method := handler.EthGetBlockByNumber
	indexer, err := s.controller.Indexer(xapi.CollectionBlocks)
	require.NoError(err)
	s.indexData(indexer)
	for _, event := range s.events {
		height := hexutil.EncodeUint64(event.Block.Height)
		for _, params := range []jsonrpc.Params{
			{height, false},
			{height, true},
		} {
			s.runTest("blocks", method, params, nil)
		}
	}

	// Test dispatcher success
	s.testDispatcher(method, jsonrpc.Params{hexutil.EncodeUint64(s.events[0].Block.Height), true}, constants.NativeOnlyMode, nil)
}

func (s *ControllerTestSuite) TestBlocksByHash() {
	require := testutil.Require(s.T())

	method := handler.EthGetBlockByHash
	indexer, err := s.controller.Indexer(xapi.CollectionBlocksByHash)
	require.NoError(err)
	s.indexData(indexer)
	for _, event := range s.events {
		hash := event.Block.Hash
		for _, params := range []jsonrpc.Params{
			{hash, false},
			{hash, true},
		} {
			s.runTest("blocksByHash", method, params, nil)
		}
	}

	// Test dispatcher success
	s.testDispatcher(method, jsonrpc.Params{s.events[0].Block.Hash, true}, constants.NativeOnlyMode, nil)
}

func (s *ControllerTestSuite) TestGetBlockTransactionCountByNumber() {
	require := testutil.Require(s.T())

	method := handler.EthGetBlockTransactionCountByNumber
	indexer, err := s.controller.Indexer(xapi.CollectionBlocks)
	require.NoError(err)
	s.indexData(indexer)
	for _, event := range s.events {
		s.runTest("blockTransactionCountByNumber", method, jsonrpc.Params{hexutil.EncodeUint64(event.Block.Height)}, nil)
	}

	// Test dispatcher success
	s.testDispatcher(method, jsonrpc.Params{hexutil.EncodeUint64(s.events[0].Block.Height)}, constants.NativeOnlyMode, nil)
}

func (s *ControllerTestSuite) TestGetBlockTransactionCountByHash() {
	require := testutil.Require(s.T())

	method := handler.EthGetBlockTransactionCountByHash
	indexer, err := s.controller.Indexer(xapi.CollectionBlocksByHash)
	require.NoError(err)
	s.indexData(indexer)
	for _, event := range s.events {
		s.runTest("blockTransactionCountByHash", method, jsonrpc.Params{event.Block.Hash}, nil)
	}

	// Test dispatcher success
	s.testDispatcher(method, jsonrpc.Params{s.events[0].Block.Hash}, constants.NativeOnlyMode, nil)
}

func (s *ControllerTestSuite) TestGetUncleCountByBlockNumber() {
	require := testutil.Require(s.T())

	method := handler.EthGetUncleCountByBlockNumber
	indexer, err := s.controller.Indexer(xapi.CollectionBlocks)
	require.NoError(err)
	s.indexData(indexer)
	for _, event := range s.events {
		s.runTest("uncleCountByBlockNumber", method, jsonrpc.Params{hexutil.EncodeUint64(event.Block.Height)}, nil)
	}

	// Test dispatcher success
	s.testDispatcher(method, jsonrpc.Params{hexutil.EncodeUint64(s.events[0].Block.Height)}, constants.NativeOnlyMode, nil)
}

func (s *ControllerTestSuite) TestGetUncleCountByBlockHash() {
	require := testutil.Require(s.T())

	method := handler.EthGetUncleCountByBlockHash
	indexer, err := s.controller.Indexer(xapi.CollectionBlocksByHash)
	require.NoError(err)
	s.indexData(indexer)
	for _, event := range s.events {
		s.runTest("uncleCountByBlockHash", method, jsonrpc.Params{event.Block.Hash}, nil)
	}

	// Test dispatcher success
	s.testDispatcher(method, jsonrpc.Params{s.events[0].Block.Hash}, constants.NativeOnlyMode, nil)
}

func (s *ControllerTestSuite) TestLogs() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	indexer, err := s.controller.Indexer(xapi.CollectionLogsV2)
	require.NoError(err)
	s.indexData(indexer)
	method := handler.EthGetLogs
	for i, event := range s.events {
		height := big.NewInt(int64(event.Block.Height))
		params := jsonrpc.Params{
			handler.FilterCriteria{
				FromBlock: height,
				ToBlock:   height,
			},
		}
		block, err := s.chainStorageParser.ParseNativeBlock(ctx, s.blocks[i])
		require.NoError(err)
		if block.NumTransactions != 0 {
			s.runTest("logsByBlock", method, params, nil)
		}
	}

	// Filter by range.
	startHeight := s.events[0].Block.Height
	endHeight := s.events[len(s.events)-1].Block.Height
	params := jsonrpc.Params{
		handler.FilterCriteria{
			FromBlock: big.NewInt(int64(startHeight)),
			ToBlock:   big.NewInt(int64(endHeight)),
		},
	}
	s.runTest("logsByRange", method, params, nil)

	// Filter by hash.
	blockHash := ethereum.HexToHash(s.events[0].Block.Hash)
	params = jsonrpc.Params{
		handler.FilterCriteria{
			BlockHash: &blockHash,
		},
	}
	s.runTest("logsByHash", method, params, nil)

	// Filter by addresses.
	addresses := make([]ethereum.Address, 0)
	for i := range s.blocks {
		block, err := s.chainStorageParser.ParseNativeBlock(ctx, s.blocks[i])
		require.NoError(err)
		ethereumBlock := block.GetEthereum()
		require.NotNil(ethereumBlock)
		if block.NumTransactions != 0 {
			address := ethereumBlock.Transactions[s.config.AddressIndex].To
			addresses = append(addresses, ethereum.HexToAddress(address))
		}
	}
	params = jsonrpc.Params{
		handler.FilterCriteria{
			FromBlock: big.NewInt(int64(startHeight)),
			ToBlock:   big.NewInt(int64(endHeight)),
			Addresses: addresses,
		},
	}
	s.runTest("logsByAddresses", method, params, nil)

	// Filter by topics.
	topics := [][]ethereum.Hash{{ethereum.HexToHash(s.config.TransferEventTopic)}}
	params = jsonrpc.Params{
		handler.FilterCriteria{
			FromBlock: big.NewInt(int64(startHeight)),
			ToBlock:   big.NewInt(int64(endHeight)),
			Topics:    topics,
		},
	}
	s.runTest("logsByTopics", method, params, nil)

	// Filter by addresses and topics.
	params = jsonrpc.Params{
		handler.FilterCriteria{
			FromBlock: big.NewInt(int64(startHeight)),
			ToBlock:   big.NewInt(int64(endHeight)),
			Addresses: addresses,
			Topics:    topics,
		},
	}
	s.runTest("logsByAddressesAndTopics", method, params, nil)

	// Test dispatcher success
	s.testDispatcher(method, params, constants.NativeOnlyMode, nil)
}

func (s *ControllerTestSuite) TestTransactions() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	method := handler.EthGetTransactionByHash
	indexer, err := s.controller.Indexer(xapi.CollectionTransactions)
	require.NoError(err)
	s.indexData(indexer)
	for i := range s.blocks {
		block, err := s.chainStorageParser.ParseNativeBlock(ctx, s.blocks[i])
		require.NoError(err)
		ethereumBlock := block.GetEthereum()
		require.NotNil(ethereumBlock)
		for j, transaction := range ethereumBlock.GetHeader().Transactions {
			if j >= maxTransactions {
				// Only validate the first few transactions to save time.
				break
			}

			params := jsonrpc.Params{
				transaction,
			}
			s.runTest("transactions", method, params, nil)
			if i == 0 && j == 0 {
				// Test dispatcher success. Only test once
				s.testDispatcher(method, jsonrpc.Params{ethereumBlock.GetHeader().Transactions[0]}, constants.NativeOnlyMode, nil)
			}
		}
	}

}

func (s *ControllerTestSuite) TestTransactionReceipts() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	method := handler.EthGetTransactionReceipt
	indexer, err := s.controller.Indexer(xapi.CollectionTransactionReceipts)
	require.NoError(err)
	s.indexData(indexer)
	for i := range s.blocks {
		block, err := s.chainStorageParser.ParseNativeBlock(ctx, s.blocks[i])
		require.NoError(err)
		ethereumBlock := block.GetEthereum()
		require.NotNil(ethereumBlock)
		for j, transaction := range ethereumBlock.GetHeader().Transactions {
			if j >= maxTransactions {
				// Only validate the first few transactions to save time.
				break
			}

			params := jsonrpc.Params{
				transaction,
			}
			s.runTest("transactionReceipts", method, params, nil)

			if i == 0 && j == 0 {
				// Test dispatcher success. Only test once
				s.testDispatcher(method, params, constants.NativeOnlyMode, nil)
			}
		}
	}
}

func (s *ControllerTestSuite) TestGetTransactionByBlockHashAndIndex() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	method := handler.EthGetTransactionByBlockHashAndIndex
	indexer, err := s.controller.Indexer(xapi.CollectionBlocksByHash)
	require.NoError(err)
	s.indexData(indexer)
	for i, event := range s.events {
		hash := event.Block.Hash
		block, err := s.chainStorageParser.ParseNativeBlock(ctx, s.blocks[i])
		require.NoError(err)

		randIndex := big.NewInt(0)
		numTransactions := block.NumTransactions
		if numTransactions != 0 {
			randIndex, err = rand.Int(rand.Reader, big.NewInt(int64(numTransactions)))
			require.NoError(err)

			s.runTest("transactionByBlockHashAndIndex", method, jsonrpc.Params{hash, hexutil.EncodeUint64(randIndex.Uint64())}, nil)
		}
	}

	// Test dispatcher success
	s.testDispatcher(method, jsonrpc.Params{s.events[0].Block.Hash, hexutil.EncodeUint64(0)}, constants.NativeOnlyMode, nil)
}

func (s *ControllerTestSuite) TestGetTransactionByBlockNumberAndIndex() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	method := handler.EthGetTransactionByBlockNumberAndIndex
	indexer, err := s.controller.Indexer(xapi.CollectionBlocks)
	require.NoError(err)
	s.indexData(indexer)
	for i, event := range s.events {
		height := hexutil.EncodeUint64(event.Block.Height)
		block, err := s.chainStorageParser.ParseNativeBlock(ctx, s.blocks[i])
		require.NoError(err)

		randIndex := big.NewInt(0)
		numTransactions := block.NumTransactions
		if numTransactions != 0 {
			randIndex, err = rand.Int(rand.Reader, big.NewInt(int64(numTransactions)))
			require.NoError(err)

			s.runTest("transactionByBlockNumberAndIndex", method, jsonrpc.Params{height, hexutil.EncodeUint64(randIndex.Uint64())}, nil)
		}
	}

	// Test dispatcher success
	s.testDispatcher(method, jsonrpc.Params{hexutil.EncodeUint64(s.events[0].Block.Height), hexutil.EncodeUint64(0)}, constants.NativeOnlyMode, nil)
}

func (s *ControllerTestSuite) TestTracesByHash() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	method := handler.DebugTraceBlockByHash
	indexer, err := s.controller.Indexer(xapi.CollectionTracesByHash)
	require.NoError(err)
	s.indexData(indexer)
	for i := range s.config.TraceFixtures {
		expected, err := fixtures.ReadFile(s.config.TraceFixtures[i])
		require.NoError(err)
		block, err := s.chainStorageParser.ParseNativeBlock(ctx, s.blocks[i])
		require.NoError(err)
		ethereumBlock := block.GetEthereum()
		require.NotNil(ethereumBlock)

		params := jsonrpc.Params{
			ethereumBlock.GetHeader().Hash,
			map[string]string{
				"tracer": "callTracer",
			},
		}
		s.runTest("tracesByHash", method, params, expected)

		if i == 0 {
			// Test dispatcher success. Only test once
			s.testDispatcher(method, params, constants.NativeOnlyMode, nil)
		}
	}
}

func (s *ControllerTestSuite) TestTracesByNumber() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	method := handler.DebugTraceBlockByNumber
	indexer, err := s.controller.Indexer(xapi.CollectionTracesByNumber)
	require.NoError(err)
	s.indexData(indexer)
	for i := range s.config.TraceFixtures {
		expected, err := fixtures.ReadFile(s.config.TraceFixtures[i])
		require.NoError(err)
		block, err := s.chainStorageParser.ParseNativeBlock(ctx, s.blocks[i])
		require.NoError(err)
		ethereumBlock := block.GetEthereum()
		require.NotNil(ethereumBlock)

		params := jsonrpc.Params{
			hexutil.EncodeUint64(ethereumBlock.GetHeader().Number),
			map[string]string{
				"tracer": "callTracer",
			},
		}
		s.runTest("tracesByNumber", method, params, expected)

		if i == 0 {
			// Test dispatcher success. Only test once
			s.testDispatcher(method, params, constants.NativeOnlyMode, nil)
		}
	}
}

func (s *ControllerTestSuite) TestChainId() {
	method := handler.EthChainId
	s.runTest("chainId", method, nil, nil)
	s.testDispatcher(method, nil, constants.NativeOnlyMode, nil)
}

func (s *ControllerTestSuite) TestVersion() {
	method := handler.NetVersion
	s.runTest("version", method, nil, nil)
	s.testDispatcher(method, nil, constants.NativeOnlyMode, nil)
}

// One proxied api test to ensure correct error is returned
func (s *ControllerTestSuite) TestGasPrice() {
	method := handler.EthGasPrice
	s.testDispatcher(method, nil, constants.NativeOnlyMode, xerrors.Errorf("unable to dispatch method=%v: not allowed", method.Name))
}

func (s *ControllerTestSuite) initData() {
	require := testutil.Require(s.T())
	ctx := context.Background()
	events, err := s.chainStorageClient.GetChainEvents(ctx, &chainstorageapi.GetChainEventsRequest{
		SequenceNum:  s.config.SequenceNum,
		MaxNumEvents: numEvents,
		EventTag:     s.app.Config().Workflows.Coordinator.EventTag,
	})
	require.NoError(err)
	require.Equal(numEvents, len(events))
	blocks := make([]*api.Block, len(events))

	group, groupCtx := syncgroup.New(ctx)
	for i := range events {
		i := i
		event := events[i]
		require.Equal(chainstorageapi.BlockchainEvent_BLOCK_ADDED, event.Type)
		group.Go(func() error {
			blockID := event.Block
			block, err := s.chainStorageClient.GetBlockWithTag(groupCtx, blockID.Tag, blockID.Height, blockID.Hash)
			if err != nil {
				return xerrors.Errorf("failed to get block: %w", err)
			}

			blocks[i] = block
			return nil
		})
	}

	err = group.Wait()
	require.NoError(err)

	event := events[0]
	checkpoint := api.NewCheckpoint(
		api.CollectionEarliestCheckpoint,
		s.tagStable,
		api.Sequence(event.SequenceNum),
		event.Block.Height,
	).WithLastBlockTimestamp(event.Block.Timestamp)
	err = s.checkpointer.Set(ctx, checkpoint)
	require.NoError(err)

	event = events[len(events)-1]
	checkpoint = api.NewCheckpoint(
		api.CollectionLatestCheckpoint,
		s.tagStable,
		api.Sequence(event.SequenceNum),
		event.Block.Height,
	).WithLastBlockTimestamp(event.Block.Timestamp)
	err = s.checkpointer.Set(ctx, checkpoint)
	require.NoError(err)

	s.events = events
	s.blocks = blocks
}

func (s *ControllerTestSuite) TestGetAuthor() {
	if s.config.Blockchain != common.Blockchain_BLOCKCHAIN_POLYGON {
		s.T().Skip()
	}
	require := testutil.Require(s.T())

	method := handler.BorGetAuthor
	indexer, err := s.controller.Indexer(xapi.CollectionBlocksExtraDataByNumber)
	require.NoError(err)
	s.indexData(indexer)
	for _, event := range s.events {
		s.runTest("getAuthor", method, jsonrpc.Params{hexutil.EncodeUint64(event.Block.Height)}, nil)
	}

	// Test dispatcher success
	s.testDispatcher(method, jsonrpc.Params{hexutil.EncodeUint64(s.events[0].Block.Height)}, constants.NativeOnlyMode, nil)
}

func (s *ControllerTestSuite) indexData(indexer internal.Indexer) {
	require := testutil.Require(s.T())
	ctx := context.Background()

	for i := range s.events {
		err := indexer.Index(ctx, s.tagStable, s.events[i], s.blocks[i])
		require.NoError(err)
	}
}

// runTest tests a method against server and node and compares the result
// if fixture params is not empty, it will not call the node but use the fixture as the expected data source
func (s *ControllerTestSuite) runTest(name string, method *jsonrpc.RequestMethod, params jsonrpc.Params, fixture []byte) {
	require := testutil.Require(s.T())

	logger := s.app.Logger()
	logger.Info(
		"running test",
		zap.String("name", name),
		zap.String("method", method.Name),
		zap.Reflect("params", params),
	)

	var expected, actual string
	group, _ := syncgroup.New(context.Background())
	group.Go(func() error {
		var err error
		actual, err = s.callServer(method.Name, params, constants.DynamicMode)
		return err
	})
	group.Go(func() error {
		if len(fixture) != 0 {
			formatted, err := s.formatJSON(fixture)
			expected = formatted
			return err
		}

		var err error
		expected, err = s.callNode(method.Name, params)
		return err
	})
	err := group.Wait()
	require.NoError(err)

	// A valid test case should not produce an empty response.
	require.NotEmpty(actual)
	require.NotEqual("null", actual)
	require.NotEqual("[]", actual)

	if expected != actual {
		// Write the responses to disk for easier debugging.
		expectedFile, err := s.writeToDisk("expected", expected)
		require.NoError(err)

		actualFile, err := s.writeToDisk("actual", actual)
		require.NoError(err)

		logger.Error(
			"responses are different",
			zap.String("expected", expectedFile),
			zap.String("actual", actualFile),
			zap.String("diff", fmt.Sprintf("vimdiff %v %v", expectedFile, actualFile)),
		)

		require.Equal(expected, actual)
	}
}

func (s *ControllerTestSuite) testDispatcher(method *jsonrpc.RequestMethod, params jsonrpc.Params, dispatchMode constants.DispatchMode, expectedError error) {
	require := testutil.Require(s.T())
	logger := s.app.Logger()

	logger.Info(
		"running dispatcher test",
		zap.String("method", method.Name),
		zap.Reflect("params", params),
	)

	actual, err := s.callServer(method.Name, params, dispatchMode)
	if err != nil {
		require.EqualError(err, expectedError.Error())
		require.Empty(actual)
	} else {
		require.NotEmpty(actual)
		require.NotEqual("null", actual)
		require.NotEqual("[]", actual)
	}

	logger.Info(
		"completed dispatcher test",
		zap.String("method", method.Name),
	)
}

func (s *ControllerTestSuite) callServer(method string, params jsonrpc.Params, dispatchMode constants.DispatchMode) (string, error) {
	request := &jsonrpc.Request{
		JSONRPC: "2.0",
		Method:  method,
		Params:  params,
		ID:      0,
	}
	requestData, err := json.Marshal(request)
	if err != nil {
		return "", xerrors.Errorf("failed to marshal request: %w", err)
	}

	url := s.server.URL + s.handler.Path()
	req, err := http.NewRequest("POST", url, bytes.NewBuffer(requestData))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	if dispatchMode == constants.NativeOnlyMode {
		req.Header.Set(constants.RoutingModeHttpHeaderName, string(dispatchMode))
	}
	responseData, err := s.httpClient.Do(req)
	if err != nil {
		return "", xerrors.Errorf("failed to call server: %w", err)
	}

	defer responseData.Body.Close()
	var response jsonrpc.Response
	err = json.NewDecoder(responseData.Body).Decode(&response)
	if err != nil {
		return "", xerrors.Errorf("failed to read response: %w", err)
	}

	if response.Error != nil {
		return "", response.Error
	}

	return s.formatJSON(response.Result)
}

func (s *ControllerTestSuite) callNode(method string, params jsonrpc.Params) (string, error) {
	requestMethod := &jsonrpc.RequestMethod{
		Name:    method,
		Timeout: nodeTimeout,
	}
	response, err := s.nodeClient.Call(context.Background(), requestMethod, params)
	if err != nil {
		return "", xerrors.Errorf("failed to call node: %w", err)
	}

	return s.formatJSON(response.Result)
}

func (s *ControllerTestSuite) formatJSON(input []byte) (string, error) {
	var v interface{}
	err := json.Unmarshal(input, &v)
	if err != nil {
		return "", xerrors.Errorf("failed to unmarshal result: %w", err)
	}

	jsonutil.FilterNulls(v)
	return jsonutil.FormatJSON(v)
}

func (s *ControllerTestSuite) writeToDisk(prefix string, response string) (string, error) {
	pattern := fmt.Sprintf("chainnode-%v-*.txt", prefix)
	file, err := ioutil.TempFile("", pattern)
	if err != nil {
		return "", xerrors.Errorf("failed to create temp file: %w", err)
	}

	_, err = file.WriteString(response)
	if err != nil {
		return "", xerrors.Errorf("failed to write to temp file: %w", err)
	}

	return file.Name(), nil
}
