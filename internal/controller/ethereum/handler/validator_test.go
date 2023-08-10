package handler_test

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/eth/tracers"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally/v4"
	"go.uber.org/fx"

	"github.com/coinbase/chainnode/internal/api"
	"github.com/coinbase/chainnode/internal/clients/blockchain/jsonrpc"
	jsonrpcmocks "github.com/coinbase/chainnode/internal/clients/blockchain/jsonrpc/mocks"
	"github.com/coinbase/chainnode/internal/config"
	"github.com/coinbase/chainnode/internal/controller/ethereum/handler"
	handlermocks "github.com/coinbase/chainnode/internal/controller/ethereum/handler/mocks"
	"github.com/coinbase/chainnode/internal/server/rpc"
	"github.com/coinbase/chainnode/internal/utils/constants"
	"github.com/coinbase/chainnode/internal/utils/fixtures"
	"github.com/coinbase/chainnode/internal/utils/pointer"
	"github.com/coinbase/chainnode/internal/utils/testapp"
	"github.com/coinbase/chainnode/internal/utils/testutil"

	c3common "github.com/coinbase/chainstorage/protos/coinbase/c3/common"
)

type (
	ValidatorTestSuite struct {
		suite.Suite
		ctrl            *gomock.Controller
		config          ValidatorTestSuiteConfig
		app             testapp.TestApp
		jsonrpcClient   *jsonrpcmocks.MockClient
		receiver        *handlermocks.MockReceiver
		validatorResult *handler.ValidatorResult
		validator       handler.Validator
		scope           tally.TestScope
	}
	ValidatorTestSuiteConfig struct {
		Blockchain c3common.Blockchain
		Network    c3common.Network
	}
)

var (
	validatorBlockNumber = rpc.BlockNumber(0x123)
	validatorBlockHash   = common.HexToHash("0x1ab1")
	validatorTxHash      = common.HexToHash("0xabc")
)

func TestValidatorTestSuite_Ethereum_Mainnet(t *testing.T) {
	suite.Run(t, &ValidatorTestSuite{
		config: ValidatorTestSuiteConfig{
			Blockchain: c3common.Blockchain_BLOCKCHAIN_ETHEREUM,
			Network:    c3common.Network_NETWORK_ETHEREUM_MAINNET,
		},
	})
}

func (s *ValidatorTestSuite) SetupTest() {
	cfg, err := config.New(
		config.WithBlockchain(s.config.Blockchain),
		config.WithNetwork(s.config.Network),
	)
	s.Require().NoError(err)

	// Always shadow.
	cfg.Controller.Handler.ShadowPercentage = 100
	cfg.Controller.Handler.MethodConfigs = []config.MethodConfig{}

	s.ctrl = gomock.NewController(s.T())
	s.jsonrpcClient = jsonrpcmocks.NewMockClient(s.ctrl)
	s.receiver = handlermocks.NewMockReceiver(s.ctrl)
	s.validatorResult = &handler.ValidatorResult{
		Done: make(chan struct{}, 1),
	}

	var deps struct {
		fx.In
		Validator handler.Validator
		Scope     tally.Scope
	}
	s.app = testapp.New(
		s.T(),
		testapp.WithConfig(cfg),
		fx.Provide(fx.Annotated{Name: "validator", Target: func() jsonrpc.Client { return s.jsonrpcClient }}),
		fx.Provide(handler.NewValidator),
		fx.Provide(func() *handler.ValidatorResult { return s.validatorResult }),
		fx.Populate(&deps),
	)
	s.validator = deps.Validator
	s.scope = deps.Scope.(tally.TestScope)
}

func (s *ValidatorTestSuite) TearDownTest() {
	s.app.Close()
	s.ctrl.Finish()
}

func (s *ValidatorTestSuite) TestCall() {
	require := testutil.Require(s.T())

	method := handler.EthCall
	s.receiver.EXPECT().
		Call(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(json.RawMessage("0x1"), nil)

	interceptor := s.validator.WithValidatorInterceptor(s.receiver)
	require.NotNil(interceptor)

	ctx := context.Background()
	_, err := interceptor.Call(ctx, nil, validatorBlockNumber)
	require.NoError(err)
	close(s.validatorResult.Done)
	s.verifyParityCounters(method, 0, 0, 0)
}

func (s *ValidatorTestSuite) TestBlockNumber() {
	require := testutil.Require(s.T())

	method := handler.EthBlockNumber
	blockNumber := `"0xdaaaa9"`
	s.jsonrpcClient.EXPECT().
		Call(gomock.Any(), method, gomock.Any()).
		Return(&jsonrpc.Response{
			Result: json.RawMessage(blockNumber),
		}, nil)

	s.receiver.EXPECT().
		BlockNumber(gomock.Any()).
		Return(json.RawMessage(blockNumber), nil)

	interceptor := s.validator.WithValidatorInterceptor(s.receiver)
	require.NotNil(interceptor)

	ctx := context.Background()
	_, err := interceptor.BlockNumber(ctx)
	require.NoError(err)
	s.verifyParityCounters(method, 1, 0, 0)
}

func (s *ValidatorTestSuite) TestBlockNumber_Skipped() {
	require := testutil.Require(s.T())

	method := handler.EthBlockNumber
	primaryBlockNumber := `"0xdaaaa9"`
	shadowBlockNumber := `"0xdaaaa8"`
	s.jsonrpcClient.EXPECT().
		Call(gomock.Any(), method, gomock.Any()).
		Return(&jsonrpc.Response{
			Result: json.RawMessage(shadowBlockNumber),
		}, nil)

	s.receiver.EXPECT().
		BlockNumber(gomock.Any()).
		Return(json.RawMessage(primaryBlockNumber), nil)

	interceptor := s.validator.WithValidatorInterceptor(s.receiver)
	require.NotNil(interceptor)

	ctx := context.Background()
	_, err := interceptor.BlockNumber(ctx)
	require.NoError(err)
	s.verifyParityCounters(method, 0, 0, 1)
}

func (s *ValidatorTestSuite) TestBlockNumber_NoShadow() {
	// Override shadow percentage to 0
	s.app.Config().Controller.Handler.MethodConfigs = []config.MethodConfig{{MethodName: "eth_blockNumber", ShadowPercentage: 0}}

	require := testutil.Require(s.T())

	method := handler.EthBlockNumber
	blockNumber := `"0xdaaaa9"`

	s.receiver.EXPECT().
		BlockNumber(gomock.Any()).
		Return(json.RawMessage(blockNumber), nil)

	interceptor := s.validator.WithValidatorInterceptor(s.receiver)
	require.NotNil(interceptor)

	ctx := context.Background()
	_, err := interceptor.BlockNumber(ctx)
	require.NoError(err)
	close(s.validatorResult.Done)
	s.verifyParityCounters(method, 0, 0, 0)
}

func (s *ValidatorTestSuite) TestGetBlockByNumber() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	method := handler.EthGetBlockByNumber
	block := fixtures.MustReadFile("controller/ethereum/eth_header.json")
	s.jsonrpcClient.EXPECT().
		Call(gomock.Any(), method, gomock.Any()).
		Return(&jsonrpc.Response{
			Result: block,
		}, nil)

	s.receiver.EXPECT().
		GetBlockByNumber(ctx, validatorBlockNumber, false).
		Return(block, nil)

	interceptor := s.validator.WithValidatorInterceptor(s.receiver)
	require.NotNil(interceptor)

	_, err := interceptor.GetBlockByNumber(ctx, validatorBlockNumber, false)
	require.NoError(err)
	s.verifyParityCounters(method, 1, 0, 0)
}

func (s *ValidatorTestSuite) TestGetBlockByNumber_WithoutFullTx() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	method := handler.EthGetBlockByNumber
	block := fixtures.MustReadFile("controller/ethereum/eth_header_without_full_tx.json")
	s.jsonrpcClient.EXPECT().
		Call(gomock.Any(), method, gomock.Any()).
		Return(&jsonrpc.Response{
			Result: block,
		}, nil)

	s.receiver.EXPECT().
		GetBlockByNumber(ctx, validatorBlockNumber, false).
		Return(block, nil)

	interceptor := s.validator.WithValidatorInterceptor(s.receiver)
	require.NotNil(interceptor)

	_, err := interceptor.GetBlockByNumber(ctx, validatorBlockNumber, false)
	require.NoError(err)
	s.verifyParityCounters(method, 1, 0, 0)
}

func (s *ValidatorTestSuite) TestGetBlockByNumber_FilterChainId() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	method := handler.EthGetBlockByNumber
	block := fixtures.MustReadFile("controller/ethereum/eth_header.json")
	s.jsonrpcClient.EXPECT().
		Call(gomock.Any(), method, gomock.Any()).
		Return(&jsonrpc.Response{
			Result: block,
		}, nil)

	actualBlock := fixtures.MustReadFile("controller/ethereum/eth_header_chainId_0x0.json")
	s.receiver.EXPECT().
		GetBlockByNumber(ctx, validatorBlockNumber, false).
		Return(actualBlock, nil)

	interceptor := s.validator.WithValidatorInterceptor(s.receiver)
	require.NotNil(interceptor)

	_, err := interceptor.GetBlockByNumber(ctx, validatorBlockNumber, false)
	require.NoError(err)
	s.verifyParityCounters(method, 1, 0, 0)
}

func (s *ValidatorTestSuite) TestGetBlockByNumber_NoShadow() {
	// Override shadow percentage to 0
	s.app.Config().Controller.Handler.MethodConfigs = []config.MethodConfig{{MethodName: "eth_getBlockByNumber", ShadowPercentage: 0}}
	require := testutil.Require(s.T())

	ctx := context.Background()
	method := handler.EthGetBlockByNumber
	block := fixtures.MustReadFile("controller/ethereum/eth_header.json")

	s.receiver.EXPECT().
		GetBlockByNumber(ctx, validatorBlockNumber, false).
		Return(block, nil)

	interceptor := s.validator.WithValidatorInterceptor(s.receiver)
	require.NotNil(interceptor)

	_, err := interceptor.GetBlockByNumber(ctx, validatorBlockNumber, false)
	require.NoError(err)
	close(s.validatorResult.Done)
	s.verifyParityCounters(method, 0, 0, 0)
}

func (s *ValidatorTestSuite) TestGetBlockByNumber_Pending() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	method := handler.EthGetBlockByNumber
	block := fixtures.MustReadFile("controller/ethereum/eth_header.json")

	s.receiver.EXPECT().
		GetBlockByNumber(ctx, rpc.PendingBlockNumber, false).
		Return(block, nil)

	interceptor := s.validator.WithValidatorInterceptor(s.receiver)
	require.NotNil(interceptor)

	_, err := interceptor.GetBlockByNumber(ctx, rpc.PendingBlockNumber, false)
	require.NoError(err)
	close(s.validatorResult.Done)
	s.verifyParityCounters(method, 0, 0, 0)
}

func (s *ValidatorTestSuite) TestGetBlockByNumber_Unmatched() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	method := handler.EthGetBlockByNumber
	block := fixtures.MustReadFile("controller/ethereum/eth_header.json")
	s.jsonrpcClient.EXPECT().
		Call(gomock.Any(), method, gomock.Any()).
		Return(&jsonrpc.Response{
			Result: block,
		}, nil)

	anotherBlock := fixtures.MustReadFile("controller/ethereum/eth_header_no_block_time.json")
	s.receiver.EXPECT().
		GetBlockByNumber(ctx, validatorBlockNumber, false).
		Return(anotherBlock, nil)

	interceptor := s.validator.WithValidatorInterceptor(s.receiver)
	require.NotNil(interceptor)

	_, err := interceptor.GetBlockByNumber(ctx, validatorBlockNumber, false)
	require.NoError(err)
	s.verifyParityCounters(method, 0, 1, 0)
}

func (s *ValidatorTestSuite) TestGetBlockByNumber_Latest() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	method := handler.EthGetBlockByNumber

	block := fixtures.MustReadFile("controller/ethereum/eth_header.json")
	s.receiver.EXPECT().
		GetBlockByNumber(ctx, rpc.LatestBlockNumber, false).
		Return(block, nil)

	interceptor := s.validator.WithValidatorInterceptor(s.receiver)
	require.NotNil(interceptor)

	_, err := interceptor.GetBlockByNumber(ctx, rpc.LatestBlockNumber, false)
	require.NoError(err)
	close(s.validatorResult.Done)
	s.verifyParityCounters(method, 0, 0, 0)
}

func (s *ValidatorTestSuite) TestGetBlockByHash() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	method := handler.EthGetBlockByHash
	block := fixtures.MustReadFile("controller/ethereum/eth_header.json")
	s.jsonrpcClient.EXPECT().
		Call(gomock.Any(), method, gomock.Any()).
		Return(&jsonrpc.Response{
			Result: block,
		}, nil)

	s.receiver.EXPECT().
		GetBlockByHash(ctx, validatorBlockHash, false).
		Return(block, nil)

	interceptor := s.validator.WithValidatorInterceptor(s.receiver)
	require.NotNil(interceptor)

	_, err := interceptor.GetBlockByHash(ctx, validatorBlockHash, false)
	require.NoError(err)
	s.verifyParityCounters(method, 1, 0, 0)
}

func (s *ValidatorTestSuite) TestGetBlockByHash_WithoutFullTx() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	method := handler.EthGetBlockByHash
	block := fixtures.MustReadFile("controller/ethereum/eth_header_without_full_tx.json")
	s.jsonrpcClient.EXPECT().
		Call(gomock.Any(), method, gomock.Any()).
		Return(&jsonrpc.Response{
			Result: block,
		}, nil)

	s.receiver.EXPECT().
		GetBlockByHash(ctx, validatorBlockHash, false).
		Return(block, nil)

	interceptor := s.validator.WithValidatorInterceptor(s.receiver)
	require.NotNil(interceptor)

	_, err := interceptor.GetBlockByHash(ctx, validatorBlockHash, false)
	require.NoError(err)
	s.verifyParityCounters(method, 1, 0, 0)
}

func (s *ValidatorTestSuite) TestGetBlockByHash_FilterChainId() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	method := handler.EthGetBlockByHash
	block := fixtures.MustReadFile("controller/ethereum/eth_header.json")
	s.jsonrpcClient.EXPECT().
		Call(gomock.Any(), method, gomock.Any()).
		Return(&jsonrpc.Response{
			Result: block,
		}, nil)

	actualBlock := fixtures.MustReadFile("controller/ethereum/eth_header_chainId_0x0.json")
	s.receiver.EXPECT().
		GetBlockByHash(ctx, validatorBlockHash, false).
		Return(actualBlock, nil)

	interceptor := s.validator.WithValidatorInterceptor(s.receiver)
	require.NotNil(interceptor)

	_, err := interceptor.GetBlockByHash(ctx, validatorBlockHash, false)
	require.NoError(err)
	s.verifyParityCounters(method, 1, 0, 0)
}

func (s *ValidatorTestSuite) TestGetBlockTransactionCountByHash() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	method := handler.EthGetBlockTransactionCountByHash
	numberOfTransactions := `"0x11a"`
	s.jsonrpcClient.EXPECT().
		Call(gomock.Any(), method, gomock.Any()).
		Return(&jsonrpc.Response{
			Result: json.RawMessage(numberOfTransactions),
		}, nil)

	s.receiver.EXPECT().
		GetBlockTransactionCountByHash(ctx, validatorBlockHash).
		Return(json.RawMessage(numberOfTransactions), nil)

	interceptor := s.validator.WithValidatorInterceptor(s.receiver)
	require.NotNil(interceptor)

	_, err := interceptor.GetBlockTransactionCountByHash(ctx, validatorBlockHash)
	require.NoError(err)
	s.verifyParityCounters(method, 1, 0, 0)
}

func (s *ValidatorTestSuite) TestGetBlockTransactionCountByNumber() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	method := handler.EthGetBlockTransactionCountByNumber
	numberOfTransactions := `"0x11a"`
	s.jsonrpcClient.EXPECT().
		Call(gomock.Any(), method, gomock.Any()).
		Return(&jsonrpc.Response{
			Result: json.RawMessage(numberOfTransactions),
		}, nil)

	s.receiver.EXPECT().
		GetBlockTransactionCountByNumber(ctx, validatorBlockNumber).
		Return(json.RawMessage(numberOfTransactions), nil)

	interceptor := s.validator.WithValidatorInterceptor(s.receiver)
	require.NotNil(interceptor)

	_, err := interceptor.GetBlockTransactionCountByNumber(ctx, validatorBlockNumber)
	require.NoError(err)
	s.verifyParityCounters(method, 1, 0, 0)
}

func (s *ValidatorTestSuite) TestGetBlockTransactionCountByNumber_Pending() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	method := handler.EthGetBlockTransactionCountByNumber
	numberOfTransactions := `"0x11a"`

	s.receiver.EXPECT().
		GetBlockTransactionCountByNumber(ctx, rpc.PendingBlockNumber).
		Return(json.RawMessage(numberOfTransactions), nil)

	interceptor := s.validator.WithValidatorInterceptor(s.receiver)
	require.NotNil(interceptor)

	_, err := interceptor.GetBlockTransactionCountByNumber(ctx, rpc.PendingBlockNumber)
	require.NoError(err)
	close(s.validatorResult.Done)
	s.verifyParityCounters(method, 0, 0, 0)
}

func (s *ValidatorTestSuite) TestGetBlockTransactionCountByNumber_Latest() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	method := handler.EthGetBlockTransactionCountByNumber
	numberOfTransactions := `"0x11a"`

	s.receiver.EXPECT().
		GetBlockTransactionCountByNumber(ctx, rpc.LatestBlockNumber).
		Return(json.RawMessage(numberOfTransactions), nil)

	interceptor := s.validator.WithValidatorInterceptor(s.receiver)
	require.NotNil(interceptor)

	_, err := interceptor.GetBlockTransactionCountByNumber(ctx, rpc.LatestBlockNumber)
	require.NoError(err)
	close(s.validatorResult.Done)
	s.verifyParityCounters(method, 0, 0, 0)
}

func (s *ValidatorTestSuite) TestGetUncleCountByBlockHash() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	method := handler.EthGetUncleCountByBlockHash
	numberOfUncles := `"0x11a"`
	s.jsonrpcClient.EXPECT().
		Call(gomock.Any(), method, gomock.Any()).
		Return(&jsonrpc.Response{
			Result: json.RawMessage(numberOfUncles),
		}, nil)

	s.receiver.EXPECT().
		GetUncleCountByBlockHash(ctx, validatorBlockHash).
		Return(json.RawMessage(numberOfUncles), nil)

	interceptor := s.validator.WithValidatorInterceptor(s.receiver)
	require.NotNil(interceptor)

	_, err := interceptor.GetUncleCountByBlockHash(ctx, validatorBlockHash)
	require.NoError(err)
	s.verifyParityCounters(method, 1, 0, 0)
}

func (s *ValidatorTestSuite) TestGetUncleCountByBlockNumber() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	method := handler.EthGetUncleCountByBlockNumber
	numberOfUncles := `"0x11a"`
	s.jsonrpcClient.EXPECT().
		Call(gomock.Any(), method, gomock.Any()).
		Return(&jsonrpc.Response{
			Result: json.RawMessage(numberOfUncles),
		}, nil)

	s.receiver.EXPECT().
		GetUncleCountByBlockNumber(ctx, validatorBlockNumber).
		Return(json.RawMessage(numberOfUncles), nil)

	interceptor := s.validator.WithValidatorInterceptor(s.receiver)
	require.NotNil(interceptor)

	_, err := interceptor.GetUncleCountByBlockNumber(ctx, validatorBlockNumber)
	require.NoError(err)
	s.verifyParityCounters(method, 1, 0, 0)
}

func (s *ValidatorTestSuite) TestGetUncleCountByBlockNumber_Pending() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	method := handler.EthGetUncleCountByBlockNumber
	numberOfUncles := `"0x11a"`

	s.receiver.EXPECT().
		GetUncleCountByBlockNumber(ctx, rpc.PendingBlockNumber).
		Return(json.RawMessage(numberOfUncles), nil)

	interceptor := s.validator.WithValidatorInterceptor(s.receiver)
	require.NotNil(interceptor)

	_, err := interceptor.GetUncleCountByBlockNumber(ctx, rpc.PendingBlockNumber)
	require.NoError(err)
	close(s.validatorResult.Done)
	s.verifyParityCounters(method, 0, 0, 0)
}

func (s *ValidatorTestSuite) TestGetUncleCountByBlockNumber_Latest() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	method := handler.EthGetUncleCountByBlockNumber
	numberOfUncles := `"0x11a"`

	s.receiver.EXPECT().
		GetUncleCountByBlockNumber(ctx, rpc.LatestBlockNumber).
		Return(json.RawMessage(numberOfUncles), nil)

	interceptor := s.validator.WithValidatorInterceptor(s.receiver)
	require.NotNil(interceptor)

	_, err := interceptor.GetUncleCountByBlockNumber(ctx, rpc.LatestBlockNumber)
	require.NoError(err)
	close(s.validatorResult.Done)
	s.verifyParityCounters(method, 0, 0, 0)
}

func (s *ValidatorTestSuite) TestGetBalance() {
	require := testutil.Require(s.T())
	method := handler.EthGetBalance
	s.receiver.EXPECT().
		GetBalance(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(json.RawMessage{}, nil)

	interceptor := s.validator.WithValidatorInterceptor(s.receiver)
	require.NotNil(interceptor)

	ctx := context.Background()
	_, err := interceptor.GetBalance(ctx, nil, validatorBlockNumber)
	require.NoError(err)
	close(s.validatorResult.Done)
	s.verifyParityCounters(method, 0, 0, 0)
}

func (s *ValidatorTestSuite) TestGetCode() {
	require := testutil.Require(s.T())

	method := handler.EthGetCode
	s.receiver.EXPECT().
		GetCode(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(json.RawMessage{}, nil)

	interceptor := s.validator.WithValidatorInterceptor(s.receiver)
	require.NotNil(interceptor)

	ctx := context.Background()
	_, err := interceptor.GetCode(ctx, nil, validatorBlockNumber)
	require.NoError(err)
	close(s.validatorResult.Done)
	s.verifyParityCounters(method, 0, 0, 0)
}

func (s *ValidatorTestSuite) TestGetLogs_FromBlockAndToBlock() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	method := handler.EthGetLogs
	logs := fixtures.MustReadFile("controller/ethereum/eth_logs_1.json")
	s.jsonrpcClient.EXPECT().
		Call(gomock.Any(), method, gomock.Any()).
		Return(&jsonrpc.Response{
			Result: logs,
		}, nil)

	filterCriteria := handler.FilterCriteria{
		FromBlock: big.NewInt(int64(validatorBlockNumber)),
		ToBlock:   big.NewInt(int64(validatorBlockNumber)),
	}

	s.receiver.EXPECT().
		GetLogs(ctx, filterCriteria).
		Return(logs, nil)

	interceptor := s.validator.WithValidatorInterceptor(s.receiver)
	require.NotNil(interceptor)

	_, err := interceptor.GetLogs(ctx, filterCriteria)
	require.NoError(err)
	s.verifyParityCounters(method, 1, 0, 0)
}

func (s *ValidatorTestSuite) TestGetLogs_BlockHash() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	method := handler.EthGetLogs
	logs := fixtures.MustReadFile("controller/ethereum/eth_logs_1.json")
	s.jsonrpcClient.EXPECT().
		Call(gomock.Any(), method, gomock.Any()).
		Return(&jsonrpc.Response{
			Result: logs,
		}, nil)

	filterCriteria := handler.FilterCriteria{
		BlockHash: &validatorBlockHash,
	}

	s.receiver.EXPECT().
		GetLogs(ctx, filterCriteria).
		Return(logs, nil)

	interceptor := s.validator.WithValidatorInterceptor(s.receiver)
	require.NotNil(interceptor)

	_, err := interceptor.GetLogs(ctx, filterCriteria)
	require.NoError(err)
	s.verifyParityCounters(method, 1, 0, 0)
}

func (s *ValidatorTestSuite) TestGetLogs_EmptyParam() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	method := handler.EthGetLogs
	logs := fixtures.MustReadFile("controller/ethereum/eth_logs_1.json")

	s.receiver.EXPECT().
		GetLogs(ctx, handler.FilterCriteria{}).
		Return(logs, nil)

	interceptor := s.validator.WithValidatorInterceptor(s.receiver)
	require.NotNil(interceptor)

	_, err := interceptor.GetLogs(ctx, handler.FilterCriteria{})
	require.NoError(err)
	close(s.validatorResult.Done)
	s.verifyParityCounters(method, 0, 0, 0)
}

func (s *ValidatorTestSuite) TestGetLogs_Latest() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	method := handler.EthGetLogs
	logs := fixtures.MustReadFile("controller/ethereum/eth_logs_1.json")

	filterCriteria := handler.FilterCriteria{
		FromBlock: big.NewInt(rpc.LatestBlockNumber.Int64()),
		ToBlock:   big.NewInt(rpc.LatestBlockNumber.Int64()),
	}

	s.receiver.EXPECT().
		GetLogs(ctx, filterCriteria).
		Return(logs, nil)

	interceptor := s.validator.WithValidatorInterceptor(s.receiver)
	require.NotNil(interceptor)

	_, err := interceptor.GetLogs(ctx, filterCriteria)
	require.NoError(err)
	close(s.validatorResult.Done)
	s.verifyParityCounters(method, 0, 0, 0)
}

func (s *ValidatorTestSuite) TestGetTransactionCount() {
	require := testutil.Require(s.T())
	method := handler.EthGetTransactionCount
	s.receiver.EXPECT().
		GetTransactionCount(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(json.RawMessage{}, nil)

	interceptor := s.validator.WithValidatorInterceptor(s.receiver)
	require.NotNil(interceptor)

	ctx := context.Background()
	_, err := interceptor.GetTransactionCount(ctx, nil, validatorBlockNumber)
	require.NoError(err)
	close(s.validatorResult.Done)
	s.verifyParityCounters(method, 0, 0, 0)
}

func (s *ValidatorTestSuite) TestGetTransactionByHash() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	method := handler.EthGetTransactionByHash
	tx := fixtures.MustReadFile("controller/ethereum/eth_transaction_1.json")
	s.jsonrpcClient.EXPECT().
		Call(gomock.Any(), method, gomock.Any()).
		Return(&jsonrpc.Response{
			Result: tx,
		}, nil)

	s.receiver.EXPECT().
		GetTransactionByHash(ctx, validatorTxHash).
		Return(tx, nil)

	interceptor := s.validator.WithValidatorInterceptor(s.receiver)
	require.NotNil(interceptor)

	_, err := interceptor.GetTransactionByHash(ctx, validatorTxHash)
	require.NoError(err)
	s.verifyParityCounters(method, 1, 0, 0)
}

func (s *ValidatorTestSuite) TestGetTransactionReceipt() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	method := handler.EthGetTransactionReceipt
	receipt := fixtures.MustReadFile("controller/ethereum/eth_transactionreceipt_1.json")
	s.jsonrpcClient.EXPECT().
		Call(gomock.Any(), method, gomock.Any()).
		Return(&jsonrpc.Response{
			Result: receipt,
		}, nil)

	s.receiver.EXPECT().
		GetTransactionReceipt(ctx, validatorTxHash).
		Return(receipt, nil)

	interceptor := s.validator.WithValidatorInterceptor(s.receiver)
	require.NotNil(interceptor)

	_, err := interceptor.GetTransactionReceipt(ctx, validatorTxHash)
	require.NoError(err)
	s.verifyParityCounters(method, 1, 0, 0)
}

func (s *ValidatorTestSuite) TestGetTransactionByBlockHashAndIndex() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	method := handler.EthGetTransactionByBlockHashAndIndex
	tx := fixtures.MustReadJson("controller/ethereum/eth_transaction_1.json")

	s.jsonrpcClient.EXPECT().
		Call(gomock.Any(), method, gomock.Any()).
		Return(&jsonrpc.Response{
			Result: tx,
		}, nil)

	s.receiver.EXPECT().
		GetTransactionByBlockHashAndIndex(ctx, validatorBlockHash, rpc.DecimalOrHex(0)).
		Return(tx, nil)

	interceptor := s.validator.WithValidatorInterceptor(s.receiver)
	require.NotNil(interceptor)

	_, err := interceptor.GetTransactionByBlockHashAndIndex(ctx, validatorBlockHash, rpc.DecimalOrHex(0))
	require.NoError(err)
	s.verifyParityCounters(method, 1, 0, 0)
}

func (s *ValidatorTestSuite) TestGetTransactionByBlockNumberAndIndex() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	method := handler.EthGetTransactionByBlockNumberAndIndex
	tx := fixtures.MustReadJson("controller/ethereum/eth_transaction_1.json")

	s.jsonrpcClient.EXPECT().
		Call(gomock.Any(), method, gomock.Any()).
		Return(&jsonrpc.Response{
			Result: tx,
		}, nil)

	s.receiver.EXPECT().
		GetTransactionByBlockNumberAndIndex(ctx, validatorBlockNumber, rpc.DecimalOrHex(0)).
		Return(tx, nil)

	interceptor := s.validator.WithValidatorInterceptor(s.receiver)
	require.NotNil(interceptor)

	_, err := interceptor.GetTransactionByBlockNumberAndIndex(ctx, validatorBlockNumber, rpc.DecimalOrHex(0))
	require.NoError(err)
	s.verifyParityCounters(method, 1, 0, 0)
}

func (s *ValidatorTestSuite) TestGetTransactionByBlockNumberAndIndex_Pending() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	method := handler.EthGetTransactionByBlockNumberAndIndex
	tx := fixtures.MustReadJson("controller/ethereum/eth_transaction_1.json")

	s.receiver.EXPECT().
		GetTransactionByBlockNumberAndIndex(ctx, rpc.PendingBlockNumber, rpc.DecimalOrHex(0)).
		Return(tx, nil)

	interceptor := s.validator.WithValidatorInterceptor(s.receiver)
	require.NotNil(interceptor)

	_, err := interceptor.GetTransactionByBlockNumberAndIndex(ctx, rpc.PendingBlockNumber, rpc.DecimalOrHex(0))
	require.NoError(err)
	close(s.validatorResult.Done)
	s.verifyParityCounters(method, 0, 0, 0)
}

func (s *ValidatorTestSuite) TestGetTransactionByBlockNumberAndIndex_Latest() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	method := handler.EthGetBlockTransactionCountByNumber

	tx := fixtures.MustReadJson("controller/ethereum/eth_transaction_1.json")
	s.receiver.EXPECT().
		GetTransactionByBlockNumberAndIndex(ctx, rpc.LatestBlockNumber, rpc.DecimalOrHex(0)).
		Return(tx, nil)

	interceptor := s.validator.WithValidatorInterceptor(s.receiver)
	require.NotNil(interceptor)

	_, err := interceptor.GetTransactionByBlockNumberAndIndex(ctx, rpc.LatestBlockNumber, rpc.DecimalOrHex(0))
	require.NoError(err)
	close(s.validatorResult.Done)
	s.verifyParityCounters(method, 0, 0, 0)
}

func (s *ValidatorTestSuite) TestTraceBlockByHash() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	method := handler.DebugTraceBlockByHash
	trace := fixtures.MustReadFile("controller/ethereum/eth_trace.json")
	s.jsonrpcClient.EXPECT().
		Call(gomock.Any(), method, gomock.Any()).
		Return(&jsonrpc.Response{Result: trace}, nil)

	traceConfig := &tracers.TraceConfig{
		Tracer: pointer.String("callTracer"),
	}
	s.receiver.EXPECT().
		TraceBlockByHash(ctx, validatorBlockHash, traceConfig).
		Return(trace, nil)

	interceptor := s.validator.WithValidatorInterceptor(s.receiver)
	require.NotNil(interceptor)

	_, err := interceptor.TraceBlockByHash(ctx, validatorBlockHash, traceConfig)
	require.NoError(err)
	s.verifyParityCounters(method, 1, 0, 0)
}

func (s *ValidatorTestSuite) TestTraceBlockByNumber() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	method := handler.DebugTraceBlockByNumber
	trace := fixtures.MustReadFile("controller/ethereum/eth_trace.json")

	traceConfig := &tracers.TraceConfig{
		Tracer: pointer.String("callTracer"),
	}
	s.receiver.EXPECT().
		TraceBlockByNumber(ctx, validatorBlockNumber, traceConfig).
		Return(trace, nil)

	interceptor := s.validator.WithValidatorInterceptor(s.receiver)
	require.NotNil(interceptor)

	_, err := interceptor.TraceBlockByNumber(ctx, validatorBlockNumber, traceConfig)
	require.NoError(err)
	close(s.validatorResult.Done)
	s.verifyParityCounters(method, 0, 0, 0)
}

func (s *ValidatorTestSuite) TestGetAuthor_UnableToGetCachedCheckpoint_Matched() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	method := handler.BorGetAuthor

	result := `"0xadbs"`
	s.jsonrpcClient.EXPECT().
		Call(gomock.Any(), method, gomock.Any()).
		Return(&jsonrpc.Response{Result: json.RawMessage(result)}, nil)

	s.receiver.EXPECT().
		GetAuthor(ctx, validatorBlockNumber).
		Return(json.RawMessage(result), nil)

	interceptor := s.validator.WithValidatorInterceptor(s.receiver)
	require.NotNil(interceptor)

	_, err := interceptor.GetAuthor(ctx, validatorBlockNumber)
	require.NoError(err)
	s.verifyParityCounters(method, 1, 0, 0)
}

func (s *ValidatorTestSuite) TestGetAuthor_Latest_Skipped() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	method := handler.BorGetAuthor

	result := `"0xadbs"`
	s.receiver.EXPECT().
		GetAuthor(ctx, rpc.LatestBlockNumber).
		Return(json.RawMessage(result), nil)

	interceptor := s.validator.WithValidatorInterceptor(s.receiver)
	require.NotNil(interceptor)

	_, err := interceptor.GetAuthor(ctx, rpc.LatestBlockNumber)
	require.NoError(err)
	close(s.validatorResult.Done)
	s.verifyParityCounters(method, 0, 0, 1)
}

func (s *ValidatorTestSuite) TestGetAuthor_WithinIrreversibleDistance_Skipped() {
	require := testutil.Require(s.T())

	ctx := context.WithValue(context.Background(), constants.ContextKeyLatestCheckpoint, &api.Checkpoint{Height: 126})
	method := handler.BorGetAuthor

	result := `"0xadbs"`
	s.receiver.EXPECT().
		GetAuthor(gomock.Any(), gomock.Any()).
		Return(json.RawMessage(result), nil)

	interceptor := s.validator.WithValidatorInterceptor(s.receiver)
	require.NotNil(interceptor)

	_, err := interceptor.GetAuthor(ctx, 123)
	require.NoError(err)
	close(s.validatorResult.Done)
	s.verifyParityCounters(method, 0, 0, 1)
}

func (s *ValidatorTestSuite) TestGetAuthor_EqualToIrreversibleDistance_Matched() {
	require := testutil.Require(s.T())

	ctx := context.WithValue(context.Background(), constants.ContextKeyLatestCheckpoint, &api.Checkpoint{Height: 158})
	method := handler.BorGetAuthor

	result := `"0xadbs"`
	s.jsonrpcClient.EXPECT().
		Call(gomock.Any(), method, gomock.Any()).
		Return(&jsonrpc.Response{Result: json.RawMessage(result)}, nil)

	s.receiver.EXPECT().
		GetAuthor(ctx, gomock.Any()).
		Return(json.RawMessage(result), nil)

	interceptor := s.validator.WithValidatorInterceptor(s.receiver)
	require.NotNil(interceptor)

	_, err := interceptor.GetAuthor(ctx, 123)
	require.NoError(err)
	s.verifyParityCounters(method, 1, 0, 0)
}

func (s *ValidatorTestSuite) TestGetAuthor_OutOfIrreversibleDistance_Matched() {
	require := testutil.Require(s.T())

	ctx := context.WithValue(context.Background(), constants.ContextKeyLatestCheckpoint, &api.Checkpoint{Height: 200})
	method := handler.BorGetAuthor

	result := `"0xadbs"`
	s.jsonrpcClient.EXPECT().
		Call(gomock.Any(), method, gomock.Any()).
		Return(&jsonrpc.Response{Result: json.RawMessage(result)}, nil)

	s.receiver.EXPECT().
		GetAuthor(ctx, gomock.Any()).
		Return(json.RawMessage(result), nil)

	interceptor := s.validator.WithValidatorInterceptor(s.receiver)
	require.NotNil(interceptor)

	_, err := interceptor.GetAuthor(ctx, 123)
	require.NoError(err)
	s.verifyParityCounters(method, 1, 0, 0)
}

func (s *ValidatorTestSuite) TestTraceBlockByHash_Ethereum_FilterTxHash() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	method := handler.DebugTraceBlockByHash
	trace1 := fixtures.MustReadFile("controller/ethereum/eth_trace_with_txhash_1.json")
	trace2 := fixtures.MustReadFile("controller/ethereum/eth_trace_with_txhash_2.json")
	s.jsonrpcClient.EXPECT().
		Call(gomock.Any(), method, gomock.Any()).
		Return(&jsonrpc.Response{Result: trace1}, nil)

	traceConfig := &tracers.TraceConfig{
		Tracer: pointer.String("callTracer"),
	}
	s.receiver.EXPECT().
		TraceBlockByHash(ctx, validatorBlockHash, traceConfig).
		Return(trace2, nil)

	interceptor := s.validator.WithValidatorInterceptor(s.receiver)
	require.NotNil(interceptor)

	_, err := interceptor.TraceBlockByHash(ctx, validatorBlockHash, traceConfig)
	require.NoError(err)
	s.verifyParityCounters(method, 1, 0, 0)
}

func (s *ValidatorTestSuite) TestTraceBlockByHash_Polygon_FilterGasUsed() {
	require := testutil.Require(s.T())
	cfg, err := config.New(config.WithBlockchain(c3common.Blockchain_BLOCKCHAIN_POLYGON), config.WithNetwork(c3common.Network_NETWORK_POLYGON_MAINNET))
	require.NoError(err)

	// Always shadow.
	cfg.Controller.Handler.ShadowPercentage = 100
	cfg.Controller.Handler.MethodConfigs = []config.MethodConfig{}

	var deps struct {
		fx.In
		Validator handler.Validator
		Scope     tally.Scope
	}
	s.app = testapp.New(
		s.T(),
		testapp.WithConfig(cfg),
		fx.Provide(fx.Annotated{Name: "validator", Target: func() jsonrpc.Client { return s.jsonrpcClient }}),
		fx.Provide(handler.NewValidator),
		fx.Provide(func() *handler.ValidatorResult { return s.validatorResult }),
		fx.Populate(&deps),
	)
	s.config = ValidatorTestSuiteConfig{
		Blockchain: c3common.Blockchain_BLOCKCHAIN_POLYGON,
		Network:    c3common.Network_NETWORK_POLYGON_MAINNET,
	}
	s.validator = deps.Validator
	s.scope = deps.Scope.(tally.TestScope)

	ctx := context.Background()
	method := handler.DebugTraceBlockByHash
	trace1 := fixtures.MustReadFile("controller/ethereum/polygon_trace_with_txhash_1.json")
	trace2 := fixtures.MustReadFile("controller/ethereum/polygon_trace_with_txhash_2.json")
	s.jsonrpcClient.EXPECT().
		Call(gomock.Any(), method, gomock.Any()).
		Return(&jsonrpc.Response{Result: trace1}, nil)

	traceConfig := &tracers.TraceConfig{
		Tracer: pointer.String("callTracer"),
	}
	s.receiver.EXPECT().
		TraceBlockByHash(ctx, validatorBlockHash, traceConfig).
		Return(trace2, nil)

	interceptor := s.validator.WithValidatorInterceptor(s.receiver)
	require.NotNil(interceptor)

	_, err = interceptor.TraceBlockByHash(ctx, validatorBlockHash, traceConfig)
	require.NoError(err)
	s.verifyParityCounters(method, 1, 0, 0)
}

func (s *ValidatorTestSuite) TestProtocolVersion() {
	require := testutil.Require(s.T())
	method := handler.EthProtocolVersion
	s.receiver.EXPECT().
		ProtocolVersion(gomock.Any()).
		Return(json.RawMessage{}, nil)

	interceptor := s.validator.WithValidatorInterceptor(s.receiver)
	require.NotNil(interceptor)

	ctx := context.Background()
	_, err := interceptor.ProtocolVersion(ctx)
	require.NoError(err)
	close(s.validatorResult.Done)
	s.verifyParityCounters(method, 0, 0, 0)
}

func (s *ValidatorTestSuite) TestSyncing() {
	require := testutil.Require(s.T())
	method := handler.EthSyncing
	s.receiver.EXPECT().
		Syncing(gomock.Any()).
		Return(json.RawMessage{}, nil)

	interceptor := s.validator.WithValidatorInterceptor(s.receiver)
	require.NotNil(interceptor)

	ctx := context.Background()
	_, err := interceptor.Syncing(ctx)
	require.NoError(err)
	close(s.validatorResult.Done)
	s.verifyParityCounters(method, 0, 0, 0)
}

func (s *ValidatorTestSuite) TestFeeHistory() {
	require := testutil.Require(s.T())
	method := handler.EthFeeHistory
	s.receiver.EXPECT().
		FeeHistory(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(json.RawMessage{}, nil)

	interceptor := s.validator.WithValidatorInterceptor(s.receiver)
	require.NotNil(interceptor)

	ctx := context.Background()
	_, err := interceptor.FeeHistory(ctx, nil, nil, nil)
	require.NoError(err)
	close(s.validatorResult.Done)
	s.verifyParityCounters(method, 0, 0, 0)
}

func (s *ValidatorTestSuite) TestListening() {
	require := testutil.Require(s.T())
	method := handler.NetListening
	s.receiver.EXPECT().
		Listening(gomock.Any()).
		Return(json.RawMessage{}, nil)

	interceptor := s.validator.WithValidatorInterceptor(s.receiver)
	require.NotNil(interceptor)

	ctx := context.Background()
	_, err := interceptor.Listening(ctx)
	require.NoError(err)
	close(s.validatorResult.Done)
	s.verifyParityCounters(method, 0, 0, 0)
}

func (s *ValidatorTestSuite) TestPeerCount() {
	require := testutil.Require(s.T())
	method := handler.NetPeerCount
	s.receiver.EXPECT().
		PeerCount(gomock.Any()).
		Return(json.RawMessage{}, nil)

	interceptor := s.validator.WithValidatorInterceptor(s.receiver)
	require.NotNil(interceptor)

	ctx := context.Background()
	_, err := interceptor.PeerCount(ctx)
	require.NoError(err)
	close(s.validatorResult.Done)
	s.verifyParityCounters(method, 0, 0, 0)
}

func (s *ValidatorTestSuite) verifyParityCounters(method *jsonrpc.RequestMethod, matched int, unmatched int, skipped int) {
	<-s.validatorResult.Done
	require := testutil.Require(s.T())
	require.Equal(matched, s.getParityCounter(method, "matched"))
	require.Equal(unmatched, s.getParityCounter(method, "unmatched"))
	require.Equal(skipped, s.getParityCounter(method, "skipped"))
	require.Equal(0, s.getParityCounter(method, "error"))
}

func (s *ValidatorTestSuite) getParityCounter(method *jsonrpc.RequestMethod, resultType string) int {
	severity := handler.GetMethodSeverity(method)
	snapshot := s.scope.Snapshot()
	key := fmt.Sprintf("chainnode.validator.parity+method=%v,result_type=%v,severity=%v", method.Name, resultType, severity)
	counter := snapshot.Counters()[key]
	if counter == nil {
		return 0
	}

	return int(counter.Value())
}
