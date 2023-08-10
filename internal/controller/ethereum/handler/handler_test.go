package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/eth/tracers"
	"github.com/golang/mock/gomock"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"
	"golang.org/x/xerrors"
	"google.golang.org/grpc/metadata"

	c3common "github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	chainstorageapi "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"

	"github.com/coinbase/chainnode/internal/api"
	"github.com/coinbase/chainnode/internal/api/ethereum"
	"github.com/coinbase/chainnode/internal/clients/blockchain/jsonrpc"
	jsonrpcmocks "github.com/coinbase/chainnode/internal/clients/blockchain/jsonrpc/mocks"
	"github.com/coinbase/chainnode/internal/config"
	"github.com/coinbase/chainnode/internal/controller/internal"
	controllermocks "github.com/coinbase/chainnode/internal/controller/mocks"
	"github.com/coinbase/chainnode/internal/server/rpc"
	"github.com/coinbase/chainnode/internal/storage"
	ethereumStorage "github.com/coinbase/chainnode/internal/storage/ethereum"
	storagemock "github.com/coinbase/chainnode/internal/storage/mocks"
	"github.com/coinbase/chainnode/internal/utils/constants"
	"github.com/coinbase/chainnode/internal/utils/fixtures"
	"github.com/coinbase/chainnode/internal/utils/pointer"
	"github.com/coinbase/chainnode/internal/utils/testapp"
	"github.com/coinbase/chainnode/internal/utils/testutil"
)

var (
	sampleCallOutput           = "0x123456"
	sampleCallOutputMarshal, _ = json.Marshal(sampleCallOutput)

	sampleCallResp = &jsonrpc.Response{
		Result: sampleCallOutputMarshal,
		Error:  nil,
	}
	sampleCallRespWithError = &jsonrpc.Response{
		Result: nil,
		Error: &jsonrpc.RPCError{
			Code:    -32099,
			Message: "insufficient funds",
			Data:    nil,
		},
	}

	sampleGetCodeOutput           = "0x600160008035811a818181146012578301005b601b6001356025565b8060005260206000f25b600060078202905091905056"
	sampleGetCodeOutputMarshal, _ = json.Marshal(sampleGetCodeOutput)
	sampleGetCodeResp             = &jsonrpc.Response{
		Result: sampleGetCodeOutputMarshal,
		Error:  nil,
	}
	sampleGetCodeRespWithError = &jsonrpc.Response{
		Result: nil,
		Error: &jsonrpc.RPCError{
			Code:    -32099,
			Message: "foo",
			Data:    nil,
		},
	}

	sampleGetBalanceOutput           = "0x1bdd2b5ec7100"
	sampleGetBalanceOutputMarshal, _ = json.Marshal(sampleGetBalanceOutput)
	sampleGetBalanceResp             = &jsonrpc.Response{
		Result: sampleGetBalanceOutputMarshal,
		Error:  nil,
	}
	sampleGetBalanceRespWithError = &jsonrpc.Response{
		Result: nil,
		Error: &jsonrpc.RPCError{
			Code:    -32099,
			Message: "foo",
			Data:    nil,
		},
	}

	sampleGetTransactionCountOutput     = "0x6e"
	sampleGetTransactionCountMarshal, _ = json.Marshal(sampleGetTransactionCountOutput)
	sampleGetTransactionCountResp       = &jsonrpc.Response{
		Result: sampleGetTransactionCountMarshal,
		Error:  nil,
	}

	sampleGetUncleCountOutput     = "0x1"
	sampleGetUncleCountMarshal, _ = json.Marshal(sampleGetTransactionCountOutput)
	sampleGetUncleCountResp       = &jsonrpc.Response{
		Result: sampleGetUncleCountMarshal,
		Error:  nil,
	}

	sampleBlockHash = "0xb3e232495a99170e43c583daa9035a993bd66ddfad5ccc636b6aad26e6e38056"
	sampleTxHash    = "0xe67071db25331ea3a92a4e28b516c95f2d5b62b68329b70386c19e00807f51d8"
)

type handlerTestSuite struct {
	suite.Suite
	blockchain      c3common.Blockchain
	network         c3common.Network
	ctrl            *gomock.Controller
	app             testapp.TestApp
	handler         internal.Handler
	receiver        Receiver
	config          *config.Config
	jsonrpcClient   *jsonrpcmocks.MockClient
	ethereumStorage *storagemock.MockEthereumStorage
	checkpointer    *controllermocks.MockCheckpointer
}

func TestHandlerSuite_Ethereum(t *testing.T) {
	suite.Run(t, &handlerTestSuite{
		blockchain: c3common.Blockchain_BLOCKCHAIN_ETHEREUM,
		network:    c3common.Network_NETWORK_ETHEREUM_MAINNET,
	})
}

func TestHandlerSuite_Polygon(t *testing.T) {
	suite.Run(t, &handlerTestSuite{
		blockchain: c3common.Blockchain_BLOCKCHAIN_POLYGON,
		network:    c3common.Network_NETWORK_POLYGON_MAINNET,
	})
}

func (s *handlerTestSuite) SetupTest() {
	s.ctrl = gomock.NewController(s.T())

	s.jsonrpcClient = jsonrpcmocks.NewMockClient(s.ctrl)
	s.ethereumStorage = storagemock.NewMockEthereumStorage(s.ctrl)
	s.checkpointer = controllermocks.NewMockCheckpointer(s.ctrl)

	cfg, err := config.New(config.WithBlockchain(s.blockchain), config.WithNetwork(s.network))
	s.Require().NoError(err)

	// Disable shadowing.
	cfg.Controller.Handler.ShadowPercentage = 0

	s.app = testapp.New(
		s.T(),
		testapp.WithConfig(cfg),
		fx.Provide(fx.Annotated{Name: "proxy", Target: func() jsonrpc.Client { return s.jsonrpcClient }}),
		fx.Provide(fx.Annotated{Name: "validator", Target: func() jsonrpc.Client { return s.jsonrpcClient }}),
		fx.Provide(func() ethereumStorage.BlockStorage { return s.ethereumStorage }),
		fx.Provide(func() ethereumStorage.BlockByHashStorage { return s.ethereumStorage }),
		fx.Provide(func() ethereumStorage.BlockByHashWithoutFullTxStorage { return s.ethereumStorage }),
		fx.Provide(func() ethereumStorage.LogStorageV2 { return s.ethereumStorage }),
		fx.Provide(func() ethereumStorage.TransactionStorage { return s.ethereumStorage }),
		fx.Provide(func() ethereumStorage.TransactionReceiptStorage { return s.ethereumStorage }),
		fx.Provide(func() ethereumStorage.TraceByHashStorage { return s.ethereumStorage }),
		fx.Provide(func() ethereumStorage.TraceByNumberStorage { return s.ethereumStorage }),
		fx.Provide(func() ethereumStorage.ArbtraceBlockStorage { return s.ethereumStorage }),
		fx.Provide(func() ethereumStorage.BlockExtraDataByNumberStorage { return s.ethereumStorage }),
		fx.Provide(func() internal.Checkpointer { return s.checkpointer }),
		fx.Provide(NewHandler),
		fx.Provide(NewValidator),
		fx.Populate(&s.handler),
		fx.Populate(&s.config),
	)
	s.receiver = s.handler.Receiver().(Receiver)
}

func (s *handlerTestSuite) TearDownTest() {
	s.app.Close()
	s.ctrl.Finish()
}

func (s *handlerTestSuite) TestCall() {
	require := testutil.Require(s.T())
	s.jsonrpcClient.EXPECT().Call(gomock.Any(), EthCall, gomock.Any()).Times(1).DoAndReturn(
		func(ctx context.Context, method *jsonrpc.RequestMethod, params jsonrpc.Params) (*jsonrpc.Response, error) {
			require.Len(params, 2)
			return sampleCallResp, nil
		},
	)

	resp, err := s.receiver.Call(context.Background(), nil, nil)
	require.NoError(err)
	require.Equal(string(sampleCallOutputMarshal), string(resp))
}

func (s *handlerTestSuite) TestCall_Error() {
	require := testutil.Require(s.T())
	s.jsonrpcClient.EXPECT().Call(gomock.Any(), EthCall, gomock.Any()).Times(1).DoAndReturn(
		func(ctx context.Context, method *jsonrpc.RequestMethod, params jsonrpc.Params) (*jsonrpc.Response, error) {
			require.Len(params, 2)
			return sampleCallRespWithError, jsonrpc.NewRPCError(errorCodeGeneric, sampleCallRespWithError.Error.Message)
		},
	)

	resp, err := s.receiver.Call(context.Background(), nil, nil)
	require.Error(err)
	require.Equal("insufficient funds", err.Error())
	require.Nil(resp)

	rpcError, ok := err.(rpc.Error)
	require.True(ok)
	require.Equal(errorCodeGeneric, rpcError.ErrorCode())
}

func (s *handlerTestSuite) TestCall_Fail_NativeOnly() {
	require := testutil.Require(s.T())
	resp, err := s.receiver.Call(newContextWithMode(constants.NativeOnlyMode), nil, nil)
	require.Equal(xerrors.Errorf(invalidDispatchModeErrString, "eth_call", api.ErrNotAllowed).Error(), err.Error())
	s.validateErrorCode(errorCodeBadRequest, err)
	require.Nil(resp)
}

func (s *handlerTestSuite) TestBlockNumber() {
	require := testutil.Require(s.T())

	height := uint64(123)
	heightHex := hexutil.EncodeUint64(height)
	s.checkpointer.EXPECT().Get(gomock.Any(), api.CollectionLatestCheckpoint, s.config.Tag.Stable).Return(&api.Checkpoint{Height: height}, nil)

	resp, err := s.receiver.BlockNumber(context.Background())
	require.NoError(err)
	var respStr string
	err = json.Unmarshal(resp, &respStr)
	require.NoError(err)
	require.Equal(heightHex, respStr)
}

func (s *handlerTestSuite) TestGetBlockByHash() {
	require := testutil.Require(s.T())

	header, err := fixtures.ReadFile("controller/ethereum/eth_header.json")
	require.NoError(err)

	hash := common.HexToHash(sampleBlockHash)
	maxSequence := api.Sequence(1)
	s.checkpointer.EXPECT().Get(gomock.Any(), api.CollectionLatestCheckpoint, s.config.Tag.Stable).Return(&api.Checkpoint{Sequence: maxSequence}, nil)
	s.ethereumStorage.EXPECT().GetBlockByHash(gomock.Any(), s.config.Tag.Stable, sampleBlockHash, maxSequence).Return(&ethereum.Block{Data: header}, nil)

	resp, err := s.receiver.GetBlockByHash(context.Background(), hash, true)
	require.NoError(err)
	require.Equal(string(header), string(resp))
}

func (s *handlerTestSuite) TestGetBlockByHash_NotFound() {
	require := testutil.Require(s.T())

	hash := common.HexToHash(sampleBlockHash)
	maxSequence := api.Sequence(1)
	s.checkpointer.EXPECT().Get(gomock.Any(), api.CollectionLatestCheckpoint, s.config.Tag.Stable).Return(&api.Checkpoint{Sequence: maxSequence}, nil)
	s.ethereumStorage.EXPECT().GetBlockByHash(gomock.Any(), s.config.Tag.Stable, sampleBlockHash, maxSequence).Return(nil, storage.ErrItemNotFound)

	resp, err := s.receiver.GetBlockByHash(context.Background(), hash, true)
	require.NoError(err)
	require.Nil(resp)
}

func (s *handlerTestSuite) TestGetBlockByHashWithoutFullTx() {
	require := testutil.Require(s.T())

	header, err := fixtures.ReadFile("controller/ethereum/eth_header_without_full_tx.json")
	require.NoError(err)

	hash := common.HexToHash(sampleBlockHash)
	maxSequence := api.Sequence(1)
	s.checkpointer.EXPECT().Get(gomock.Any(), api.CollectionLatestCheckpoint, s.config.Tag.Stable).Return(&api.Checkpoint{Sequence: maxSequence}, nil)
	s.ethereumStorage.EXPECT().GetBlockByHashWithoutFullTx(gomock.Any(), s.config.Tag.Stable, sampleBlockHash, maxSequence).Return(&ethereum.Block{Data: header}, nil)

	resp, err := s.receiver.GetBlockByHash(context.Background(), hash, false)
	require.NoError(err)
	require.Equal(string(header), string(resp))
}

func (s *handlerTestSuite) TestGetBlockByHashWithoutFullTx_NotFound() {
	require := testutil.Require(s.T())

	hash := common.HexToHash(sampleBlockHash)
	maxSequence := api.Sequence(1)
	s.checkpointer.EXPECT().Get(gomock.Any(), api.CollectionLatestCheckpoint, s.config.Tag.Stable).Return(&api.Checkpoint{Sequence: maxSequence}, nil)
	s.ethereumStorage.EXPECT().GetBlockByHashWithoutFullTx(gomock.Any(), s.config.Tag.Stable, sampleBlockHash, maxSequence).Return(nil, storage.ErrItemNotFound)

	resp, err := s.receiver.GetBlockByHash(context.Background(), hash, false)
	require.NoError(err)
	require.Nil(resp)
}

func (s *handlerTestSuite) TestGetBlockByNumber() {
	require := testutil.Require(s.T())

	header, err := fixtures.ReadFile("controller/ethereum/eth_header.json")
	require.NoError(err)

	maxSequence := api.Sequence(1)
	height := uint64(123)
	s.checkpointer.EXPECT().Get(gomock.Any(), api.CollectionLatestCheckpoint, s.config.Tag.Stable).Return(&api.Checkpoint{Sequence: maxSequence}, nil)
	s.ethereumStorage.EXPECT().GetBlock(gomock.Any(), s.config.Tag.Stable, height, maxSequence).Return(&ethereum.Block{Data: header}, nil)

	resp, err := s.receiver.GetBlockByNumber(context.Background(), rpc.BlockNumber(height), true)
	require.NoError(err)
	require.Equal(string(header), string(resp))
}

func (s *handlerTestSuite) TestGetBlockByNumber_Success_NativeOnly_NonPending() {
	require := testutil.Require(s.T())

	header, err := fixtures.ReadFile("controller/ethereum/eth_header.json")
	require.NoError(err)

	maxSequence := api.Sequence(1)
	height := uint64(123)
	s.checkpointer.EXPECT().Get(gomock.Any(), api.CollectionLatestCheckpoint, s.config.Tag.Stable).Return(&api.Checkpoint{Sequence: maxSequence}, nil)
	s.ethereumStorage.EXPECT().GetBlock(gomock.Any(), s.config.Tag.Stable, height, maxSequence).Return(&ethereum.Block{Data: header}, nil)

	resp, err := s.receiver.GetBlockByNumber(newContextWithMode(constants.NativeOnlyMode), rpc.BlockNumber(height), true)
	require.NoError(err)
	require.Equal(string(header), string(resp))
}

func (s *handlerTestSuite) TestGetBlockByNumber_Fail_NativeOnly_Pending() {
	require := testutil.Require(s.T())
	resp, err := s.receiver.GetBlockByNumber(newContextWithMode(constants.NativeOnlyMode), rpc.PendingBlockNumber, true)
	require.Equal(xerrors.Errorf(invalidDispatchModeErrString, "eth_getBlockByNumber", api.ErrNotAllowed).Error(), err.Error())
	s.validateErrorCode(errorCodeBadRequest, err)
	require.Nil(resp)
}

func (s *handlerTestSuite) TestGetBlockTransactionCountByHash() {
	require := testutil.Require(s.T())

	expectedNumOfTxs := uint64(2)
	expectedNumOfTxsHex := hexutil.EncodeUint64(expectedNumOfTxs)

	header, err := fixtures.ReadFile("controller/ethereum/eth_header_without_full_tx.json")
	require.NoError(err)

	maxSequence := api.Sequence(1)
	hash := common.HexToHash(sampleBlockHash)
	s.checkpointer.EXPECT().Get(gomock.Any(), api.CollectionLatestCheckpoint, s.config.Tag.Stable).Return(&api.Checkpoint{Sequence: maxSequence}, nil)
	s.ethereumStorage.EXPECT().GetBlockByHashWithoutFullTx(gomock.Any(), s.config.Tag.Stable, sampleBlockHash, maxSequence).Return(&ethereum.Block{Data: header}, nil)

	resp, err := s.receiver.GetBlockTransactionCountByHash(context.Background(), hash)
	require.NoError(err)
	var respStr string
	err = json.Unmarshal(resp, &respStr)
	require.NoError(err)
	require.Equal(expectedNumOfTxsHex, respStr)
}

func (s *handlerTestSuite) TestGetBlockTransactionCountByNumber() {
	require := testutil.Require(s.T())

	expectedNumOfTxs := uint64(2)
	expectedNumOfTxsHex := hexutil.EncodeUint64(expectedNumOfTxs)

	header, err := fixtures.ReadFile("controller/ethereum/eth_header.json")
	require.NoError(err)

	maxSequence := api.Sequence(1)
	height := uint64(123)
	s.checkpointer.EXPECT().Get(gomock.Any(), api.CollectionLatestCheckpoint, s.config.Tag.Stable).Return(&api.Checkpoint{Sequence: maxSequence}, nil)
	s.ethereumStorage.EXPECT().GetBlock(gomock.Any(), s.config.Tag.Stable, height, maxSequence).Return(&ethereum.Block{Data: header}, nil)

	resp, err := s.receiver.GetBlockTransactionCountByNumber(context.Background(), rpc.BlockNumber(height))
	require.NoError(err)
	var respStr string
	err = json.Unmarshal(resp, &respStr)
	require.NoError(err)
	require.Equal(expectedNumOfTxsHex, respStr)
}

func (s *handlerTestSuite) TestGetBlockTransactionCountByNumber_Success_Latest() {
	require := testutil.Require(s.T())

	expectedNumOfTxs := uint64(2)
	expectedNumOfTxsHex := hexutil.EncodeUint64(expectedNumOfTxs)

	header, err := fixtures.ReadFile("controller/ethereum/eth_header.json")
	require.NoError(err)

	maxSequence := api.Sequence(1)
	latestBlockNumber := uint64(123)
	s.checkpointer.EXPECT().Get(gomock.Any(), api.CollectionLatestCheckpoint, s.config.Tag.Stable).Return(&api.Checkpoint{Sequence: maxSequence, Height: latestBlockNumber}, nil)
	s.ethereumStorage.EXPECT().GetBlock(gomock.Any(), s.config.Tag.Stable, latestBlockNumber, maxSequence).Return(&ethereum.Block{Data: header}, nil)

	resp, err := s.receiver.GetBlockTransactionCountByNumber(context.Background(), rpc.BlockNumber(-1))
	require.NoError(err)
	var respStr string
	err = json.Unmarshal(resp, &respStr)
	require.NoError(err)
	require.Equal(expectedNumOfTxsHex, respStr)
}

func (s *handlerTestSuite) TestGetBlockTransactionCountByNumber_Success_Pending() {
	require := testutil.Require(s.T())

	s.jsonrpcClient.EXPECT().Call(gomock.Any(), EthGetBlockTransactionCountByNumber, gomock.Any()).Times(1).DoAndReturn(
		func(ctx context.Context, method *jsonrpc.RequestMethod, params jsonrpc.Params) (*jsonrpc.Response, error) {
			require.Len(params, 1)
			return sampleGetTransactionCountResp, nil
		},
	)

	resp, err := s.receiver.GetBlockTransactionCountByNumber(context.Background(), rpc.PendingBlockNumber)
	require.NoError(err)
	var respStr string
	err = json.Unmarshal(resp, &respStr)
	require.NoError(err)
	require.Equal(string(sampleGetTransactionCountMarshal), string(resp))
}

func (s *handlerTestSuite) TestGetBlockTransactionCountByNumber_Success_NativeOnly_NonPending() {
	require := testutil.Require(s.T())

	expectedNumOfTxs := uint64(2)
	expectedNumOfTxsHex := hexutil.EncodeUint64(expectedNumOfTxs)

	header, err := fixtures.ReadFile("controller/ethereum/eth_header.json")
	require.NoError(err)

	maxSequence := api.Sequence(1)
	height := uint64(123)
	s.checkpointer.EXPECT().Get(gomock.Any(), api.CollectionLatestCheckpoint, s.config.Tag.Stable).Return(&api.Checkpoint{Sequence: maxSequence}, nil)
	s.ethereumStorage.EXPECT().GetBlock(gomock.Any(), s.config.Tag.Stable, height, maxSequence).Return(&ethereum.Block{Data: header}, nil)

	resp, err := s.receiver.GetBlockTransactionCountByNumber(newContextWithMode(constants.NativeOnlyMode), rpc.BlockNumber(height))
	require.NoError(err)
	var respStr string
	err = json.Unmarshal(resp, &respStr)
	require.NoError(err)
	require.Equal(expectedNumOfTxsHex, respStr)
}

func (s *handlerTestSuite) TestGetBlockTransactionCountByNumber_Fail_NativeOnly_Pending() {
	require := testutil.Require(s.T())
	resp, err := s.receiver.GetBlockTransactionCountByNumber(newContextWithMode(constants.NativeOnlyMode), rpc.PendingBlockNumber)
	require.Equal(xerrors.Errorf(invalidDispatchModeErrString, "eth_getBlockTransactionCountByNumber", api.ErrNotAllowed).Error(), err.Error())
	s.validateErrorCode(errorCodeBadRequest, err)
	require.Nil(resp)
}

func (s *handlerTestSuite) TestGetUncleCountByBlockHash() {
	require := testutil.Require(s.T())

	expectedNumOfUncles := uint64(1)
	expectedNumOfUnclesHex := hexutil.EncodeUint64(expectedNumOfUncles)

	header, err := fixtures.ReadFile("controller/ethereum/eth_header_with_uncles.json")
	require.NoError(err)

	maxSequence := api.Sequence(1)
	hash := common.HexToHash(sampleBlockHash)
	s.checkpointer.EXPECT().Get(gomock.Any(), api.CollectionLatestCheckpoint, s.config.Tag.Stable).Return(&api.Checkpoint{Sequence: maxSequence}, nil)
	s.ethereumStorage.EXPECT().GetBlockByHashWithoutFullTx(gomock.Any(), s.config.Tag.Stable, sampleBlockHash, maxSequence).Return(&ethereum.Block{Data: header}, nil)

	resp, err := s.receiver.GetUncleCountByBlockHash(context.Background(), hash)
	require.NoError(err)
	var respStr string
	err = json.Unmarshal(resp, &respStr)
	require.NoError(err)
	require.Equal(expectedNumOfUnclesHex, respStr)
}

func (s *handlerTestSuite) TestGetUncleCountByBlockNumber() {
	require := testutil.Require(s.T())

	expectedNumOfUncles := uint64(1)
	expectedNumOfUnclesHex := hexutil.EncodeUint64(expectedNumOfUncles)

	header, err := fixtures.ReadFile("controller/ethereum/eth_header_with_uncles.json")
	require.NoError(err)

	maxSequence := api.Sequence(1)
	height := uint64(123)
	s.checkpointer.EXPECT().Get(gomock.Any(), api.CollectionLatestCheckpoint, s.config.Tag.Stable).Return(&api.Checkpoint{Sequence: maxSequence}, nil)
	s.ethereumStorage.EXPECT().GetBlock(gomock.Any(), s.config.Tag.Stable, height, maxSequence).Return(&ethereum.Block{Data: header}, nil)

	resp, err := s.receiver.GetUncleCountByBlockNumber(context.Background(), rpc.BlockNumber(height))
	require.NoError(err)
	var respStr string
	err = json.Unmarshal(resp, &respStr)
	require.NoError(err)
	require.Equal(expectedNumOfUnclesHex, respStr)
}

func (s *handlerTestSuite) TestGetUncleCountByBlockNumber_Success_Latest() {
	require := testutil.Require(s.T())

	expectedNumOfUncles := uint64(1)
	expectedNumOfUnclesHex := hexutil.EncodeUint64(expectedNumOfUncles)

	header, err := fixtures.ReadFile("controller/ethereum/eth_header_with_uncles.json")
	require.NoError(err)

	maxSequence := api.Sequence(1)
	latestBlockNumber := uint64(123)
	s.checkpointer.EXPECT().Get(gomock.Any(), api.CollectionLatestCheckpoint, s.config.Tag.Stable).Return(&api.Checkpoint{Sequence: maxSequence, Height: latestBlockNumber}, nil)
	s.ethereumStorage.EXPECT().GetBlock(gomock.Any(), s.config.Tag.Stable, latestBlockNumber, maxSequence).Return(&ethereum.Block{Data: header}, nil)

	resp, err := s.receiver.GetUncleCountByBlockNumber(context.Background(), rpc.BlockNumber(-1))
	require.NoError(err)
	var respStr string
	err = json.Unmarshal(resp, &respStr)
	require.NoError(err)
	require.Equal(expectedNumOfUnclesHex, respStr)
}

func (s *handlerTestSuite) TestGetUncleCountByBlockNumber_Success_Pending() {
	require := testutil.Require(s.T())

	s.jsonrpcClient.EXPECT().Call(gomock.Any(), EthGetUncleCountByBlockNumber, gomock.Any()).Times(1).DoAndReturn(
		func(ctx context.Context, method *jsonrpc.RequestMethod, params jsonrpc.Params) (*jsonrpc.Response, error) {
			require.Len(params, 1)
			return sampleGetUncleCountResp, nil
		},
	)

	resp, err := s.receiver.GetUncleCountByBlockNumber(context.Background(), rpc.PendingBlockNumber)
	require.NoError(err)
	var respStr string
	err = json.Unmarshal(resp, &respStr)
	require.NoError(err)
	require.Equal(string(sampleGetUncleCountMarshal), string(resp))
}

func (s *handlerTestSuite) TestGetUncleCountByBlockNumber_Success_NativeOnly_NonPending() {
	require := testutil.Require(s.T())

	expectedNumOfUncles := uint64(1)
	expectedNumOfUnclesHex := hexutil.EncodeUint64(expectedNumOfUncles)

	header, err := fixtures.ReadFile("controller/ethereum/eth_header_with_uncles.json")
	require.NoError(err)

	maxSequence := api.Sequence(1)
	height := uint64(123)
	s.checkpointer.EXPECT().Get(gomock.Any(), api.CollectionLatestCheckpoint, s.config.Tag.Stable).Return(&api.Checkpoint{Sequence: maxSequence}, nil)
	s.ethereumStorage.EXPECT().GetBlock(gomock.Any(), s.config.Tag.Stable, height, maxSequence).Return(&ethereum.Block{Data: header}, nil)

	resp, err := s.receiver.GetUncleCountByBlockNumber(newContextWithMode(constants.NativeOnlyMode), rpc.BlockNumber(height))
	require.NoError(err)
	var respStr string
	err = json.Unmarshal(resp, &respStr)
	require.NoError(err)
	require.Equal(expectedNumOfUnclesHex, respStr)
}

func (s *handlerTestSuite) TestGetUncleCountByBlockNumber_Fail_NativeOnly_Pending() {
	require := testutil.Require(s.T())
	resp, err := s.receiver.GetUncleCountByBlockNumber(newContextWithMode(constants.NativeOnlyMode), rpc.PendingBlockNumber)
	require.Equal(xerrors.Errorf(invalidDispatchModeErrString, "eth_getUncleCountByBlockNumber", api.ErrNotAllowed).Error(), err.Error())
	s.validateErrorCode(errorCodeBadRequest, err)
	require.Nil(resp)
}

func (s *handlerTestSuite) TestGetBlockByNumberOnlyHash() {
	require := testutil.Require(s.T())

	header, err := fixtures.ReadFile("controller/ethereum/eth_header.json")
	require.NoError(err)

	maxSequence := api.Sequence(1)
	height := uint64(123)
	s.checkpointer.EXPECT().Get(gomock.Any(), api.CollectionLatestCheckpoint, s.config.Tag.Stable).Return(&api.Checkpoint{Sequence: maxSequence}, nil)
	s.ethereumStorage.EXPECT().GetBlock(gomock.Any(), s.config.Tag.Stable, height, maxSequence).Return(&ethereum.Block{Data: header}, nil)

	resp, err := s.receiver.GetBlockByNumber(context.Background(), rpc.BlockNumber(height), false)
	require.NoError(err)
	var headerExpected map[string]json.RawMessage
	err = json.Unmarshal(header, &headerExpected)
	require.NoError(err)
	var headerActual map[string]json.RawMessage
	err = json.Unmarshal(resp, &headerActual)
	require.NoError(err)
	for k, v := range headerExpected {
		vActual, ok := headerActual[k]
		require.True(ok)
		if k != "transactions" {
			require.Equal(v, vActual)
		} else {
			var txsExpected []transactionLite
			err = json.Unmarshal(v, &txsExpected)
			require.NoError(err)
			var txsActual []json.RawMessage
			err = json.Unmarshal(vActual, &txsActual)
			require.NoError(err)
			require.Equal(len(txsExpected), len(txsActual))
			for i, tx := range txsExpected {
				require.Equal(tx.Hash, txsActual[i])
			}
		}
	}
}

func (s *handlerTestSuite) TestGetBlockByNumberEmptyTransactions() {
	require := testutil.Require(s.T())

	headerRaw, err := fixtures.ReadFile("controller/ethereum/eth_header.json")
	require.NoError(err)
	var header map[string]json.RawMessage
	err = json.Unmarshal(headerRaw, &header)
	require.NoError(err)
	header["transactions"] = json.RawMessage("[]")
	headerEmptyTx, err := json.Marshal(header)
	require.NoError(err)

	maxSequence := api.Sequence(1)
	height := uint64(123)
	s.checkpointer.EXPECT().Get(gomock.Any(), api.CollectionLatestCheckpoint, s.config.Tag.Stable).Return(&api.Checkpoint{Sequence: maxSequence}, nil)
	s.ethereumStorage.EXPECT().GetBlock(gomock.Any(), s.config.Tag.Stable, height, maxSequence).Return(&ethereum.Block{Data: headerEmptyTx}, nil)

	resp, err := s.receiver.GetBlockByNumber(context.Background(), rpc.BlockNumber(height), true)
	require.NoError(err)
	require.Equal(string(headerEmptyTx), string(resp))
}

func (s *handlerTestSuite) TestGetBlockByNumberNoTransactionsField() {
	require := testutil.Require(s.T())

	headerRaw, err := fixtures.ReadFile("controller/ethereum/eth_header.json")
	require.NoError(err)
	var header map[string]json.RawMessage
	err = json.Unmarshal(headerRaw, &header)
	require.NoError(err)
	delete(header, "transactions")
	headerNoTx, err := json.Marshal(header)
	require.NoError(err)

	maxSequence := api.Sequence(1)
	height := uint64(123)
	s.checkpointer.EXPECT().Get(gomock.Any(), api.CollectionLatestCheckpoint, s.config.Tag.Stable).Return(&api.Checkpoint{Sequence: maxSequence}, nil)
	s.ethereumStorage.EXPECT().GetBlock(gomock.Any(), s.config.Tag.Stable, height, maxSequence).Return(&ethereum.Block{Data: headerNoTx}, nil)

	resp, err := s.receiver.GetBlockByNumber(context.Background(), rpc.BlockNumber(height), true)
	require.NoError(err)
	require.Equal(string(headerNoTx), string(resp))
}

func (s *handlerTestSuite) TestGetBlockByNumberEarliestBlockNumber() {
	require := testutil.Require(s.T())

	header, err := fixtures.ReadFile("controller/ethereum/eth_header.json")
	require.NoError(err)

	blkNum := rpc.EarliestBlockNumber
	maxSequence := api.Sequence(1)
	s.checkpointer.EXPECT().Get(gomock.Any(), api.CollectionLatestCheckpoint, s.config.Tag.Stable).Return(&api.Checkpoint{Sequence: maxSequence}, nil)
	s.ethereumStorage.EXPECT().GetBlock(gomock.Any(), s.config.Tag.Stable, uint64(blkNum), maxSequence).Return(&ethereum.Block{Data: header}, nil)

	resp, err := s.receiver.GetBlockByNumber(context.Background(), blkNum, true)
	require.NoError(err)
	require.Equal(string(header), string(resp))
}

func (s *handlerTestSuite) TestGetBlockByNumberLatestBlockNumber() {
	require := testutil.Require(s.T())

	header, err := fixtures.ReadFile("controller/ethereum/eth_header.json")
	require.NoError(err)

	maxSequence := api.Sequence(1)
	maxHeight := uint64(123)

	s.checkpointer.EXPECT().Get(gomock.Any(), api.CollectionLatestCheckpoint, s.config.Tag.Stable).Return(&api.Checkpoint{Sequence: maxSequence, Height: uint64(maxHeight)}, nil)
	s.ethereumStorage.EXPECT().GetBlock(gomock.Any(), s.config.Tag.Stable, maxHeight, maxSequence).Return(&ethereum.Block{Data: header}, nil)

	resp, err := s.receiver.GetBlockByNumber(context.Background(), rpc.LatestBlockNumber, true)
	require.NoError(err)
	require.Equal(string(header), string(resp))
}

func (s *handlerTestSuite) TestGetBlockByNumberPendingBlockNumber() {
	require := testutil.Require(s.T())

	header, err := fixtures.ReadFile("controller/ethereum/eth_header.json")
	require.NoError(err)

	s.jsonrpcClient.EXPECT().Call(gomock.Any(), EthGetBlockByNumber, gomock.Any()).Times(1).DoAndReturn(
		func(ctx context.Context, method *jsonrpc.RequestMethod, params jsonrpc.Params) (*jsonrpc.Response, error) {
			require.Len(params, 2)
			return &jsonrpc.Response{
				Result: header,
				Error:  nil,
			}, nil
		},
	)

	resp, err := s.receiver.GetBlockByNumber(context.Background(), rpc.PendingBlockNumber, true)
	require.NoError(err)
	require.Equal(string(header), string(resp))
}

func (s *handlerTestSuite) TestGetBlockByNumberUnsupportedArg() {
	require := testutil.Require(s.T())

	for _, blkNum := range []rpc.BlockNumber{-10, -5, -3} {
		resp, err := s.receiver.GetBlockByNumber(context.Background(), blkNum, true)
		require.Nil(resp)
		require.Error(err)
		require.Contains(err.Error(), "not implemented")
	}
}

func (s *handlerTestSuite) TestGetBlockByNumberBlockNotFound() {
	require := testutil.Require(s.T())

	maxSequence := api.Sequence(1)
	height := uint64(123)
	s.checkpointer.EXPECT().Get(gomock.Any(), api.CollectionLatestCheckpoint, s.config.Tag.Stable).Return(&api.Checkpoint{Sequence: maxSequence}, nil)
	s.ethereumStorage.EXPECT().GetBlock(gomock.Any(), s.config.Tag.Stable, height, maxSequence).Return(nil, storage.ErrItemNotFound)

	resp, err := s.receiver.GetBlockByNumber(context.Background(), rpc.BlockNumber(height), true)
	require.NoError(err)
	require.Nil(resp)
}

func (s *handlerTestSuite) TestGetBalance() {
	require := testutil.Require(s.T())
	s.jsonrpcClient.EXPECT().Call(gomock.Any(), EthGetBalance, gomock.Any()).Times(1).DoAndReturn(
		func(ctx context.Context, method *jsonrpc.RequestMethod, params jsonrpc.Params) (*jsonrpc.Response, error) {
			require.Len(params, 2)
			return sampleGetBalanceResp, nil
		},
	)

	resp, err := s.receiver.GetBalance(context.Background(), nil, nil)
	require.NoError(err)
	require.Equal(string(sampleGetBalanceOutputMarshal), string(resp))
}

func (s *handlerTestSuite) TestGetBalance_Error() {
	require := testutil.Require(s.T())
	s.jsonrpcClient.EXPECT().Call(gomock.Any(), EthGetBalance, gomock.Any()).Times(1).DoAndReturn(
		func(ctx context.Context, method *jsonrpc.RequestMethod, params jsonrpc.Params) (*jsonrpc.Response, error) {
			require.Len(params, 2)
			return sampleGetBalanceRespWithError, jsonrpc.NewRPCError(errorCodeGeneric, sampleGetCodeRespWithError.Error.Message)
		},
	)

	resp, err := s.receiver.GetBalance(context.Background(), nil, nil)
	require.Error(err)
	require.Equal("foo", err.Error())
	require.Nil(resp)

	rpcError, ok := err.(rpc.Error)
	require.True(ok)
	require.Equal(errorCodeGeneric, rpcError.ErrorCode())
}

func (s *handlerTestSuite) TestGetBalance_Fail_NativeOnly() {
	require := testutil.Require(s.T())
	resp, err := s.receiver.GetBalance(newContextWithMode(constants.NativeOnlyMode), nil, nil)
	require.Equal(xerrors.Errorf(invalidDispatchModeErrString, "eth_getBalance", api.ErrNotAllowed).Error(), err.Error())
	s.validateErrorCode(errorCodeBadRequest, err)
	require.Nil(resp)
}

func (s *handlerTestSuite) TestGetCode() {
	require := testutil.Require(s.T())
	s.jsonrpcClient.EXPECT().Call(gomock.Any(), EthGetCode, gomock.Any()).Times(1).DoAndReturn(
		func(ctx context.Context, method *jsonrpc.RequestMethod, params jsonrpc.Params) (*jsonrpc.Response, error) {
			require.Len(params, 2)
			return sampleGetCodeResp, nil
		},
	)

	resp, err := s.receiver.GetCode(context.Background(), nil, nil)
	require.NoError(err)
	require.Equal(string(sampleGetCodeOutputMarshal), string(resp))
}

func (s *handlerTestSuite) TestGetCode_Error() {
	require := testutil.Require(s.T())
	s.jsonrpcClient.EXPECT().Call(gomock.Any(), EthGetCode, gomock.Any()).Times(1).DoAndReturn(
		func(ctx context.Context, method *jsonrpc.RequestMethod, params jsonrpc.Params) (*jsonrpc.Response, error) {
			require.Len(params, 2)
			return sampleGetCodeRespWithError, jsonrpc.NewRPCError(errorCodeGeneric, sampleGetCodeRespWithError.Error.Message)
		},
	)

	resp, err := s.receiver.GetCode(context.Background(), nil, nil)
	require.Error(err)
	require.Equal("foo", err.Error())
	require.Nil(resp)

	rpcError, ok := err.(rpc.Error)
	require.True(ok)
	require.Equal(errorCodeGeneric, rpcError.ErrorCode())
}

func (s *handlerTestSuite) TestGetCode_Fail_NativeOnly() {
	require := testutil.Require(s.T())
	resp, err := s.receiver.GetCode(newContextWithMode(constants.NativeOnlyMode), nil, nil)
	require.Equal(xerrors.Errorf(invalidDispatchModeErrString, "eth_getCode", api.ErrNotAllowed).Error(), err.Error())
	s.validateErrorCode(errorCodeBadRequest, err)
	require.Nil(resp)
}

func (s *handlerTestSuite) TestGetTransactionCount() {
	require := testutil.Require(s.T())
	s.jsonrpcClient.EXPECT().Call(gomock.Any(), EthGetTransactionCount, gomock.Any()).Times(1).DoAndReturn(
		func(ctx context.Context, method *jsonrpc.RequestMethod, params jsonrpc.Params) (*jsonrpc.Response, error) {
			require.Len(params, 2)
			return sampleGetTransactionCountResp, nil
		},
	)

	resp, err := s.receiver.GetTransactionCount(context.Background(), nil, nil)
	require.NoError(err)
	require.Equal(string(sampleGetTransactionCountMarshal), string(resp))
}

func (s *handlerTestSuite) TestGetTransactionCount_Error() {
	require := testutil.Require(s.T())
	s.jsonrpcClient.EXPECT().Call(gomock.Any(), EthGetTransactionCount, gomock.Any()).Times(1).DoAndReturn(
		func(ctx context.Context, method *jsonrpc.RequestMethod, params jsonrpc.Params) (*jsonrpc.Response, error) {
			require.Len(params, 2)
			return sampleGetBalanceRespWithError, jsonrpc.NewRPCError(errorCodeGeneric, sampleGetCodeRespWithError.Error.Message)
		},
	)

	resp, err := s.receiver.GetTransactionCount(context.Background(), nil, nil)
	require.Error(err)
	require.Equal("foo", err.Error())
	require.Nil(resp)

	rpcError, ok := err.(rpc.Error)
	require.True(ok)
	require.Equal(errorCodeGeneric, rpcError.ErrorCode())
}

func (s *handlerTestSuite) TestGetTransactionCount_Fail_NativeOnly() {
	require := testutil.Require(s.T())
	resp, err := s.receiver.GetTransactionCount(newContextWithMode(constants.NativeOnlyMode), nil, nil)
	require.Equal(xerrors.Errorf(invalidDispatchModeErrString, "eth_getTransactionCount", api.ErrNotAllowed).Error(), err.Error())
	s.validateErrorCode(errorCodeBadRequest, err)
	require.Nil(resp)
}

func (s *handlerTestSuite) TestSendRawTransaction_Fail_NativeOnly() {
	require := testutil.Require(s.T())
	resp, err := s.receiver.SendRawTransaction(newContextWithMode(constants.NativeOnlyMode), nil)
	require.Equal(xerrors.Errorf(invalidDispatchModeErrString, "eth_sendRawTransaction", api.ErrNotAllowed).Error(), err.Error())
	s.validateErrorCode(errorCodeBadRequest, err)
	require.Nil(resp)
}

func (s *handlerTestSuite) TestGasPrice_Fail_NativeOnly() {
	require := testutil.Require(s.T())
	resp, err := s.receiver.GasPrice(newContextWithMode(constants.NativeOnlyMode))
	require.Equal(xerrors.Errorf(invalidDispatchModeErrString, "eth_gasPrice", api.ErrNotAllowed).Error(), err.Error())
	s.validateErrorCode(errorCodeBadRequest, err)
	require.Nil(resp)
}

func (s *handlerTestSuite) TestGetStorageAt_Fail_NativeOnly() {
	require := testutil.Require(s.T())
	resp, err := s.receiver.GetStorageAt(newContextWithMode(constants.NativeOnlyMode), nil, nil, nil)
	require.Equal(xerrors.Errorf(invalidDispatchModeErrString, "eth_getStorageAt", api.ErrNotAllowed).Error(), err.Error())
	s.validateErrorCode(errorCodeBadRequest, err)
	require.Nil(resp)
}

func (s *handlerTestSuite) TestEstimateGas_Fail_NativeOnly() {
	require := testutil.Require(s.T())
	resp, err := s.receiver.EstimateGas(newContextWithMode(constants.NativeOnlyMode), nil, nil)
	require.Equal(xerrors.Errorf(invalidDispatchModeErrString, "eth_estimateGas", api.ErrNotAllowed).Error(), err.Error())
	s.validateErrorCode(errorCodeBadRequest, err)
	require.Nil(resp)
}

func (s *handlerTestSuite) TestGetLogsWithHash() {
	require := testutil.Require(s.T())

	logs, err := fixtures.ReadJson("controller/ethereum/eth_logs_1.json")
	require.NoError(err)

	blockHash := common.HexToHash("0xbaa42c87b7c764c548fa37e61e9764415fd4a79d7e073d4f92a456698002016b")
	maxSequence := api.Sequence(11322001)
	height := uint64(11322000)
	s.checkpointer.EXPECT().Get(gomock.Any(), api.CollectionLatestCheckpoint, s.config.Tag.Stable).Return(&api.Checkpoint{Sequence: maxSequence, Height: height + 1}, nil)
	s.ethereumStorage.EXPECT().GetBlockByHashWithoutFullTx(gomock.Any(), s.config.Tag.Stable, blockHash.String(), maxSequence).Return(&ethereum.Block{Height: height}, nil)
	s.ethereumStorage.EXPECT().GetLogsV2(gomock.Any(), s.config.Tag.Stable, height, maxSequence).Return(&ethereum.Logs{Data: logs}, nil)

	resp, err := s.receiver.GetLogs(context.Background(), FilterCriteria{BlockHash: &blockHash})
	require.NoError(err)
	require.Equal(string(logs), string(resp))
}

func (s *handlerTestSuite) TestGetLogsWithHeights() {
	require := testutil.Require(s.T())

	logs, err := fixtures.ReadJson("controller/ethereum/eth_logs_1.json")
	require.NoError(err)

	maxSequence := api.Sequence(1)
	height := uint64(123)
	heightBigInt := big.NewInt(int64(height))
	s.checkpointer.EXPECT().Get(gomock.Any(), api.CollectionLatestCheckpoint, s.config.Tag.Stable).Return(&api.Checkpoint{Sequence: maxSequence, Height: height + 2}, nil)
	s.ethereumStorage.EXPECT().GetLogsV2(gomock.Any(), s.config.Tag.Stable, height, maxSequence).Return(&ethereum.Logs{Data: logs}, nil)

	resp, err := s.receiver.GetLogs(context.Background(), FilterCriteria{FromBlock: heightBigInt, ToBlock: heightBigInt})
	require.NoError(err)
	require.Equal(string(logs), string(resp))
}

func (s *handlerTestSuite) TestGetLogsWithTaggedBlockNumbers() {
	require := testutil.Require(s.T())

	logs1, err := fixtures.ReadJson("controller/ethereum/eth_logs_1.json")
	require.NoError(err)
	logs2, err := fixtures.ReadJson("controller/ethereum/eth_logs_2.json")
	require.NoError(err)

	maxSequence := api.Sequence(1)
	height := uint64(11) // must be odd
	s.checkpointer.EXPECT().Get(gomock.Any(), api.CollectionLatestCheckpoint, s.config.Tag.Stable).Return(&api.Checkpoint{Sequence: maxSequence, Height: height}, nil)
	for blkNum := uint64(earliestBlockHeight); blkNum <= height; blkNum++ {
		// Use logs1 and logs2 alternately
		if blkNum%2 == 0 {
			s.ethereumStorage.EXPECT().GetLogsV2(gomock.Any(), s.config.Tag.Stable, blkNum, maxSequence).Return(&ethereum.Logs{Data: logs1}, nil)
		} else {
			s.ethereumStorage.EXPECT().GetLogsV2(gomock.Any(), s.config.Tag.Stable, blkNum, maxSequence).Return(&ethereum.Logs{Data: logs2}, nil)
		}
	}

	resp, err := s.receiver.GetLogs(context.Background(), FilterCriteria{FromBlock: big.NewInt(rpc.EarliestBlockNumber.Int64()), ToBlock: big.NewInt(rpc.LatestBlockNumber.Int64())})
	require.NoError(err)
	// Make sure the returned order is correct
	// Ignoring the last character in comparison (they should be different: ",", "]")
	logs1Strip := string(logs1[1:(len(logs1) - 1)])
	logs2Strip := string(logs2[1:(len(logs2) - 1)])
	// Ingoring the first and the last character of resp: "[" and "]"
	i := 1
	for i < len(resp)-1 {
		require.Equal(logs1Strip, string(resp[i:(i+len(logs1Strip))]))
		i += len(logs1Strip) + 1
		require.Equal(logs2Strip, string(resp[i:(i+len(logs2Strip))]))
		i += len(logs2Strip) + 1
	}
}

func (s *handlerTestSuite) TestGetLogsWithTaggedBlockNumberPending() {
	require := testutil.Require(s.T())

	maxSequence := api.Sequence(1)
	height := uint64(12)
	s.checkpointer.EXPECT().Get(gomock.Any(), api.CollectionLatestCheckpoint, s.config.Tag.Stable).Return(&api.Checkpoint{Sequence: maxSequence, Height: height}, nil)

	resp, err := s.receiver.GetLogs(context.Background(), FilterCriteria{FromBlock: big.NewInt(rpc.PendingBlockNumber.Int64()), ToBlock: big.NewInt(rpc.PendingBlockNumber.Int64())})
	require.Nil(resp)
	require.Error(err)
	require.Contains(err.Error(), "not implemented")
}

func (s *handlerTestSuite) TestGetLogsWithoutHeightsOrHash() {
	require := testutil.Require(s.T())

	logs, err := fixtures.ReadJson("controller/ethereum/eth_logs_1.json")
	require.NoError(err)

	maxSequence := api.Sequence(1)
	height := uint64(123)
	s.checkpointer.EXPECT().Get(gomock.Any(), api.CollectionLatestCheckpoint, s.config.Tag.Stable).Return(&api.Checkpoint{Sequence: maxSequence, Height: height}, nil)
	s.ethereumStorage.EXPECT().GetLogsV2(gomock.Any(), s.config.Tag.Stable, height, maxSequence).Return(&ethereum.Logs{Data: logs}, nil)

	resp, err := s.receiver.GetLogs(context.Background(), FilterCriteria{})
	require.NoError(err)
	require.Equal(string(logs), string(resp))
}

func (s *handlerTestSuite) TestGetLogsWithAddresses() {
	require := testutil.Require(s.T())

	logs, err := fixtures.ReadJson("controller/ethereum/eth_logs_1.json")
	require.NoError(err)
	logsRes, err := fixtures.ReadJson("controller/ethereum/eth_getLogsWithAddressesOutput.json")
	require.NoError(err)

	addresses := []common.Address{common.HexToAddress("0xe5caef4af8780e59df925470b050fb23c43ca68c"), common.HexToAddress("0xad72c532d9fe5c51292d950dd0a160c76ff3fa30")}

	maxSequence := api.Sequence(1)
	height := uint64(123)
	heightBigInt := big.NewInt(int64(height))

	var rawLogs []*types.Log
	err = json.Unmarshal(logs, &rawLogs)
	require.NoError(err)
	bloom := types.BytesToBloom(types.LogsBloom(rawLogs))
	bloomText, err := bloom.MarshalText()
	require.NoError(err)
	logsLite := ethereum.NewLogsLite(s.config.Tag.Stable, height, maxSequence, string(bloomText))

	s.checkpointer.EXPECT().Get(gomock.Any(), api.CollectionLatestCheckpoint, s.config.Tag.Stable).Return(&api.Checkpoint{Sequence: maxSequence, Height: height}, nil)
	s.ethereumStorage.EXPECT().GetLogsLiteByBlockRange(gomock.Any(), s.config.Tag.Stable, height, height+1, maxSequence).
		Return([]*ethereum.LogsLite{logsLite}, nil)
	s.ethereumStorage.EXPECT().GetLogsV2(gomock.Any(), s.config.Tag.Stable, height, maxSequence).Return(&ethereum.Logs{Data: logs}, nil)

	resp, err := s.receiver.GetLogs(context.Background(), FilterCriteria{FromBlock: heightBigInt, ToBlock: heightBigInt, Addresses: addresses})
	require.NoError(err)
	var respUnMarshal []logWithAddressAndTopics
	err = json.Unmarshal(resp, &respUnMarshal)
	require.NoError(err)
	require.Len(respUnMarshal, 3)
	require.Equal(string(logsRes), string(resp))
}

func (s *handlerTestSuite) TestGetLogsWithAddresses_AddressNotAllowed() {
	require := testutil.Require(s.T())

	if s.blockchain == c3common.Blockchain_BLOCKCHAIN_ETHEREUM {
		addresses := []common.Address{common.HexToAddress("0x6639cdb3ea7a48b0ad95b47bec78023c6f706160")}
		height := uint64(123)
		heightBigInt := big.NewInt(int64(height))

		resp, err := s.receiver.GetLogs(context.Background(), FilterCriteria{FromBlock: heightBigInt, ToBlock: heightBigInt, Addresses: addresses})
		require.NoError(err)
		require.Equal("[]", string(resp))
	}
}

func (s *handlerTestSuite) TestGetLogsWithTopics() {
	require := testutil.Require(s.T())

	logs, err := fixtures.ReadJson("controller/ethereum/eth_logs_1.json")
	require.NoError(err)
	logsRes, err := fixtures.ReadJson("controller/ethereum/eth_getLogsWithTopicsOutput.json")
	require.NoError(err)
	topics := [][]common.Hash{
		{common.HexToHash("0x29ae811400000000000000000000000000000000000000000000000000000000"),
			common.HexToHash("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"),
			common.HexToHash("0xc1405953cccdad6b442e266c84d66ad671e2534c6584f8e6ef92802f7ad294d5")},
		{common.HexToHash("0x00000000000000000000000098265d92b016df8758f361fb8d2f9a813c82494a"),
			common.HexToHash("0x000000000000000000000000be8e3e3618f7474f8cb1d074a26affef007e98fb")},
		{common.HexToHash("0x00000000000000000000000092330d8818e8a3b50f027c819fa46031ffba2c8c"),
			common.HexToHash("0x6473720000000000000000000000000000000000000000000000000000000000")}}

	maxSequence := api.Sequence(1)
	height := uint64(123)
	heightBigInt := big.NewInt(int64(height))

	var rawLogs []*types.Log
	err = json.Unmarshal(logs, &rawLogs)
	require.NoError(err)
	bloom := types.BytesToBloom(types.LogsBloom(rawLogs))
	bloomText, err := bloom.MarshalText()
	require.NoError(err)
	logsLite := ethereum.NewLogsLite(s.config.Tag.Stable, height, maxSequence, string(bloomText))

	s.checkpointer.EXPECT().Get(gomock.Any(), api.CollectionLatestCheckpoint, s.config.Tag.Stable).Return(&api.Checkpoint{Sequence: maxSequence, Height: height}, nil)
	s.ethereumStorage.EXPECT().GetLogsLiteByBlockRange(gomock.Any(), s.config.Tag.Stable, height, height+1, maxSequence).
		Return([]*ethereum.LogsLite{logsLite}, nil)
	s.ethereumStorage.EXPECT().GetLogsV2(gomock.Any(), s.config.Tag.Stable, height, maxSequence).Return(&ethereum.Logs{Data: logs}, nil)

	resp, err := s.receiver.GetLogs(context.Background(), FilterCriteria{FromBlock: heightBigInt, ToBlock: heightBigInt, Topics: topics})
	require.NoError(err)
	var respUnMarshal []logWithAddressAndTopics
	err = json.Unmarshal(resp, &respUnMarshal)
	require.NoError(err)
	require.Len(respUnMarshal, 2)
	require.Equal(string(logsRes), string(resp))
}

func (s *handlerTestSuite) TestGetLogsWithAddressesAndTopics() {
	require := testutil.Require(s.T())

	logs1, err := fixtures.ReadJson("controller/ethereum/eth_logs_1.json")
	require.NoError(err)
	logs2, err := fixtures.ReadJson("controller/ethereum/eth_logs_2.json")
	require.NoError(err)
	logsRes, err := fixtures.ReadJson("controller/ethereum/eth_getLogsWithTopicsOutput.json")
	require.NoError(err)
	addresses := []common.Address{common.HexToAddress("0xe5caef4af8780e59df925470b050fb23c43ca68c"), common.HexToAddress("0xad72c532d9fe5c51292d950dd0a160c76ff3fa30")}
	topics := [][]common.Hash{
		{common.HexToHash("0x29ae811400000000000000000000000000000000000000000000000000000000"),
			common.HexToHash("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"),
			common.HexToHash("0xc1405953cccdad6b442e266c84d66ad671e2534c6584f8e6ef92802f7ad294d5")},
		{common.HexToHash("0x00000000000000000000000098265d92b016df8758f361fb8d2f9a813c82494a"),
			common.HexToHash("0x000000000000000000000000be8e3e3618f7474f8cb1d074a26affef007e98fb")},
		{common.HexToHash("0x00000000000000000000000092330d8818e8a3b50f027c819fa46031ffba2c8c"),
			common.HexToHash("0x6473720000000000000000000000000000000000000000000000000000000000")}}

	maxSequence := api.Sequence(1)
	beginHeight := uint64(123)
	beginHeightBigInt := big.NewInt(int64(beginHeight))

	endHeight := uint64(123) + 1
	endHeightBigInt := big.NewInt(int64(endHeight))

	checkpointHeight := beginHeight + 4

	var rawLogs []*types.Log
	err = json.Unmarshal(logs1, &rawLogs)
	require.NoError(err)
	bloom := types.BytesToBloom(types.LogsBloom(rawLogs))
	bloomText, err := bloom.MarshalText()
	require.NoError(err)
	logsLite1 := ethereum.NewLogsLite(s.config.Tag.Stable, beginHeight, maxSequence, string(bloomText))

	err = json.Unmarshal(logs2, &rawLogs)
	require.NoError(err)
	bloom = types.BytesToBloom(types.LogsBloom(rawLogs))
	bloomText, err = bloom.MarshalText()
	require.NoError(err)
	logsLite2 := ethereum.NewLogsLite(s.config.Tag.Stable, endHeight, maxSequence, string(bloomText))

	s.checkpointer.EXPECT().Get(gomock.Any(), api.CollectionLatestCheckpoint, s.config.Tag.Stable).Return(&api.Checkpoint{Sequence: maxSequence, Height: checkpointHeight}, nil)
	s.ethereumStorage.EXPECT().GetLogsLiteByBlockRange(gomock.Any(), s.config.Tag.Stable, beginHeight, beginHeight+2, maxSequence).
		Return([]*ethereum.LogsLite{logsLite1, logsLite2}, nil)
	s.ethereumStorage.EXPECT().GetLogsV2(gomock.Any(), s.config.Tag.Stable, beginHeight, maxSequence).Return(&ethereum.Logs{Data: logs1}, nil)

	resp, err := s.receiver.GetLogs(context.Background(), FilterCriteria{FromBlock: beginHeightBigInt, ToBlock: endHeightBigInt, Topics: topics, Addresses: addresses})
	require.NoError(err)
	var respUnMarshal []logWithAddressAndTopics
	err = json.Unmarshal(resp, &respUnMarshal)
	require.NoError(err)
	require.Len(respUnMarshal, 2)
	require.Equal(string(logsRes), string(resp))
}

func (s *handlerTestSuite) TestGetLogsWithAddressesAndTopics_BeyondCheckpoint() {
	require := testutil.Require(s.T())

	logs1, err := fixtures.ReadJson("controller/ethereum/eth_logs_1.json")
	require.NoError(err)
	logs2, err := fixtures.ReadJson("controller/ethereum/eth_logs_2.json")
	require.NoError(err)
	logsRes, err := fixtures.ReadJson("controller/ethereum/eth_getLogsWithTopicsOutput.json")
	require.NoError(err)
	addresses := []common.Address{common.HexToAddress("0xe5caef4af8780e59df925470b050fb23c43ca68c"), common.HexToAddress("0xad72c532d9fe5c51292d950dd0a160c76ff3fa30")}
	topics := [][]common.Hash{
		{common.HexToHash("0x29ae811400000000000000000000000000000000000000000000000000000000"),
			common.HexToHash("0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"),
			common.HexToHash("0xc1405953cccdad6b442e266c84d66ad671e2534c6584f8e6ef92802f7ad294d5")},
		{common.HexToHash("0x00000000000000000000000098265d92b016df8758f361fb8d2f9a813c82494a"),
			common.HexToHash("0x000000000000000000000000be8e3e3618f7474f8cb1d074a26affef007e98fb")},
		{common.HexToHash("0x00000000000000000000000092330d8818e8a3b50f027c819fa46031ffba2c8c"),
			common.HexToHash("0x6473720000000000000000000000000000000000000000000000000000000000")}}

	maxSequence := api.Sequence(1)
	beginHeight := uint64(123)
	beginHeightBigInt := big.NewInt(int64(beginHeight))
	endHeight := beginHeight + 1

	checkpointHeight := endHeight

	var rawLogs []*types.Log
	err = json.Unmarshal(logs1, &rawLogs)
	require.NoError(err)
	bloom := types.BytesToBloom(types.LogsBloom(rawLogs))
	bloomText, err := bloom.MarshalText()
	require.NoError(err)
	logsLite1 := ethereum.NewLogsLite(s.config.Tag.Stable, beginHeight, maxSequence, string(bloomText))

	err = json.Unmarshal(logs2, &rawLogs)
	require.NoError(err)
	bloom = types.BytesToBloom(types.LogsBloom(rawLogs))
	bloomText, err = bloom.MarshalText()
	require.NoError(err)
	logsLite2 := ethereum.NewLogsLite(s.config.Tag.Stable, endHeight, maxSequence, string(bloomText))

	s.checkpointer.EXPECT().Get(gomock.Any(), api.CollectionLatestCheckpoint, s.config.Tag.Stable).Return(&api.Checkpoint{Sequence: maxSequence, Height: checkpointHeight}, nil)
	s.ethereumStorage.EXPECT().GetLogsLiteByBlockRange(gomock.Any(), s.config.Tag.Stable, beginHeight, beginHeight+2, maxSequence).
		Return([]*ethereum.LogsLite{logsLite1, logsLite2}, nil)
	s.ethereumStorage.EXPECT().GetLogsV2(gomock.Any(), s.config.Tag.Stable, beginHeight, maxSequence).Return(&ethereum.Logs{Data: logs1}, nil)

	resp, err := s.receiver.GetLogs(context.Background(), FilterCriteria{FromBlock: beginHeightBigInt, ToBlock: big.NewInt(int64(checkpointHeight) + 3), Topics: topics, Addresses: addresses})
	require.NoError(err)
	var respUnMarshal []logWithAddressAndTopics
	err = json.Unmarshal(resp, &respUnMarshal)
	require.NoError(err)
	require.Len(respUnMarshal, 2)
	require.Equal(string(logsRes), string(resp))
}

func (s *handlerTestSuite) TestGetLogsEmpty() {
	require := testutil.Require(s.T())

	logs := []byte("[]")

	maxSequence := api.Sequence(1)
	height := uint64(123)
	s.checkpointer.EXPECT().Get(gomock.Any(), api.CollectionLatestCheckpoint, s.config.Tag.Stable).Return(&api.Checkpoint{Sequence: maxSequence, Height: height + 2}, nil)
	s.ethereumStorage.EXPECT().GetLogsV2(gomock.Any(), s.config.Tag.Stable, height, maxSequence).Return(&ethereum.Logs{Data: logs}, nil)

	resp, err := s.receiver.GetLogs(context.Background(), FilterCriteria{FromBlock: big.NewInt(int64(height)), ToBlock: big.NewInt(int64(height))})
	require.NoError(err)
	require.Equal("[]", string(resp))
}

func (s *handlerTestSuite) TestGetLogsBloomFilter_NotFound() {
	require := testutil.Require(s.T())

	addresses := []common.Address{common.HexToAddress("0xe5caef4af8780e59df925470b050fb23c43ca68c"), common.HexToAddress("0xad72c532d9fe5c51292d950dd0a160c76ff3fa30")}
	maxSequence := api.Sequence(1)
	height := uint64(123)
	heightBigInt := big.NewInt(int64(height))

	s.checkpointer.EXPECT().Get(gomock.Any(), api.CollectionLatestCheckpoint, s.config.Tag.Stable).Return(&api.Checkpoint{Sequence: maxSequence, Height: height}, nil)
	s.ethereumStorage.EXPECT().GetLogsLiteByBlockRange(gomock.Any(), s.config.Tag.Stable, height, height+1, maxSequence).Return(nil, storage.ErrItemNotFound)

	resp, err := s.receiver.GetLogs(context.Background(), FilterCriteria{FromBlock: heightBigInt, ToBlock: heightBigInt, Addresses: addresses})
	require.NoError(err)
	require.Equal("[]", string(resp))
}

func (s *handlerTestSuite) TestGetLogsBloomFilter_NilLogsLite_Matched() {
	require := testutil.Require(s.T())

	addresses := []common.Address{common.HexToAddress("0xe5caef4af8780e59df925470b050fb23c43ca68c"), common.HexToAddress("0xad72c532d9fe5c51292d950dd0a160c76ff3fa30")}
	maxSequence := api.Sequence(1)
	height := uint64(123)
	heightBigInt := big.NewInt(int64(height))
	logs := fixtures.MustReadJson("controller/ethereum/eth_logs_1.json")
	expectedResp := fixtures.MustReadJson("controller/ethereum/eth_logs_1_filtered_by_addresses.json")

	logsLite := make([]*ethereum.LogsLite, 1)
	s.checkpointer.EXPECT().Get(gomock.Any(), api.CollectionLatestCheckpoint, s.config.Tag.Stable).Return(&api.Checkpoint{Sequence: maxSequence, Height: height}, nil)
	s.ethereumStorage.EXPECT().GetLogsLiteByBlockRange(gomock.Any(), s.config.Tag.Stable, height, height+1, maxSequence).Return(logsLite, nil)

	s.ethereumStorage.EXPECT().GetLogsV2(gomock.Any(), s.config.Tag.Stable, height, maxSequence).Return(&ethereum.Logs{Data: logs}, nil)
	resp, err := s.receiver.GetLogs(context.Background(), FilterCriteria{FromBlock: heightBigInt, ToBlock: heightBigInt, Addresses: addresses})
	require.NoError(err)
	require.Equal(string(expectedResp), string(resp))
}

func (s *handlerTestSuite) TestGetLogsBloomFilter_NilLogsLite_NotMatched() {
	require := testutil.Require(s.T())

	addresses := []common.Address{common.HexToAddress("0xe5caef4af8")}
	maxSequence := api.Sequence(1)
	height := uint64(123)
	heightBigInt := big.NewInt(int64(height))
	logs := fixtures.MustReadJson("controller/ethereum/eth_logs_1.json")

	logsLite := make([]*ethereum.LogsLite, 1)
	s.checkpointer.EXPECT().Get(gomock.Any(), api.CollectionLatestCheckpoint, s.config.Tag.Stable).Return(&api.Checkpoint{Sequence: maxSequence, Height: height}, nil)
	s.ethereumStorage.EXPECT().GetLogsLiteByBlockRange(gomock.Any(), s.config.Tag.Stable, height, height+1, maxSequence).Return(logsLite, nil)

	s.ethereumStorage.EXPECT().GetLogsV2(gomock.Any(), s.config.Tag.Stable, height, maxSequence).Return(&ethereum.Logs{Data: logs}, nil)
	resp, err := s.receiver.GetLogs(context.Background(), FilterCriteria{FromBlock: heightBigInt, ToBlock: heightBigInt, Addresses: addresses})
	require.NoError(err)
	require.Equal("[]", string(resp))
}

func (s *handlerTestSuite) TestGetLogsBlockHashNotFound() {
	require := testutil.Require(s.T())

	blockHash := common.HexToHash("0xbaa42c87b7c764c548fa37e61e9764415fd4a79d7e073d4f92a456698002016b")
	maxSequence := api.Sequence(11322001)
	s.checkpointer.EXPECT().Get(gomock.Any(), api.CollectionLatestCheckpoint, s.config.Tag.Stable).Return(&api.Checkpoint{Sequence: maxSequence}, nil)
	s.ethereumStorage.EXPECT().GetBlockByHashWithoutFullTx(gomock.Any(), s.config.Tag.Stable, blockHash.String(), maxSequence).Return(nil, storage.ErrItemNotFound)

	_, err := s.receiver.GetLogs(context.Background(), FilterCriteria{BlockHash: &blockHash})
	require.Error(err)

	rpcError, ok := err.(rpc.Error)
	require.True(ok)
	require.Equal(errorCodeBadRequest, rpcError.ErrorCode())
	require.Equal("unknown block", rpcError.Error())
}

func (s *handlerTestSuite) TestGetLogsNotSupported() {
	require := testutil.Require(s.T())

	maxSequence := api.Sequence(1)
	fromBlock := int64(123)
	toBlock := fromBlock + getLogsBlockRangeLimit

	s.checkpointer.EXPECT().Get(gomock.Any(), api.CollectionLatestCheckpoint, s.config.Tag.Stable).Return(&api.Checkpoint{Sequence: maxSequence, Height: uint64(toBlock)}, nil)
	_, err := s.receiver.GetLogs(context.Background(), FilterCriteria{FromBlock: big.NewInt(fromBlock), ToBlock: big.NewInt(toBlock)})
	require.Error(err)

	rpcError, ok := err.(rpc.Error)
	require.True(ok)
	require.Equal(errorCodeNotSupported, rpcError.ErrorCode())
	require.Equal(fmt.Sprintf("please limit the query to at most %v blocks", getLogsBlockRangeLimit), rpcError.Error())
}

func (s *handlerTestSuite) TestGetLogsNotFound() {
	require := testutil.Require(s.T())

	maxSequence := api.Sequence(1)
	height := uint64(123)
	s.checkpointer.EXPECT().Get(gomock.Any(), api.CollectionLatestCheckpoint, s.config.Tag.Stable).Return(&api.Checkpoint{Sequence: maxSequence, Height: height + 2}, nil)
	s.ethereumStorage.EXPECT().GetLogsV2(gomock.Any(), s.config.Tag.Stable, height, maxSequence).Return(nil, storage.ErrItemNotFound)

	resp, err := s.receiver.GetLogs(context.Background(), FilterCriteria{FromBlock: big.NewInt(int64(height)), ToBlock: big.NewInt(int64(height))})
	require.NoError(err)
	require.Equal("[]", string(resp))
}

func (s *handlerTestSuite) TestGetTransactionByHash() {
	require := testutil.Require(s.T())

	tx, err := fixtures.ReadJson("controller/ethereum/eth_transaction_1.json")
	require.NoError(err)

	maxSequence := api.Sequence(1)
	txHash := common.HexToHash(sampleTxHash)
	s.checkpointer.EXPECT().Get(gomock.Any(), api.CollectionLatestCheckpoint, s.config.Tag.Stable).Return(&api.Checkpoint{Sequence: maxSequence}, nil)
	s.ethereumStorage.EXPECT().GetTransactionByHash(gomock.Any(), s.config.Tag.Stable, sampleTxHash, maxSequence).Return(&ethereum.Transaction{Data: tx}, nil)

	resp, err := s.receiver.GetTransactionByHash(context.Background(), txHash)
	require.NoError(err)
	require.Equal(string(tx), string(resp))
}

func (s *handlerTestSuite) TestGetTransactionByHashError() {
	require := testutil.Require(s.T())

	sampleErrMsg := "sample error"
	sampleErr := xerrors.New(sampleErrMsg)

	maxSequence := api.Sequence(1)
	txHash := common.HexToHash(sampleTxHash)
	s.checkpointer.EXPECT().Get(gomock.Any(), api.CollectionLatestCheckpoint, s.config.Tag.Stable).Return(&api.Checkpoint{Sequence: maxSequence}, nil)
	s.ethereumStorage.EXPECT().GetTransactionByHash(gomock.Any(), s.config.Tag.Stable, sampleTxHash, maxSequence).Return(nil, sampleErr)

	resp, err := s.receiver.GetTransactionByHash(context.Background(), txHash)
	require.Nil(resp)
	require.Error(err)
	require.Contains(err.Error(), sampleErrMsg)
}

func (s *handlerTestSuite) TestGetTransactionByHashEmptyTx() {
	require := testutil.Require(s.T())

	tx := make([]byte, 0)

	maxSequence := api.Sequence(1)
	txHash := common.HexToHash(sampleTxHash)
	s.checkpointer.EXPECT().Get(gomock.Any(), api.CollectionLatestCheckpoint, s.config.Tag.Stable).Return(&api.Checkpoint{Sequence: maxSequence}, nil)
	s.ethereumStorage.EXPECT().GetTransactionByHash(gomock.Any(), s.config.Tag.Stable, sampleTxHash, maxSequence).Return(&ethereum.Transaction{Data: tx}, nil)

	resp, err := s.receiver.GetTransactionByHash(context.Background(), txHash)
	require.NoError(err)
	require.Equal(string(tx), string(resp))
}

func (s *handlerTestSuite) TestGetTransactionByHashNotFound() {
	require := testutil.Require(s.T())

	maxSequence := api.Sequence(1)
	txHash := common.HexToHash(sampleTxHash)
	s.checkpointer.EXPECT().Get(gomock.Any(), api.CollectionLatestCheckpoint, s.config.Tag.Stable).Return(&api.Checkpoint{Sequence: maxSequence}, nil)
	s.ethereumStorage.EXPECT().GetTransactionByHash(gomock.Any(), s.config.Tag.Stable, sampleTxHash, maxSequence).Return(nil, storage.ErrItemNotFound)

	resp, err := s.receiver.GetTransactionByHash(context.Background(), txHash)
	require.NoError(err)
	require.Nil(resp)
}

func (s *handlerTestSuite) TestGetTransactionByBlockHashAndIndex() {
	require := testutil.Require(s.T())

	header, err := fixtures.ReadJson("controller/ethereum/eth_header.json")
	require.NoError(err)

	tx, err := fixtures.ReadJson("controller/ethereum/eth_transaction_1.json")
	require.NoError(err)

	maxSequence := api.Sequence(1)
	hash := common.HexToHash(sampleBlockHash)
	s.checkpointer.EXPECT().Get(gomock.Any(), api.CollectionLatestCheckpoint, s.config.Tag.Stable).Return(&api.Checkpoint{Sequence: maxSequence}, nil)
	s.ethereumStorage.EXPECT().GetBlockByHash(gomock.Any(), s.config.Tag.Stable, sampleBlockHash, maxSequence).Return(&ethereum.Block{Data: header}, nil)

	resp, err := s.receiver.GetTransactionByBlockHashAndIndex(context.Background(), hash, rpc.DecimalOrHex(0))
	require.NoError(err)
	require.Equal(string(tx), string(resp))
}

func (s *handlerTestSuite) TestGetTransactionByBlockHashAndIndexError() {
	require := testutil.Require(s.T())

	sampleErrMsg := "sample error"
	sampleErr := xerrors.New(sampleErrMsg)

	maxSequence := api.Sequence(1)
	hash := common.HexToHash(sampleBlockHash)
	s.checkpointer.EXPECT().Get(gomock.Any(), api.CollectionLatestCheckpoint, s.config.Tag.Stable).Return(&api.Checkpoint{Sequence: maxSequence}, nil)
	s.ethereumStorage.EXPECT().GetBlockByHash(gomock.Any(), s.config.Tag.Stable, sampleBlockHash, maxSequence).Return(nil, sampleErr)

	resp, err := s.receiver.GetTransactionByBlockHashAndIndex(context.Background(), hash, rpc.DecimalOrHex(0))
	require.Nil(resp)
	require.Error(err)
	require.Contains(err.Error(), sampleErrMsg)
}

func (s *handlerTestSuite) TestGetTransactionByBlockHashAndIndexOutOfBound() {
	require := testutil.Require(s.T())

	header, err := fixtures.ReadJson("controller/ethereum/eth_header.json")
	require.NoError(err)

	maxSequence := api.Sequence(1)
	hash := common.HexToHash(sampleBlockHash)
	s.checkpointer.EXPECT().Get(gomock.Any(), api.CollectionLatestCheckpoint, s.config.Tag.Stable).Return(&api.Checkpoint{Sequence: maxSequence}, nil)
	s.ethereumStorage.EXPECT().GetBlockByHash(gomock.Any(), s.config.Tag.Stable, sampleBlockHash, maxSequence).Return(&ethereum.Block{Data: header}, nil)

	resp, err := s.receiver.GetTransactionByBlockHashAndIndex(context.Background(), hash, rpc.DecimalOrHex(2))
	require.NoError(err)
	require.Nil(resp)
}

func (s *handlerTestSuite) TestGetTransactionByBlockNumberAndIndex() {
	require := testutil.Require(s.T())

	header, err := fixtures.ReadJson("controller/ethereum/eth_header.json")
	require.NoError(err)

	tx, err := fixtures.ReadJson("controller/ethereum/eth_transaction_1.json")
	require.NoError(err)

	maxSequence := api.Sequence(1)
	height := uint64(123)
	s.checkpointer.EXPECT().Get(gomock.Any(), api.CollectionLatestCheckpoint, s.config.Tag.Stable).Return(&api.Checkpoint{Sequence: maxSequence}, nil)
	s.ethereumStorage.EXPECT().GetBlock(gomock.Any(), s.config.Tag.Stable, height, maxSequence).Return(&ethereum.Block{Data: header}, nil)

	resp, err := s.receiver.GetTransactionByBlockNumberAndIndex(context.Background(), rpc.BlockNumber(height), rpc.DecimalOrHex(0))
	require.NoError(err)
	require.Equal(string(tx), string(resp))
}

func (s *handlerTestSuite) TestGetTransactionByBlockNumberAndIndex_Success_Latest() {
	require := testutil.Require(s.T())

	header, err := fixtures.ReadJson("controller/ethereum/eth_header.json")
	require.NoError(err)

	tx, err := fixtures.ReadJson("controller/ethereum/eth_transaction_1.json")
	require.NoError(err)

	maxSequence := api.Sequence(1)
	latestBlockNumber := uint64(123)
	s.checkpointer.EXPECT().Get(gomock.Any(), api.CollectionLatestCheckpoint, s.config.Tag.Stable).Return(&api.Checkpoint{Sequence: maxSequence, Height: latestBlockNumber}, nil)
	s.ethereumStorage.EXPECT().GetBlock(gomock.Any(), s.config.Tag.Stable, latestBlockNumber, maxSequence).Return(&ethereum.Block{Data: header}, nil)

	resp, err := s.receiver.GetTransactionByBlockNumberAndIndex(context.Background(), rpc.BlockNumber(-1), rpc.DecimalOrHex(0))
	require.NoError(err)
	require.Equal(string(tx), string(resp))
}

func (s *handlerTestSuite) TestGetTransactionByBlockNumberAndIndexError() {
	require := testutil.Require(s.T())

	sampleErrMsg := "sample error"
	sampleErr := xerrors.New(sampleErrMsg)

	maxSequence := api.Sequence(1)
	height := uint64(123)
	s.checkpointer.EXPECT().Get(gomock.Any(), api.CollectionLatestCheckpoint, s.config.Tag.Stable).Return(&api.Checkpoint{Sequence: maxSequence}, nil)
	s.ethereumStorage.EXPECT().GetBlock(gomock.Any(), s.config.Tag.Stable, height, maxSequence).Return(nil, sampleErr)

	resp, err := s.receiver.GetTransactionByBlockNumberAndIndex(context.Background(), rpc.BlockNumber(height), rpc.DecimalOrHex(0))
	require.Nil(resp)
	require.Error(err)
	require.Contains(err.Error(), sampleErrMsg)
}

func (s *handlerTestSuite) TestGetTransactionByBlockNumberAndIndexOutOfBound() {
	require := testutil.Require(s.T())

	header, err := fixtures.ReadJson("controller/ethereum/eth_header.json")
	require.NoError(err)

	maxSequence := api.Sequence(1)
	height := uint64(123)
	s.checkpointer.EXPECT().Get(gomock.Any(), api.CollectionLatestCheckpoint, s.config.Tag.Stable).Return(&api.Checkpoint{Sequence: maxSequence}, nil)
	s.ethereumStorage.EXPECT().GetBlock(gomock.Any(), s.config.Tag.Stable, height, maxSequence).Return(&ethereum.Block{Data: header}, nil)

	resp, err := s.receiver.GetTransactionByBlockNumberAndIndex(context.Background(), rpc.BlockNumber(height), rpc.DecimalOrHex(2))
	require.NoError(err)
	require.Nil(resp)
}

func (s *handlerTestSuite) TestGetTransactionByBlockNumberAndIndex_Success_Pending() {
	require := testutil.Require(s.T())

	tx, err := fixtures.ReadJson("controller/ethereum/eth_transaction_1.json")
	require.NoError(err)

	s.jsonrpcClient.EXPECT().Call(gomock.Any(), EthGetTransactionByBlockNumberAndIndex, gomock.Any()).Times(1).DoAndReturn(
		func(ctx context.Context, method *jsonrpc.RequestMethod, params jsonrpc.Params) (*jsonrpc.Response, error) {
			require.Len(params, 2)
			return &jsonrpc.Response{
				Result: tx,
				Error:  nil,
			}, nil
		},
	)

	resp, err := s.receiver.GetTransactionByBlockNumberAndIndex(context.Background(), rpc.PendingBlockNumber, rpc.DecimalOrHex(0))
	require.NoError(err)
	require.Equal(string(tx), string(resp))
}

func (s *handlerTestSuite) TestGetTransactionByBlockNumberAndIndex_Success_NativeOnly_NonPending() {
	require := testutil.Require(s.T())

	header, err := fixtures.ReadJson("controller/ethereum/eth_header.json")
	require.NoError(err)

	tx, err := fixtures.ReadJson("controller/ethereum/eth_transaction_1.json")
	require.NoError(err)

	maxSequence := api.Sequence(1)
	height := uint64(123)
	s.checkpointer.EXPECT().Get(gomock.Any(), api.CollectionLatestCheckpoint, s.config.Tag.Stable).Return(&api.Checkpoint{Sequence: maxSequence}, nil)
	s.ethereumStorage.EXPECT().GetBlock(gomock.Any(), s.config.Tag.Stable, height, maxSequence).Return(&ethereum.Block{Data: header}, nil)

	resp, err := s.receiver.GetTransactionByBlockNumberAndIndex(newContextWithMode(constants.NativeOnlyMode), rpc.BlockNumber(height), rpc.DecimalOrHex(0))
	require.NoError(err)
	require.Equal(string(tx), string(resp))
}

func (s *handlerTestSuite) TestGetTransactionByBlockNumberAndIndex_Fail_NativeOnly_Pending() {
	require := testutil.Require(s.T())
	resp, err := s.receiver.GetTransactionByBlockNumberAndIndex(newContextWithMode(constants.NativeOnlyMode), rpc.PendingBlockNumber, rpc.DecimalOrHex(0))
	require.Equal(xerrors.Errorf(invalidDispatchModeErrString, "eth_getTransactionByBlockNumberAndIndex", api.ErrNotAllowed).Error(), err.Error())
	s.validateErrorCode(errorCodeBadRequest, err)
	require.Nil(resp)
}

func (s *handlerTestSuite) TestGetTransactionReceipt() {
	require := testutil.Require(s.T())

	receipt, err := fixtures.ReadJson("controller/ethereum/eth_transactionreceipt_1.json")
	require.NoError(err)

	maxSequence := api.Sequence(1)
	txHash := common.HexToHash(sampleTxHash)
	s.checkpointer.EXPECT().Get(gomock.Any(), api.CollectionLatestCheckpoint, s.config.Tag.Stable).Return(&api.Checkpoint{Sequence: maxSequence}, nil)
	s.ethereumStorage.EXPECT().GetTransactionReceipt(gomock.Any(), s.config.Tag.Stable, sampleTxHash, maxSequence).Return(&ethereum.TransactionReceipt{Data: receipt}, nil)

	resp, err := s.receiver.GetTransactionReceipt(context.Background(), txHash)
	require.NoError(err)
	require.Equal(string(receipt), string(resp))
}

func (s *handlerTestSuite) TestGetTransactionReceiptWithCachedCheckpoint() {
	require := testutil.Require(s.T())

	receipt, err := fixtures.ReadJson("controller/ethereum/eth_transactionreceipt_1.json")
	require.NoError(err)

	maxSequence := api.Sequence(1)
	txHash := common.HexToHash(sampleTxHash)
	s.ethereumStorage.EXPECT().GetTransactionReceipt(gomock.Any(), s.config.Tag.Stable, sampleTxHash, maxSequence).Return(&ethereum.TransactionReceipt{Data: receipt}, nil)

	ctx := context.WithValue(context.Background(), constants.ContextKeyLatestCheckpoint, &api.Checkpoint{Sequence: maxSequence})
	resp, err := s.receiver.GetTransactionReceipt(ctx, txHash)
	require.NoError(err)
	require.Equal(string(receipt), string(resp))
}

func (s *handlerTestSuite) TestGetTransactionReceiptError() {
	require := testutil.Require(s.T())

	sampleErrMsg := "sample error"
	sampleErr := xerrors.New(sampleErrMsg)

	maxSequence := api.Sequence(1)
	txHash := common.HexToHash(sampleTxHash)
	s.checkpointer.EXPECT().Get(gomock.Any(), api.CollectionLatestCheckpoint, s.config.Tag.Stable).Return(&api.Checkpoint{Sequence: maxSequence}, nil)
	s.ethereumStorage.EXPECT().GetTransactionReceipt(gomock.Any(), s.config.Tag.Stable, sampleTxHash, maxSequence).Return(nil, sampleErr)

	resp, err := s.receiver.GetTransactionReceipt(context.Background(), txHash)
	require.Nil(resp)
	require.Error(err)
	require.Contains(err.Error(), sampleErrMsg)
}

func (s *handlerTestSuite) TestGetTransactionReceiptEmptyReceipt() {
	require := testutil.Require(s.T())

	receipt := make([]byte, 0)

	maxSequence := api.Sequence(1)
	txHash := common.HexToHash(sampleTxHash)
	s.checkpointer.EXPECT().Get(gomock.Any(), api.CollectionLatestCheckpoint, s.config.Tag.Stable).Return(&api.Checkpoint{Sequence: maxSequence}, nil)
	s.ethereumStorage.EXPECT().GetTransactionReceipt(gomock.Any(), s.config.Tag.Stable, sampleTxHash, maxSequence).Return(&ethereum.TransactionReceipt{Data: receipt}, nil)

	resp, err := s.receiver.GetTransactionReceipt(context.Background(), txHash)
	require.NoError(err)
	require.Equal(string(receipt), string(resp))
}

func (s *handlerTestSuite) TestGetTransactionReceiptNotFound() {
	require := testutil.Require(s.T())

	maxSequence := api.Sequence(1)
	txHash := common.HexToHash(sampleTxHash)
	s.checkpointer.EXPECT().Get(gomock.Any(), api.CollectionLatestCheckpoint, s.config.Tag.Stable).Return(&api.Checkpoint{Sequence: maxSequence}, nil)
	s.ethereumStorage.EXPECT().GetTransactionReceipt(gomock.Any(), s.config.Tag.Stable, sampleTxHash, maxSequence).Return(nil, storage.ErrItemNotFound)

	resp, err := s.receiver.GetTransactionReceipt(context.Background(), txHash)
	require.NoError(err)
	require.Nil(resp)
}

func (s *handlerTestSuite) TestTraceBlockByHash() {
	require := testutil.Require(s.T())

	trace, err := fixtures.ReadJson("controller/ethereum/eth_trace.json")
	require.NoError(err)

	maxSequence := api.Sequence(1)
	blockHash := common.HexToHash(sampleBlockHash)
	s.checkpointer.EXPECT().Get(gomock.Any(), api.CollectionLatestCheckpoint, s.config.Tag.Stable).Return(&api.Checkpoint{Sequence: maxSequence}, nil)
	s.ethereumStorage.EXPECT().GetTraceByHash(gomock.Any(), s.config.Tag.Stable, sampleBlockHash, maxSequence).Return(&ethereum.Trace{Data: trace}, nil)

	resp, err := s.receiver.TraceBlockByHash(context.Background(), blockHash, &tracers.TraceConfig{Tracer: pointer.String(ethCallTracer)})
	require.NoError(err)
	require.Equal(string(trace), string(resp))
}

func (s *handlerTestSuite) TestTraceBlockByHash_NoTracer() {
	require := testutil.Require(s.T())

	blockHash := common.HexToHash(sampleBlockHash)
	resp, err := s.receiver.TraceBlockByHash(context.Background(), blockHash, &tracers.TraceConfig{})
	require.Error(err)
	require.Nil(resp)
	require.Contains(err.Error(), "not implemented")
}

func (s *handlerTestSuite) TestTraceBlockByHash_WithTracer() {
	require := testutil.Require(s.T())

	if s.blockchain == c3common.Blockchain_BLOCKCHAIN_POLYGON {
		trace, err := fixtures.ReadJson("controller/ethereum/eth_trace.json")
		require.NoError(err)

		maxSequence := api.Sequence(1)
		maxHeight := uint64(123)
		blockHash := common.HexToHash(sampleBlockHash)
		s.checkpointer.EXPECT().Get(gomock.Any(), api.CollectionLatestCheckpoint, s.config.Tag.Stable).Return(
			&api.Checkpoint{Sequence: maxSequence, Height: uint64(maxHeight)}, nil)
		s.ethereumStorage.EXPECT().GetTraceByHash(gomock.Any(), s.config.Tag.Stable, sampleBlockHash, maxSequence).Return(&ethereum.Trace{Data: trace}, nil)

		resp, err := s.receiver.TraceBlockByHash(context.Background(), blockHash, &tracers.TraceConfig{Tracer: pointer.String("Customized tracer")})
		require.NoError(err)
		require.Equal(string(trace), string(resp))
	} else {
		blockHash := common.HexToHash(sampleBlockHash)
		resp, err := s.receiver.TraceBlockByHash(context.Background(), blockHash, &tracers.TraceConfig{Tracer: pointer.String("foo")})
		require.Error(err)
		require.Nil(resp)
		require.Contains(err.Error(), "not implemented")
	}
}

func (s *handlerTestSuite) TestTraceBlockByHash_CallTracerWithTracerConfig_Err() {
	require := testutil.Require(s.T())

	blockHash := common.HexToHash(sampleBlockHash)
	traceConfig := &tracers.TraceConfig{
		Tracer:       pointer.String(ethCallTracer),
		TracerConfig: json.RawMessage(`{"onlyTopCall": true}`),
	}
	resp, err := s.receiver.TraceBlockByHash(context.Background(), blockHash, traceConfig)
	require.Error(err)
	require.Nil(resp)
	require.Contains(err.Error(), "not implemented")
}

func (s *handlerTestSuite) TestTraceBlockByHash_BlockNotFound() {
	require := testutil.Require(s.T())

	maxSequence := api.Sequence(1)
	blockHash := common.HexToHash(sampleBlockHash)
	s.checkpointer.EXPECT().Get(gomock.Any(), api.CollectionLatestCheckpoint, s.config.Tag.Stable).Return(&api.Checkpoint{Sequence: maxSequence}, nil)
	s.ethereumStorage.EXPECT().GetTraceByHash(gomock.Any(), s.config.Tag.Stable, sampleBlockHash, maxSequence).Return(nil, storage.ErrItemNotFound)

	resp, err := s.receiver.TraceBlockByHash(context.Background(), blockHash, &tracers.TraceConfig{Tracer: pointer.String(ethCallTracer)})
	require.Error(err)
	require.Nil(resp)
	require.Equal("block 0xb3e232495a99170e43c583daa9035a993bd66ddfad5ccc636b6aad26e6e38056 not found", err.Error())

	rpcErr, ok := err.(rpc.Error)
	require.True(ok)
	require.Equal(errorCodeBadRequest, rpcErr.ErrorCode())
}

func (s *handlerTestSuite) TestTraceBlockByNumber() {
	require := testutil.Require(s.T())

	trace, err := fixtures.ReadJson("controller/ethereum/eth_trace.json")
	require.NoError(err)

	maxSequence := api.Sequence(1)
	height := uint64(12345)
	maxHeight := height + 10
	s.checkpointer.EXPECT().Get(gomock.Any(), api.CollectionLatestCheckpoint, s.config.Tag.Stable).Return(
		&api.Checkpoint{Sequence: maxSequence, Height: maxHeight}, nil)
	s.ethereumStorage.EXPECT().GetTraceByNumber(gomock.Any(), s.config.Tag.Stable, height, maxSequence).Return(&ethereum.Trace{Data: trace}, nil)

	resp, err := s.receiver.TraceBlockByNumber(context.Background(), rpc.BlockNumber(height), &tracers.TraceConfig{Tracer: pointer.String(ethCallTracer)})
	require.NoError(err)
	require.Equal(string(trace), string(resp))
}

func (s *handlerTestSuite) TestTraceBlockByNumber_WithTracer() {
	require := testutil.Require(s.T())

	if s.blockchain == c3common.Blockchain_BLOCKCHAIN_POLYGON {
		trace, err := fixtures.ReadJson("controller/ethereum/eth_trace.json")
		require.NoError(err)

		maxSequence := api.Sequence(1)
		maxHeight := uint64(123)
		s.checkpointer.EXPECT().Get(gomock.Any(), api.CollectionLatestCheckpoint, s.config.Tag.Stable).Return(
			&api.Checkpoint{Sequence: maxSequence, Height: uint64(maxHeight)}, nil)
		s.ethereumStorage.EXPECT().GetTraceByNumber(gomock.Any(), s.config.Tag.Stable, maxHeight, maxSequence).Return(&ethereum.Trace{Data: trace}, nil)

		resp, err := s.receiver.TraceBlockByNumber(context.Background(), rpc.LatestBlockNumber, &tracers.TraceConfig{Tracer: pointer.String("Customized tracer")})
		require.NoError(err)
		require.Equal(string(trace), string(resp))
	} else {
		height := uint64(12345)
		resp, err := s.receiver.TraceBlockByNumber(context.Background(), rpc.BlockNumber(height), &tracers.TraceConfig{})
		require.Error(err)
		require.Nil(resp)
		require.Contains(err.Error(), "not implemented")
	}
}

func (s *handlerTestSuite) TestTraceBlockByNumber_CallTracerWithTracerConfig_Err() {
	require := testutil.Require(s.T())

	traceConfig := &tracers.TraceConfig{
		Tracer:       pointer.String(ethCallTracer),
		TracerConfig: json.RawMessage(`{"onlyTopCall": true}`),
	}
	resp, err := s.receiver.TraceBlockByNumber(context.Background(), rpc.LatestBlockNumber, traceConfig)
	require.Error(err)
	require.Nil(resp)
	require.Contains(err.Error(), "not implemented")
}

func (s *handlerTestSuite) TestTraceBlockByNumber_BlockNotFound() {
	require := testutil.Require(s.T())

	maxSequence := api.Sequence(1)
	height := uint64(12345)
	s.checkpointer.EXPECT().Get(gomock.Any(), api.CollectionLatestCheckpoint, s.config.Tag.Stable).Return(
		&api.Checkpoint{Sequence: maxSequence}, nil)
	s.ethereumStorage.EXPECT().GetTraceByNumber(gomock.Any(), s.config.Tag.Stable, height, maxSequence).Return(nil, storage.ErrItemNotFound)

	resp, err := s.receiver.TraceBlockByNumber(context.Background(), rpc.BlockNumber(height), &tracers.TraceConfig{Tracer: pointer.String(ethCallTracer)})
	require.Error(err)
	require.Nil(resp)
	require.Equal("block 12345 not found", err.Error())

	rpcErr, ok := err.(rpc.Error)
	require.True(ok)
	require.Equal(errorCodeBadRequest, rpcErr.ErrorCode())
}

func (s *handlerTestSuite) TestTraceBlockByNumber_Genesis() {
	require := testutil.Require(s.T())

	height := uint64(0)
	resp, err := s.receiver.TraceBlockByNumber(context.Background(), rpc.BlockNumber(height), &tracers.TraceConfig{Tracer: pointer.String(ethCallTracer)})
	require.Error(err)
	require.Contains(err.Error(), "genesis is not traceable")
	require.Nil(resp)
}

func (s *handlerTestSuite) TestTraceBlockByNumber_Latest() {
	require := testutil.Require(s.T())

	trace, err := fixtures.ReadJson("controller/ethereum/eth_trace.json")
	require.NoError(err)

	maxSequence := api.Sequence(1)
	maxHeight := uint64(123)
	s.checkpointer.EXPECT().Get(gomock.Any(), api.CollectionLatestCheckpoint, s.config.Tag.Stable).Return(
		&api.Checkpoint{Sequence: maxSequence, Height: uint64(maxHeight)}, nil)
	s.ethereumStorage.EXPECT().GetTraceByNumber(gomock.Any(), s.config.Tag.Stable, maxHeight, maxSequence).Return(&ethereum.Trace{Data: trace}, nil)

	resp, err := s.receiver.TraceBlockByNumber(context.Background(), rpc.LatestBlockNumber, &tracers.TraceConfig{Tracer: pointer.String(ethCallTracer)})
	require.NoError(err)
	require.Equal(string(trace), string(resp))
}

func (s *handlerTestSuite) TestTraceBlockByNumber_DefaultBlockNumbers() {
	require := testutil.Require(s.T())

	blkNum := rpc.PendingBlockNumber
	resp, err := s.receiver.TraceBlockByNumber(context.Background(), blkNum, &tracers.TraceConfig{Tracer: pointer.String(ethCallTracer)})
	require.Nil(resp)
	require.Error(err)
	require.Contains(err.Error(), "not implemented")
}

func (s *handlerTestSuite) TestChainId() {
	require := testutil.Require(s.T())

	resp, err := s.receiver.ChainId(context.Background())
	require.NoError(err)

	switch s.blockchain {
	case c3common.Blockchain_BLOCKCHAIN_ETHEREUM:
		require.Equal(`"0x1"`, string(resp))
	case c3common.Blockchain_BLOCKCHAIN_POLYGON:
		require.Equal(`"0x89"`, string(resp))
	default:
		s.Fail("chain id not defined")
	}
}

func (s *handlerTestSuite) TestVersion() {
	require := testutil.Require(s.T())

	resp, err := s.receiver.Version(context.Background())
	require.NoError(err)

	switch s.blockchain {
	case c3common.Blockchain_BLOCKCHAIN_ETHEREUM:
		require.Equal(`"1"`, string(resp))
	case c3common.Blockchain_BLOCKCHAIN_POLYGON:
		require.Equal(`"137"`, string(resp))
	default:
		s.Fail("version not defined")
	}
}

func (s *handlerTestSuite) TestProtocolVersion() {
	require := testutil.Require(s.T())
	output := &jsonrpc.Response{
		Result: json.RawMessage(""),
		Error:  nil,
	}
	s.jsonrpcClient.EXPECT().Call(gomock.Any(), EthProtocolVersion, nil).Times(1).DoAndReturn(
		func(ctx context.Context, method *jsonrpc.RequestMethod, params jsonrpc.Params) (*jsonrpc.Response, error) {
			require.Len(params, 0)
			return output, nil
		},
	)

	resp, err := s.receiver.ProtocolVersion(context.Background())
	require.NoError(err)
	require.Equal("", string(resp))
}

func (s *handlerTestSuite) TestProtocolVersion_Fail_NativeOnly() {
	require := testutil.Require(s.T())
	resp, err := s.receiver.ProtocolVersion(newContextWithMode(constants.NativeOnlyMode))
	require.Equal(xerrors.Errorf(invalidDispatchModeErrString, "eth_protocolVersion", api.ErrNotAllowed).Error(), err.Error())
	s.validateErrorCode(errorCodeBadRequest, err)
	require.Nil(resp)
}

func (s *handlerTestSuite) TestSyncing() {
	require := testutil.Require(s.T())

	resp, err := s.receiver.Syncing(context.Background())
	require.NoError(err)
	require.Equal(`false`, string(resp))
}

func (s *handlerTestSuite) TestFeeHistory() {
	require := testutil.Require(s.T())
	output := &jsonrpc.Response{
		Result: json.RawMessage(""),
		Error:  nil,
	}
	s.jsonrpcClient.EXPECT().Call(gomock.Any(), EthFeeHistory, gomock.Any()).Times(1).DoAndReturn(
		func(ctx context.Context, method *jsonrpc.RequestMethod, params jsonrpc.Params) (*jsonrpc.Response, error) {
			require.Len(params, 3)
			return output, nil
		},
	)

	resp, err := s.receiver.FeeHistory(context.Background(), nil, nil, nil)
	require.NoError(err)
	require.Equal("", string(resp))
}

func (s *handlerTestSuite) TestFeeHistory_Fail_NativeOnly() {
	require := testutil.Require(s.T())
	resp, err := s.receiver.FeeHistory(newContextWithMode(constants.NativeOnlyMode), nil, nil, nil)
	require.Equal(xerrors.Errorf(invalidDispatchModeErrString, "eth_feeHistory", api.ErrNotAllowed).Error(), err.Error())
	s.validateErrorCode(errorCodeBadRequest, err)
	require.Nil(resp)
}

func (s *handlerTestSuite) TestListening() {
	require := testutil.Require(s.T())

	resp, err := s.receiver.Listening(context.Background())
	require.NoError(err)
	require.Equal(`true`, string(resp))
}

func (s *handlerTestSuite) TestPeerCount() {
	require := testutil.Require(s.T())
	output := &jsonrpc.Response{
		Result: json.RawMessage(""),
		Error:  nil,
	}
	s.jsonrpcClient.EXPECT().Call(gomock.Any(), NetPeerCount, nil).Times(1).DoAndReturn(
		func(ctx context.Context, method *jsonrpc.RequestMethod, params jsonrpc.Params) (*jsonrpc.Response, error) {
			require.Len(params, 0)
			return output, nil
		},
	)

	resp, err := s.receiver.PeerCount(context.Background())
	require.NoError(err)
	require.Equal("", string(resp))
}

func (s *handlerTestSuite) TestPeerCount_Fail_NativeOnly() {
	require := testutil.Require(s.T())
	resp, err := s.receiver.PeerCount(newContextWithMode(constants.NativeOnlyMode))
	require.Equal(xerrors.Errorf(invalidDispatchModeErrString, "net_peerCount", api.ErrNotAllowed).Error(), err.Error())
	s.validateErrorCode(errorCodeBadRequest, err)
	require.Nil(resp)
}

func (s *handlerTestSuite) TestClientVersion_Fail_NativeOnly() {
	require := testutil.Require(s.T())
	resp, err := s.receiver.ClientVersion(newContextWithMode(constants.NativeOnlyMode))
	require.Equal(xerrors.Errorf(invalidDispatchModeErrString, "web3_clientVersion", api.ErrNotAllowed).Error(), err.Error())
	s.validateErrorCode(errorCodeBadRequest, err)
	require.Nil(resp)
}

func (s *handlerTestSuite) TestGetAuthor() {
	require := testutil.Require(s.T())
	cfg, err := config.New(config.WithBlockchain(c3common.Blockchain_BLOCKCHAIN_POLYGON), config.WithNetwork(c3common.Network_NETWORK_POLYGON_MAINNET))
	require.NoError(err)

	// Disable shadowing.
	cfg.Controller.Handler.ShadowPercentage = 0

	s.app = testapp.New(
		s.T(),
		testapp.WithConfig(cfg),
		fx.Provide(fx.Annotated{Name: "proxy", Target: func() jsonrpc.Client { return s.jsonrpcClient }}),
		fx.Provide(fx.Annotated{Name: "validator", Target: func() jsonrpc.Client { return s.jsonrpcClient }}),
		fx.Provide(func() ethereumStorage.BlockStorage { return s.ethereumStorage }),
		fx.Provide(func() ethereumStorage.BlockByHashStorage { return s.ethereumStorage }),
		fx.Provide(func() ethereumStorage.BlockByHashWithoutFullTxStorage { return s.ethereumStorage }),
		fx.Provide(func() ethereumStorage.LogStorageV2 { return s.ethereumStorage }),
		fx.Provide(func() ethereumStorage.TransactionStorage { return s.ethereumStorage }),
		fx.Provide(func() ethereumStorage.TransactionReceiptStorage { return s.ethereumStorage }),
		fx.Provide(func() ethereumStorage.TraceByHashStorage { return s.ethereumStorage }),
		fx.Provide(func() ethereumStorage.TraceByNumberStorage { return s.ethereumStorage }),
		fx.Provide(func() ethereumStorage.ArbtraceBlockStorage { return s.ethereumStorage }),
		fx.Provide(func() ethereumStorage.BlockExtraDataByNumberStorage { return s.ethereumStorage }),
		fx.Provide(func() internal.Checkpointer { return s.checkpointer }),
		fx.Provide(NewHandler),
		fx.Provide(NewValidator),
		fx.Populate(&s.handler),
		fx.Populate(&s.config),
	)
	s.receiver = s.handler.Receiver().(Receiver)

	maxSequence := api.Sequence(1)
	height := uint64(12345)
	author := []byte("0x742d13f0b2a19c823bdd362b16305e4704b97a38")
	tag := s.config.Tag.Stable
	block := &chainstorageapi.Block{
		Metadata: testutil.MakeBlockMetadata(height, tag),
		Blobdata: &chainstorageapi.Block_Ethereum{
			Ethereum: &chainstorageapi.EthereumBlobdata{
				ExtraData: &chainstorageapi.EthereumBlobdata_Polygon{
					Polygon: &chainstorageapi.PolygonExtraData{
						Author: author,
					},
				},
			},
		},
	}
	data, err := proto.Marshal(block.GetEthereum().GetPolygon())
	require.NoError(err)
	s.checkpointer.EXPECT().Get(gomock.Any(), api.CollectionLatestCheckpoint, s.config.Tag.Stable).Return(&api.Checkpoint{Sequence: maxSequence}, nil)
	s.ethereumStorage.EXPECT().GetBlockExtraDataByNumber(gomock.Any(), tag, height, maxSequence).Return(&ethereum.BlockExtraData{Data: data}, nil)

	resp, err := s.receiver.GetAuthor(context.Background(), rpc.BlockNumber(height))
	require.NoError(err)
	require.Equal(string(author), string(resp))
}

func (s *handlerTestSuite) TestGetAuthor_NotFound() {
	require := testutil.Require(s.T())
	cfg, err := config.New(config.WithBlockchain(c3common.Blockchain_BLOCKCHAIN_POLYGON), config.WithNetwork(c3common.Network_NETWORK_POLYGON_MAINNET))
	require.NoError(err)

	// Disable shadowing.
	cfg.Controller.Handler.ShadowPercentage = 0

	s.app = testapp.New(
		s.T(),
		testapp.WithConfig(cfg),
		fx.Provide(fx.Annotated{Name: "proxy", Target: func() jsonrpc.Client { return s.jsonrpcClient }}),
		fx.Provide(fx.Annotated{Name: "validator", Target: func() jsonrpc.Client { return s.jsonrpcClient }}),
		fx.Provide(func() ethereumStorage.BlockStorage { return s.ethereumStorage }),
		fx.Provide(func() ethereumStorage.BlockByHashStorage { return s.ethereumStorage }),
		fx.Provide(func() ethereumStorage.BlockByHashWithoutFullTxStorage { return s.ethereumStorage }),
		fx.Provide(func() ethereumStorage.LogStorageV2 { return s.ethereumStorage }),
		fx.Provide(func() ethereumStorage.TransactionStorage { return s.ethereumStorage }),
		fx.Provide(func() ethereumStorage.TransactionReceiptStorage { return s.ethereumStorage }),
		fx.Provide(func() ethereumStorage.TraceByHashStorage { return s.ethereumStorage }),
		fx.Provide(func() ethereumStorage.TraceByNumberStorage { return s.ethereumStorage }),
		fx.Provide(func() ethereumStorage.ArbtraceBlockStorage { return s.ethereumStorage }),
		fx.Provide(func() ethereumStorage.BlockExtraDataByNumberStorage { return s.ethereumStorage }),
		fx.Provide(func() internal.Checkpointer { return s.checkpointer }),
		fx.Provide(NewHandler),
		fx.Provide(NewValidator),
		fx.Populate(&s.handler),
		fx.Populate(&s.config),
	)
	s.receiver = s.handler.Receiver().(Receiver)

	maxSequence := api.Sequence(1)
	height := uint64(12345)
	tag := s.config.Tag.Stable
	require.NoError(err)
	s.checkpointer.EXPECT().Get(gomock.Any(), api.CollectionLatestCheckpoint, s.config.Tag.Stable).Return(&api.Checkpoint{Sequence: maxSequence}, nil)
	s.ethereumStorage.EXPECT().GetBlockExtraDataByNumber(gomock.Any(), tag, height, maxSequence).Return(nil, storage.ErrItemNotFound)

	resp, err := s.receiver.GetAuthor(context.Background(), rpc.BlockNumber(height))
	require.Error(err)
	require.Nil(resp)
	require.Contains(err.Error(), "unknown block")
}

func (s *handlerTestSuite) TestGetAuthor_NilExtraData() {
	require := testutil.Require(s.T())
	cfg, err := config.New(config.WithBlockchain(c3common.Blockchain_BLOCKCHAIN_POLYGON), config.WithNetwork(c3common.Network_NETWORK_POLYGON_MAINNET))
	require.NoError(err)

	// Disable shadowing.
	cfg.Controller.Handler.ShadowPercentage = 0

	s.app = testapp.New(
		s.T(),
		testapp.WithConfig(cfg),
		fx.Provide(fx.Annotated{Name: "proxy", Target: func() jsonrpc.Client { return s.jsonrpcClient }}),
		fx.Provide(fx.Annotated{Name: "validator", Target: func() jsonrpc.Client { return s.jsonrpcClient }}),
		fx.Provide(func() ethereumStorage.BlockStorage { return s.ethereumStorage }),
		fx.Provide(func() ethereumStorage.BlockByHashStorage { return s.ethereumStorage }),
		fx.Provide(func() ethereumStorage.BlockByHashWithoutFullTxStorage { return s.ethereumStorage }),
		fx.Provide(func() ethereumStorage.LogStorageV2 { return s.ethereumStorage }),
		fx.Provide(func() ethereumStorage.TransactionStorage { return s.ethereumStorage }),
		fx.Provide(func() ethereumStorage.TransactionReceiptStorage { return s.ethereumStorage }),
		fx.Provide(func() ethereumStorage.TraceByHashStorage { return s.ethereumStorage }),
		fx.Provide(func() ethereumStorage.TraceByNumberStorage { return s.ethereumStorage }),
		fx.Provide(func() ethereumStorage.ArbtraceBlockStorage { return s.ethereumStorage }),
		fx.Provide(func() ethereumStorage.BlockExtraDataByNumberStorage { return s.ethereumStorage }),
		fx.Provide(func() internal.Checkpointer { return s.checkpointer }),
		fx.Provide(NewHandler),
		fx.Provide(NewValidator),
		fx.Populate(&s.handler),
		fx.Populate(&s.config),
	)
	s.receiver = s.handler.Receiver().(Receiver)

	maxSequence := api.Sequence(1)
	height := uint64(12345)
	tag := s.config.Tag.Stable
	require.NoError(err)
	s.checkpointer.EXPECT().Get(gomock.Any(), api.CollectionLatestCheckpoint, s.config.Tag.Stable).Return(&api.Checkpoint{Sequence: maxSequence}, nil)
	s.ethereumStorage.EXPECT().GetBlockExtraDataByNumber(gomock.Any(), tag, height, maxSequence).Return(&ethereum.BlockExtraData{Data: nil}, nil)

	resp, err := s.receiver.GetAuthor(context.Background(), rpc.BlockNumber(height))
	require.Nil(resp)
	require.Contains(err.Error(), "unknown author")
}

func (s *handlerTestSuite) TestPrepareContext() {
	tests := []struct {
		name                  string
		shouldCacheCheckpoint bool
		fixture               string
		ctx                   context.Context
		expectedError         error
	}{
		{
			name:                  "singular",
			shouldCacheCheckpoint: true,
			fixture:               "controller/ethereum/eth_getTransactionReceipt_singular.json",
			ctx:                   context.Background(),
			expectedError:         nil,
		},
		{
			name:                  "proxy_singular",
			shouldCacheCheckpoint: false,
			fixture:               "controller/ethereum/eth_call.json",
			ctx:                   context.Background(),
			expectedError:         nil,
		},
		{
			name:                  "batch",
			shouldCacheCheckpoint: true,
			fixture:               "controller/ethereum/eth_getTransactionReceipt_batch.json",
			ctx:                   context.Background(),
			expectedError:         nil,
		},
		{
			name:                  "proxy_batch",
			shouldCacheCheckpoint: false,
			fixture:               "controller/ethereum/eth_call_batch.json",
			ctx:                   context.Background(),
			expectedError:         nil,
		},
		{
			name:                  "invalid routing mode header",
			shouldCacheCheckpoint: false,
			fixture:               "controller/ethereum/eth_getTransactionReceipt_singular.json",
			ctx:                   newContextWithMode(constants.InvalidMode),
			expectedError:         api.NewServerError(http.StatusBadRequest, api.ErrInvalidHttpHeaderValue),
		},
	}
	for _, test := range tests {
		s.Run(test.name, func() {
			require := testutil.Require(s.T())

			if test.shouldCacheCheckpoint {
				s.checkpointer.EXPECT().Get(gomock.Any(), api.CollectionLatestCheckpoint, s.config.Tag.Stable).Return(&api.Checkpoint{Height: 123}, nil)
			}

			var err error
			ctx, err := s.handler.PrepareContext(test.ctx, fixtures.MustReadFile(test.fixture))
			if test.expectedError == nil {
				require.NoError(err)
				checkpoint, ok := ctx.Value(constants.ContextKeyLatestCheckpoint).(*api.Checkpoint)
				if test.shouldCacheCheckpoint {
					require.True(ok)
					require.NotNil(checkpoint)
				} else {
					require.False(ok)
					require.Nil(checkpoint)
				}
			} else {
				require.Equal(test.expectedError.Error(), err.Error())
			}
		})
	}
}

func (s *handlerTestSuite) TestHandlerMapError() {
	tests := []struct {
		name     string
		expected int
		input    error
	}{
		{
			name:     "context.Canceled",
			expected: api.StatusCanceled,
			input:    xerrors.Errorf("mock error: %w", context.Canceled),
		},
		{
			name:     "ErrRequestCanceled",
			expected: api.StatusCanceled,
			input:    xerrors.Errorf("mock error: %w", storage.ErrRequestCanceled),
		},
		{
			name:     "Others",
			expected: http.StatusInternalServerError,
			input:    xerrors.Errorf("mock error: %w", storage.ErrItemNotFound),
		},
	}
	for _, test := range tests {
		s.Run(test.name, func() {
			require := testutil.Require(s.T())
			handler := s.handler.(*handler)
			err := handler.mapError(test.input)
			var serverErr *api.ServerError
			require.True(xerrors.As(err, &serverErr))
			require.Equal(test.expected, serverErr.HTTPStatus())
		})
	}
}

func newContextWithMode(dispatchMode constants.DispatchMode) context.Context {
	md := make(metadata.MD)
	ctx := metadata.NewIncomingContext(context.Background(), md)
	md[constants.RoutingModeHttpHeaderName] = []string{string(dispatchMode)}

	return ctx
}

func (s *handlerTestSuite) validateErrorCode(expectedErrorCode int, err error) {
	require := testutil.Require(s.T())
	rpcError, ok := err.(rpc.Error)
	require.True(ok)
	require.Equal(expectedErrorCode, rpcError.ErrorCode())
}
