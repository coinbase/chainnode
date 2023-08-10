package handler_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/eth/tracers"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainnode/internal/api"
	"github.com/coinbase/chainnode/internal/clients/blockchain/jsonrpc"
	"github.com/coinbase/chainnode/internal/controller/ethereum/handler"
	handlermocks "github.com/coinbase/chainnode/internal/controller/ethereum/handler/mocks"
	"github.com/coinbase/chainnode/internal/server/rpc"
	"github.com/coinbase/chainnode/internal/storage"
	"github.com/coinbase/chainnode/internal/utils/testutil"
)

type ErrorInterceptorTestSuite struct {
	suite.Suite
	ctrl        *gomock.Controller
	receiver    *handlermocks.MockReceiver
	interceptor handler.Receiver
}

const (
	errorCodeGeneric    = -32000
	errorCodeBadRequest = -32097
	errorCodeCanceled   = -32098
	errorCodeInternal   = -32099
)

func TestErrorInterceptorTestSuite(t *testing.T) {
	suite.Run(t, new(ErrorInterceptorTestSuite))
}

func (s *ErrorInterceptorTestSuite) SetupTest() {
	s.ctrl = gomock.NewController(s.T())
	s.receiver = handlermocks.NewMockReceiver(s.ctrl)
	s.interceptor = handler.WithErrorInterceptor(s.receiver)
}

func (s *ErrorInterceptorTestSuite) TearDownTest() {
	s.ctrl.Finish()
}

func (s *ErrorInterceptorTestSuite) TestCall() {
	require := testutil.Require(s.T())
	s.receiver.EXPECT().
		Call(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(json.RawMessage{}, nil)
	res, err := s.interceptor.Call(context.Background(), nil, nil)
	require.NoError(err)
	require.NotNil(res)
}

func (s *ErrorInterceptorTestSuite) TestBlockNumber() {
	require := testutil.Require(s.T())
	s.receiver.EXPECT().
		BlockNumber(gomock.Any()).
		Return(json.RawMessage{}, nil)
	res, err := s.interceptor.BlockNumber(context.Background())
	require.NoError(err)
	require.NotNil(res)
}

func (s *ErrorInterceptorTestSuite) TestGetBlockByHash() {
	require := testutil.Require(s.T())
	s.receiver.EXPECT().
		GetBlockByHash(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(json.RawMessage{}, nil)
	res, err := s.interceptor.GetBlockByHash(context.Background(), common.Hash{}, false)
	require.NoError(err)
	require.NotNil(res)
}

func (s *ErrorInterceptorTestSuite) TestGetBlockByNumber() {
	require := testutil.Require(s.T())
	s.receiver.EXPECT().
		GetBlockByNumber(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(json.RawMessage{}, nil)
	res, err := s.interceptor.GetBlockByNumber(context.Background(), 0, false)
	require.NoError(err)
	require.NotNil(res)
}

func (s *ErrorInterceptorTestSuite) TestGetBlockTransactionCountByHash() {
	require := testutil.Require(s.T())
	s.receiver.EXPECT().
		GetBlockTransactionCountByHash(gomock.Any(), gomock.Any()).
		Return(json.RawMessage{}, nil)
	res, err := s.interceptor.GetBlockTransactionCountByHash(context.Background(), common.Hash{})
	require.NoError(err)
	require.NotNil(res)
}

func (s *ErrorInterceptorTestSuite) TestGetBlockTransactionCountByNumber() {
	require := testutil.Require(s.T())
	s.receiver.EXPECT().
		GetBlockTransactionCountByNumber(gomock.Any(), gomock.Any()).
		Return(json.RawMessage{}, nil)
	res, err := s.interceptor.GetBlockTransactionCountByNumber(context.Background(), 0)
	require.NoError(err)
	require.NotNil(res)
}

func (s *ErrorInterceptorTestSuite) TestGetUncleCountByBlockHash() {
	require := testutil.Require(s.T())
	s.receiver.EXPECT().
		GetUncleCountByBlockHash(gomock.Any(), gomock.Any()).
		Return(json.RawMessage{}, nil)
	res, err := s.interceptor.GetUncleCountByBlockHash(context.Background(), common.Hash{})
	require.NoError(err)
	require.NotNil(res)
}

func (s *ErrorInterceptorTestSuite) TestGetUncleCountByBlockNumber() {
	require := testutil.Require(s.T())
	s.receiver.EXPECT().
		GetUncleCountByBlockNumber(gomock.Any(), gomock.Any()).
		Return(json.RawMessage{}, nil)
	res, err := s.interceptor.GetUncleCountByBlockNumber(context.Background(), 0)
	require.NoError(err)
	require.NotNil(res)
}

func (s *ErrorInterceptorTestSuite) TestGetBalance() {
	require := testutil.Require(s.T())
	s.receiver.EXPECT().
		GetBalance(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(json.RawMessage{}, nil)
	res, err := s.interceptor.GetBalance(context.Background(), nil, nil)
	require.NoError(err)
	require.NotNil(res)
}

func (s *ErrorInterceptorTestSuite) TestGetCode() {
	require := testutil.Require(s.T())
	s.receiver.EXPECT().
		GetCode(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(json.RawMessage{}, nil)
	res, err := s.interceptor.GetCode(context.Background(), nil, nil)
	require.NoError(err)
	require.NotNil(res)
}

func (s *ErrorInterceptorTestSuite) TestProtocolVersion() {
	require := testutil.Require(s.T())
	s.receiver.EXPECT().
		ProtocolVersion(gomock.Any()).
		Return(json.RawMessage{}, nil)
	res, err := s.interceptor.ProtocolVersion(context.Background())
	require.NoError(err)
	require.NotNil(res)
}

func (s *ErrorInterceptorTestSuite) TestGetLogs() {
	require := testutil.Require(s.T())
	s.receiver.EXPECT().
		GetLogs(gomock.Any(), gomock.Any()).
		Return(json.RawMessage{}, nil)
	res, err := s.interceptor.GetLogs(context.Background(), handler.FilterCriteria{})
	require.NoError(err)
	require.NotNil(res)
}

func (s *ErrorInterceptorTestSuite) TestGetTransactionCount() {
	require := testutil.Require(s.T())
	s.receiver.EXPECT().
		GetTransactionCount(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(json.RawMessage{}, nil)
	res, err := s.interceptor.GetTransactionCount(context.Background(), nil, nil)
	require.NoError(err)
	require.NotNil(res)
}

func (s *ErrorInterceptorTestSuite) TestGetTransactionByHash() {
	require := testutil.Require(s.T())
	s.receiver.EXPECT().
		GetTransactionByHash(gomock.Any(), gomock.Any()).
		Return(json.RawMessage{}, nil)
	res, err := s.interceptor.GetTransactionByHash(context.Background(), common.Hash{})
	require.NoError(err)
	require.NotNil(res)
}

func (s *ErrorInterceptorTestSuite) TestGetTransactionReceipt() {
	require := testutil.Require(s.T())
	s.receiver.EXPECT().
		GetTransactionReceipt(gomock.Any(), gomock.Any()).
		Return(json.RawMessage{}, nil)
	res, err := s.interceptor.GetTransactionReceipt(context.Background(), common.Hash{})
	require.NoError(err)
	require.NotNil(res)
}

func (s *ErrorInterceptorTestSuite) TestGetTransactionByBlockHashAndIndex() {
	require := testutil.Require(s.T())
	s.receiver.EXPECT().
		GetTransactionByBlockHashAndIndex(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(json.RawMessage{}, nil)
	res, err := s.interceptor.GetTransactionByBlockHashAndIndex(context.Background(), common.Hash{}, rpc.DecimalOrHex(0))
	require.NoError(err)
	require.NotNil(res)
}

func (s *ErrorInterceptorTestSuite) TestGetTransactionByBlockNumberAndIndex() {
	require := testutil.Require(s.T())
	s.receiver.EXPECT().
		GetTransactionByBlockNumberAndIndex(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(json.RawMessage{}, nil)
	res, err := s.interceptor.GetTransactionByBlockNumberAndIndex(context.Background(), 0, rpc.DecimalOrHex(0))
	require.NoError(err)
	require.NotNil(res)
}

func (s *ErrorInterceptorTestSuite) TestTraceBlockByHash() {
	require := testutil.Require(s.T())
	s.receiver.EXPECT().
		TraceBlockByHash(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(json.RawMessage{}, nil)
	res, err := s.interceptor.TraceBlockByHash(context.Background(), common.Hash{}, &tracers.TraceConfig{})
	require.NoError(err)
	require.NotNil(res)
}

func (s *ErrorInterceptorTestSuite) TestTraceBlockByNumber() {
	require := testutil.Require(s.T())
	s.receiver.EXPECT().
		TraceBlockByNumber(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(json.RawMessage{}, nil)
	res, err := s.interceptor.TraceBlockByNumber(context.Background(), 0, &tracers.TraceConfig{})
	require.NoError(err)
	require.NotNil(res)
}

func (s *ErrorInterceptorTestSuite) TestArbtraceBlock() {
	require := testutil.Require(s.T())
	s.receiver.EXPECT().
		Block(gomock.Any(), gomock.Any()).
		Return(json.RawMessage{}, nil)
	res, err := s.interceptor.Block(context.Background(), 0)
	require.NoError(err)
	require.NotNil(res)
}

func (s *ErrorInterceptorTestSuite) TestSyncing() {
	require := testutil.Require(s.T())
	s.receiver.EXPECT().
		Syncing(gomock.Any()).
		Return(json.RawMessage{}, nil)
	res, err := s.interceptor.Syncing(context.Background())
	require.NoError(err)
	require.NotNil(res)
}

func (s *ErrorInterceptorTestSuite) TestFeeHistory() {
	require := testutil.Require(s.T())
	s.receiver.EXPECT().
		FeeHistory(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(json.RawMessage{}, nil)
	res, err := s.interceptor.FeeHistory(context.Background(), nil, nil, nil)
	require.NoError(err)
	require.NotNil(res)
}

func (s *ErrorInterceptorTestSuite) TestListening() {
	require := testutil.Require(s.T())
	s.receiver.EXPECT().
		Listening(gomock.Any()).
		Return(json.RawMessage{}, nil)
	res, err := s.interceptor.Listening(context.Background())
	require.NoError(err)
	require.NotNil(res)
}

func (s *ErrorInterceptorTestSuite) TestPeerCount() {
	require := testutil.Require(s.T())
	s.receiver.EXPECT().
		PeerCount(gomock.Any()).
		Return(json.RawMessage{}, nil)
	res, err := s.interceptor.PeerCount(context.Background())
	require.NoError(err)
	require.NotNil(res)
}

func (s *ErrorInterceptorTestSuite) TestGetAuthor() {
	require := testutil.Require(s.T())
	s.receiver.EXPECT().
		GetAuthor(gomock.Any(), gomock.Any()).
		Return(json.RawMessage{}, nil)
	res, err := s.interceptor.GetAuthor(context.Background(), 0)
	require.NoError(err)
	require.NotNil(res)
}

func (s *ErrorInterceptorTestSuite) TestMapError_Generic() {
	require := testutil.Require(s.T())
	s.receiver.EXPECT().
		Call(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, jsonrpc.NewRPCError(errorCodeGeneric, ""))
	_, err := s.interceptor.Call(context.Background(), nil, nil)
	require.Error(err)
	var rpcErr *jsonrpc.RPCError
	require.True(xerrors.As(err, &rpcErr))
	require.Equal(errorCodeGeneric, rpcErr.ErrorCode())
}

func (s *ErrorInterceptorTestSuite) TestMapError_Canceled() {
	require := testutil.Require(s.T())
	s.receiver.EXPECT().
		Call(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, xerrors.Errorf("mock error: %w", context.Canceled))
	_, err := s.interceptor.Call(context.Background(), nil, nil)
	require.Error(err)
	var rpcErr *jsonrpc.RPCError
	require.True(xerrors.As(err, &rpcErr))
	require.Equal(errorCodeCanceled, rpcErr.ErrorCode())
}

func (s *ErrorInterceptorTestSuite) TestMapError_RequestCanceled() {
	require := testutil.Require(s.T())
	s.receiver.EXPECT().
		Call(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, xerrors.Errorf("mock error: %w", storage.ErrRequestCanceled))
	_, err := s.interceptor.Call(context.Background(), nil, nil)
	require.Error(err)
	var rpcErr *jsonrpc.RPCError
	require.True(xerrors.As(err, &rpcErr))
	require.Equal(errorCodeCanceled, rpcErr.ErrorCode())
}

func (s *ErrorInterceptorTestSuite) TestMapError_Internal() {
	require := testutil.Require(s.T())
	s.receiver.EXPECT().
		Call(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, xerrors.Errorf("mock error: %w", storage.ErrItemNotFound))
	_, err := s.interceptor.Call(context.Background(), nil, nil)
	require.Error(err)
	var rpcErr *jsonrpc.RPCError
	require.True(xerrors.As(err, &rpcErr))
	require.Equal(errorCodeInternal, rpcErr.ErrorCode())
}

func (s *ErrorInterceptorTestSuite) TestMapError_NotImplemented() {
	require := testutil.Require(s.T())
	s.receiver.EXPECT().
		Call(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, xerrors.Errorf("mock error: %w", api.ErrNotImplemented))
	_, err := s.interceptor.Call(context.Background(), nil, nil)
	require.Error(err)
	var rpcErr *jsonrpc.RPCError
	require.True(xerrors.As(err, &rpcErr))
	require.Equal(errorCodeBadRequest, rpcErr.ErrorCode())
}

func (s *ErrorInterceptorTestSuite) TestMapError_NotAllowed() {
	require := testutil.Require(s.T())
	s.receiver.EXPECT().
		Call(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, xerrors.Errorf("mock error: %w", api.ErrNotAllowed))
	_, err := s.interceptor.Call(context.Background(), nil, nil)
	require.Error(err)
	var rpcErr *jsonrpc.RPCError
	require.True(xerrors.As(err, &rpcErr))
	require.Equal(errorCodeBadRequest, rpcErr.ErrorCode())
}
