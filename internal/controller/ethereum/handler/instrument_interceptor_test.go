package handler_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/eth/tracers"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally/v4"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainnode/internal/clients/blockchain/jsonrpc"
	"github.com/coinbase/chainnode/internal/controller/ethereum/handler"
	handlermocks "github.com/coinbase/chainnode/internal/controller/ethereum/handler/mocks"
	"github.com/coinbase/chainnode/internal/server/rpc"
	"github.com/coinbase/chainnode/internal/utils/testutil"
)

type InstrumentInterceptorTestSuite struct {
	suite.Suite
	ctrl        *gomock.Controller
	scope       tally.TestScope
	receiver    *handlermocks.MockReceiver
	interceptor handler.Receiver
}

func TestInstrumentInterceptorTestSuite(t *testing.T) {
	suite.Run(t, new(InstrumentInterceptorTestSuite))
}

func (s *InstrumentInterceptorTestSuite) SetupTest() {
	s.ctrl = gomock.NewController(s.T())
	s.scope = tally.NewTestScope("chainnode", nil)
	s.receiver = handlermocks.NewMockReceiver(s.ctrl)
	s.interceptor = handler.WithInstrumentInterceptor(s.receiver, s.scope, zap.NewNop())
}

func (s *InstrumentInterceptorTestSuite) TearDownTest() {
	s.ctrl.Finish()
}

func (s *InstrumentInterceptorTestSuite) TestCall() {
	require := testutil.Require(s.T())
	s.receiver.EXPECT().
		Call(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(json.RawMessage{}, nil)
	res, err := s.interceptor.Call(context.Background(), nil, nil)
	require.NoError(err)
	require.NotNil(res)

	counter := s.getCounter("eth_call", true, handler.LatencyLevelDefault, true)
	require.NotNil(counter)
	require.Equal(int64(1), counter.Value())
}

func (s *InstrumentInterceptorTestSuite) TestCall_Error() {
	require := testutil.Require(s.T())
	s.receiver.EXPECT().
		Call(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, xerrors.Errorf("mock error: %w", jsonrpc.NewRPCError(errorCodeInternal, "")))
	res, err := s.interceptor.Call(context.Background(), nil, nil)
	require.Error(err)
	require.Nil(res)

	counter := s.getCounter("eth_call", true, handler.LatencyLevelDefault, false)
	require.NotNil(counter)
	require.Equal(int64(1), counter.Value())
}

func (s *InstrumentInterceptorTestSuite) TestCall_FilterError() {
	require := testutil.Require(s.T())
	s.receiver.EXPECT().
		Call(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, xerrors.Errorf("mock error: %w", jsonrpc.NewRPCError(errorCodeGeneric, "")))
	res, err := s.interceptor.Call(context.Background(), nil, nil)
	require.Error(err)
	require.Nil(res)

	counter := s.getCounter("eth_call", true, handler.LatencyLevelDefault, true)
	require.NotNil(counter)
	require.Equal(int64(1), counter.Value())
}

func (s *InstrumentInterceptorTestSuite) TestBlockNumber() {
	require := testutil.Require(s.T())
	s.receiver.EXPECT().
		BlockNumber(gomock.Any()).
		Return(json.RawMessage{}, nil)
	res, err := s.interceptor.BlockNumber(context.Background())
	require.NoError(err)
	require.NotNil(res)

	counter := s.getCounter("eth_blockNumber", false, handler.LatencyLevelDefault, true)
	require.NotNil(counter)
	require.Equal(int64(1), counter.Value())
}

func (s *InstrumentInterceptorTestSuite) TestBlockNumber_Error() {
	require := testutil.Require(s.T())
	s.receiver.EXPECT().
		BlockNumber(gomock.Any()).
		Return(nil, xerrors.Errorf("mock error: %w", jsonrpc.NewRPCError(errorCodeGeneric, "")))
	res, err := s.interceptor.BlockNumber(context.Background())
	require.Error(err)
	require.Nil(res)

	counter := s.getCounter("eth_blockNumber", false, handler.LatencyLevelDefault, false)
	require.NotNil(counter)
	require.Equal(int64(1), counter.Value())
}

func (s *InstrumentInterceptorTestSuite) TestBlockNumber_FilterError() {
	require := testutil.Require(s.T())
	s.receiver.EXPECT().
		BlockNumber(gomock.Any()).
		Return(nil, xerrors.Errorf("mock error: %w", jsonrpc.NewRPCError(errorCodeCanceled, "")))
	res, err := s.interceptor.BlockNumber(context.Background())
	require.Error(err)
	require.Nil(res)

	counter := s.getCounter("eth_blockNumber", false, handler.LatencyLevelDefault, true)
	require.NotNil(counter)
	require.Equal(int64(1), counter.Value())
}

func (s *InstrumentInterceptorTestSuite) TestGetBlockByNumber() {
	require := testutil.Require(s.T())
	s.receiver.EXPECT().
		GetBlockByNumber(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(json.RawMessage{}, nil)
	res, err := s.interceptor.GetBlockByNumber(context.Background(), 0, false)
	require.NoError(err)
	require.NotNil(res)

	counter := s.getCounter("eth_getBlockByNumber", false, handler.LatencyLevelDefault, true)
	require.NotNil(counter)
	require.Equal(int64(1), counter.Value())
}

func (s *InstrumentInterceptorTestSuite) TestGetBlockByNumber_Pending() {
	require := testutil.Require(s.T())
	s.receiver.EXPECT().
		GetBlockByNumber(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(json.RawMessage{}, nil)
	res, err := s.interceptor.GetBlockByNumber(context.Background(), rpc.PendingBlockNumber, false)
	require.NoError(err)
	require.NotNil(res)

	counter := s.getCounter("eth_getBlockByNumber", true, handler.LatencyLevelDefault, true)
	require.NotNil(counter)
	require.Equal(int64(1), counter.Value())
}

func (s *InstrumentInterceptorTestSuite) TestGetBlockByHash() {
	require := testutil.Require(s.T())
	s.receiver.EXPECT().
		GetBlockByHash(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(json.RawMessage{}, nil)
	res, err := s.interceptor.GetBlockByHash(context.Background(), common.Hash{}, false)
	require.NoError(err)
	require.NotNil(res)

	counter := s.getCounter("eth_getBlockByHash", false, handler.LatencyLevelDefault, true)
	require.NotNil(counter)
	require.Equal(int64(1), counter.Value())
}

func (s *InstrumentInterceptorTestSuite) TestGetBlockTransactionCountByHash() {
	require := testutil.Require(s.T())
	s.receiver.EXPECT().
		GetBlockTransactionCountByHash(gomock.Any(), gomock.Any()).
		Return(json.RawMessage{}, nil)
	res, err := s.interceptor.GetBlockTransactionCountByHash(context.Background(), common.Hash{})
	require.NoError(err)
	require.NotNil(res)

	counter := s.getCounter("eth_getBlockTransactionCountByHash", false, handler.LatencyLevelDefault, true)
	require.NotNil(counter)
	require.Equal(int64(1), counter.Value())
}

func (s *InstrumentInterceptorTestSuite) TestGetBlockTransactionCountByNumber() {
	require := testutil.Require(s.T())
	s.receiver.EXPECT().
		GetBlockTransactionCountByNumber(gomock.Any(), gomock.Any()).
		Return(json.RawMessage{}, nil)
	res, err := s.interceptor.GetBlockTransactionCountByNumber(context.Background(), 0)
	require.NoError(err)
	require.NotNil(res)

	counter := s.getCounter("eth_getBlockTransactionCountByNumber", false, handler.LatencyLevelDefault, true)
	require.NotNil(counter)
	require.Equal(int64(1), counter.Value())
}

func (s *InstrumentInterceptorTestSuite) TestGetUncleCountByBlockHash() {
	require := testutil.Require(s.T())
	s.receiver.EXPECT().
		GetUncleCountByBlockHash(gomock.Any(), gomock.Any()).
		Return(json.RawMessage{}, nil)
	res, err := s.interceptor.GetUncleCountByBlockHash(context.Background(), common.Hash{})
	require.NoError(err)
	require.NotNil(res)

	counter := s.getCounter("eth_getUncleCountByBlockHash", false, handler.LatencyLevelDefault, true)
	require.NotNil(counter)
	require.Equal(int64(1), counter.Value())
}

func (s *InstrumentInterceptorTestSuite) TestGetUncleCountByBlockNumber() {
	require := testutil.Require(s.T())
	s.receiver.EXPECT().
		GetUncleCountByBlockNumber(gomock.Any(), gomock.Any()).
		Return(json.RawMessage{}, nil)
	res, err := s.interceptor.GetUncleCountByBlockNumber(context.Background(), 0)
	require.NoError(err)
	require.NotNil(res)

	counter := s.getCounter("eth_getUncleCountByBlockNumber", false, handler.LatencyLevelDefault, true)
	require.NotNil(counter)
	require.Equal(int64(1), counter.Value())
}

func (s *InstrumentInterceptorTestSuite) TestGetBalance() {
	require := testutil.Require(s.T())
	s.receiver.EXPECT().
		GetBalance(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(json.RawMessage{}, nil)
	res, err := s.interceptor.GetBalance(context.Background(), nil, nil)
	require.NoError(err)
	require.NotNil(res)

	counter := s.getCounter("eth_getBalance", true, handler.LatencyLevelDefault, true)
	require.NotNil(counter)
	require.Equal(int64(1), counter.Value())
}

func (s *InstrumentInterceptorTestSuite) TestGetBalance_Error() {
	require := testutil.Require(s.T())
	s.receiver.EXPECT().
		GetBalance(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, xerrors.Errorf("mock error: %w", jsonrpc.NewRPCError(errorCodeInternal, "")))
	res, err := s.interceptor.GetBalance(context.Background(), nil, nil)
	require.Error(err)
	require.Nil(res)

	counter := s.getCounter("eth_getBalance", true, handler.LatencyLevelDefault, false)
	require.NotNil(counter)
	require.Equal(int64(1), counter.Value())
}

func (s *InstrumentInterceptorTestSuite) TestGetCode() {
	require := testutil.Require(s.T())
	s.receiver.EXPECT().
		GetCode(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(json.RawMessage{}, nil)
	res, err := s.interceptor.GetCode(context.Background(), nil, nil)
	require.NoError(err)
	require.NotNil(res)

	counter := s.getCounter("eth_getCode", true, handler.LatencyLevelDefault, true)
	require.NotNil(counter)
	require.Equal(int64(1), counter.Value())
}

func (s *InstrumentInterceptorTestSuite) TestGetCode_Error() {
	require := testutil.Require(s.T())
	s.receiver.EXPECT().
		GetCode(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, xerrors.Errorf("mock error: %w", jsonrpc.NewRPCError(errorCodeInternal, "")))
	res, err := s.interceptor.GetCode(context.Background(), nil, nil)
	require.Error(err)
	require.Nil(res)

	counter := s.getCounter("eth_getCode", true, handler.LatencyLevelDefault, false)
	require.NotNil(counter)
	require.Equal(int64(1), counter.Value())
}

func (s *InstrumentInterceptorTestSuite) TestGetLogs() {
	require := testutil.Require(s.T())
	s.receiver.EXPECT().
		GetLogs(gomock.Any(), gomock.Any()).
		Return(json.RawMessage{}, nil)
	res, err := s.interceptor.GetLogs(context.Background(), handler.FilterCriteria{})
	require.NoError(err)
	require.NotNil(res)

	counter := s.getCounter("eth_getLogs", false, handler.LatencyLevelHigh, true)
	require.NotNil(counter)
	require.Equal(int64(1), counter.Value())
}

func (s *InstrumentInterceptorTestSuite) TestGetTransactionCount() {
	require := testutil.Require(s.T())
	s.receiver.EXPECT().
		GetTransactionCount(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(json.RawMessage{}, nil)
	res, err := s.interceptor.GetTransactionCount(context.Background(), nil, nil)
	require.NoError(err)
	require.NotNil(res)

	counter := s.getCounter("eth_getTransactionCount", true, handler.LatencyLevelDefault, true)
	require.NotNil(counter)
	require.Equal(int64(1), counter.Value())
}

func (s *InstrumentInterceptorTestSuite) TestGetTransactionCount_Error() {
	require := testutil.Require(s.T())
	s.receiver.EXPECT().
		GetTransactionCount(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, xerrors.Errorf("mock error: %w", jsonrpc.NewRPCError(errorCodeInternal, "")))
	res, err := s.interceptor.GetTransactionCount(context.Background(), nil, nil)
	require.Error(err)
	require.Nil(res)

	counter := s.getCounter("eth_getTransactionCount", true, handler.LatencyLevelDefault, false)
	require.NotNil(counter)
	require.Equal(int64(1), counter.Value())
}

func (s *InstrumentInterceptorTestSuite) TestGetTransactionByHash() {
	require := testutil.Require(s.T())
	s.receiver.EXPECT().
		GetTransactionByHash(gomock.Any(), gomock.Any()).
		Return(json.RawMessage{}, nil)
	res, err := s.interceptor.GetTransactionByHash(context.Background(), common.Hash{})
	require.NoError(err)
	require.NotNil(res)

	counter := s.getCounter("eth_getTransactionByHash", false, handler.LatencyLevelDefault, true)
	require.NotNil(counter)
	require.Equal(int64(1), counter.Value())
}

func (s *InstrumentInterceptorTestSuite) TestGetTransactionReceipt() {
	require := testutil.Require(s.T())
	s.receiver.EXPECT().
		GetTransactionReceipt(gomock.Any(), gomock.Any()).
		Return(json.RawMessage{}, nil)
	res, err := s.interceptor.GetTransactionReceipt(context.Background(), common.Hash{})
	require.NoError(err)
	require.NotNil(res)

	counter := s.getCounter("eth_getTransactionReceipt", false, handler.LatencyLevelDefault, true)
	require.NotNil(counter)
	require.Equal(int64(1), counter.Value())
}

func (s *InstrumentInterceptorTestSuite) TestGetTransactionByBlockHashAndIndex() {
	require := testutil.Require(s.T())
	s.receiver.EXPECT().
		GetTransactionByBlockHashAndIndex(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(json.RawMessage{}, nil)
	res, err := s.interceptor.GetTransactionByBlockHashAndIndex(context.Background(), common.Hash{}, rpc.DecimalOrHex(0))
	require.NoError(err)
	require.NotNil(res)

	counter := s.getCounter("eth_getTransactionByBlockHashAndIndex", false, handler.LatencyLevelDefault, true)
	require.NotNil(counter)
	require.Equal(int64(1), counter.Value())
}

func (s *InstrumentInterceptorTestSuite) TestGetTransactionByBlockNumberAndIndex() {
	require := testutil.Require(s.T())
	s.receiver.EXPECT().
		GetTransactionByBlockNumberAndIndex(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(json.RawMessage{}, nil)
	res, err := s.interceptor.GetTransactionByBlockNumberAndIndex(context.Background(), 0, rpc.DecimalOrHex(0))
	require.NoError(err)
	require.NotNil(res)

	counter := s.getCounter("eth_getTransactionByBlockNumberAndIndex", false, handler.LatencyLevelDefault, true)
	require.NotNil(counter)
	require.Equal(int64(1), counter.Value())
}

func (s *InstrumentInterceptorTestSuite) TestTraceBlockByHash() {
	require := testutil.Require(s.T())
	s.receiver.EXPECT().
		TraceBlockByHash(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(json.RawMessage{}, nil)
	res, err := s.interceptor.TraceBlockByHash(context.Background(), common.Hash{}, &tracers.TraceConfig{})
	require.NoError(err)
	require.NotNil(res)

	counter := s.getCounter("debug_traceBlockByHash", false, handler.LatencyLevelDefault, true)
	require.NotNil(counter)
	require.Equal(int64(1), counter.Value())
}

func (s *InstrumentInterceptorTestSuite) TestTraceBlockByNumber() {
	require := testutil.Require(s.T())
	s.receiver.EXPECT().
		TraceBlockByNumber(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(json.RawMessage{}, nil)
	res, err := s.interceptor.TraceBlockByNumber(context.Background(), 0, &tracers.TraceConfig{})
	require.NoError(err)
	require.NotNil(res)

	counter := s.getCounter("debug_traceBlockByNumber", false, handler.LatencyLevelDefault, true)
	require.NotNil(counter)
	require.Equal(int64(1), counter.Value())
}

func (s *InstrumentInterceptorTestSuite) TestArbtraceBlock() {
	require := testutil.Require(s.T())
	s.receiver.EXPECT().
		Block(gomock.Any(), gomock.Any()).
		Return(json.RawMessage{}, nil)
	res, err := s.interceptor.Block(context.Background(), 0)
	require.NoError(err)
	require.NotNil(res)

	counter := s.getCounter("arbtrace_block", false, handler.LatencyLevelDefault, true)
	require.NotNil(counter)
	require.Equal(int64(1), counter.Value())
}

func (s *InstrumentInterceptorTestSuite) TestChainId() {
	require := testutil.Require(s.T())
	s.receiver.EXPECT().
		ChainId(gomock.Any()).
		Return(json.RawMessage{}, nil)
	res, err := s.interceptor.ChainId(context.Background())
	require.NoError(err)
	require.Equal(json.RawMessage{}, res)

	counter := s.getCounter("eth_chainId", false, handler.LatencyLevelDefault, true)
	require.NotNil(counter)
	require.Equal(int64(1), counter.Value())
}

func (s *InstrumentInterceptorTestSuite) TestVersion() {
	require := testutil.Require(s.T())
	s.receiver.EXPECT().
		Version(gomock.Any()).
		Return(json.RawMessage{}, nil)
	res, err := s.interceptor.Version(context.Background())
	require.NoError(err)
	require.Equal(json.RawMessage{}, res)

	counter := s.getCounter("net_version", false, handler.LatencyLevelDefault, true)
	require.NotNil(counter)
	require.Equal(int64(1), counter.Value())
}

func (s *InstrumentInterceptorTestSuite) TestProtocolVersion() {
	require := testutil.Require(s.T())
	s.receiver.EXPECT().
		ProtocolVersion(gomock.Any()).
		Return(json.RawMessage{}, nil)
	res, err := s.interceptor.ProtocolVersion(context.Background())
	require.NoError(err)
	require.Equal(json.RawMessage{}, res)

	counter := s.getCounter("eth_protocolVersion", true, handler.LatencyLevelDefault, true)
	require.NotNil(counter)
	require.Equal(int64(1), counter.Value())
}

func (s *InstrumentInterceptorTestSuite) TestSyncing() {
	require := testutil.Require(s.T())
	s.receiver.EXPECT().
		Syncing(gomock.Any()).
		Return(json.RawMessage{}, nil)
	res, err := s.interceptor.Syncing(context.Background())
	require.NoError(err)
	require.Equal(json.RawMessage{}, res)

	counter := s.getCounter("eth_syncing", false, handler.LatencyLevelDefault, true)
	require.NotNil(counter)
	require.Equal(int64(1), counter.Value())
}

func (s *InstrumentInterceptorTestSuite) TestFeeHistory() {
	require := testutil.Require(s.T())
	s.receiver.EXPECT().
		FeeHistory(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(json.RawMessage{}, nil)
	res, err := s.interceptor.FeeHistory(context.Background(), nil, nil, nil)
	require.NoError(err)
	require.Equal(json.RawMessage{}, res)

	counter := s.getCounter("eth_feeHistory", true, handler.LatencyLevelDefault, true)
	require.NotNil(counter)
	require.Equal(int64(1), counter.Value())
}

func (s *InstrumentInterceptorTestSuite) TestListening() {
	require := testutil.Require(s.T())
	s.receiver.EXPECT().
		Listening(gomock.Any()).
		Return(json.RawMessage{}, nil)
	res, err := s.interceptor.Listening(context.Background())
	require.NoError(err)
	require.Equal(json.RawMessage{}, res)

	counter := s.getCounter("net_listening", false, handler.LatencyLevelDefault, true)
	require.NotNil(counter)
	require.Equal(int64(1), counter.Value())
}

func (s *InstrumentInterceptorTestSuite) TestPeerCount() {
	require := testutil.Require(s.T())
	s.receiver.EXPECT().
		PeerCount(gomock.Any()).
		Return(json.RawMessage{}, nil)
	res, err := s.interceptor.PeerCount(context.Background())
	require.NoError(err)
	require.Equal(json.RawMessage{}, res)

	counter := s.getCounter("net_peerCount", true, handler.LatencyLevelDefault, true)
	require.NotNil(counter)
	require.Equal(int64(1), counter.Value())
}

func (s *InstrumentInterceptorTestSuite) TestGetAuthor() {
	require := testutil.Require(s.T())
	s.receiver.EXPECT().
		GetAuthor(gomock.Any(), gomock.Any()).
		Return(json.RawMessage{}, nil)
	res, err := s.interceptor.GetAuthor(context.Background(), 0)
	require.NoError(err)
	require.Equal(json.RawMessage{}, res)

	counter := s.getCounter("bor_getAuthor", false, handler.LatencyLevelDefault, true)
	require.NotNil(counter)
	require.Equal(int64(1), counter.Value())
}

func (s *InstrumentInterceptorTestSuite) getCounter(method string, proxy bool, latencyLevel string, success bool) tally.CounterSnapshot {
	resultType := "success"
	if !success {
		resultType = "error"
	}
	key := fmt.Sprintf("chainnode.handler.request+latency_level=%s,method=%s,proxy=%v,result_type=%v", latencyLevel, method, proxy, resultType)
	snapshot := s.scope.Snapshot()
	return snapshot.Counters()[key]
}
