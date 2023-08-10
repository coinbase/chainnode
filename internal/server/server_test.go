package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainnode/internal/clients/blockchain/jsonrpc"
	"github.com/coinbase/chainnode/internal/config"
	"github.com/coinbase/chainnode/internal/controller"
	"github.com/coinbase/chainnode/internal/controller/ethereum/handler"
	handlermocks "github.com/coinbase/chainnode/internal/controller/ethereum/handler/mocks"
	controllermocks "github.com/coinbase/chainnode/internal/controller/mocks"
	"github.com/coinbase/chainnode/internal/server/rpc"
	"github.com/coinbase/chainnode/internal/utils/fixtures"
	"github.com/coinbase/chainnode/internal/utils/testapp"
	"github.com/coinbase/chainnode/internal/utils/testutil"

	c3common "github.com/coinbase/chainstorage/protos/coinbase/c3/common"
)

type (
	ServerTestSuite struct {
		suite.Suite
		ctrl       *gomock.Controller
		app        testapp.TestApp
		config     ServerTestConfig
		controller *controllermocks.MockController
		handler    *controllermocks.MockHandler
		receiver   *handlermocks.MockReceiver
		namespaces map[string]interface{}
		server     *Server
		testServer *httptest.Server
	}
	ServerTestConfig struct {
		Blockchain c3common.Blockchain
		Network    c3common.Network
	}
)

const (
	handlerPath = "/v1"
)

func TestServerTestSuite_EthereumMainnet(t *testing.T) {
	suite.Run(t, &ServerTestSuite{
		config: ServerTestConfig{
			Blockchain: c3common.Blockchain_BLOCKCHAIN_ETHEREUM,
			Network:    c3common.Network_NETWORK_ETHEREUM_MAINNET,
		},
	})
}

func (s *ServerTestSuite) SetupTest() {
	cfg, err := config.New(
		config.WithBlockchain(s.config.Blockchain),
		config.WithNetwork(s.config.Network),
	)
	s.Require().NoError(err)

	s.ctrl = gomock.NewController(s.T())
	s.controller = controllermocks.NewMockController(s.ctrl)
	s.handler = controllermocks.NewMockHandler(s.ctrl)
	s.receiver = handlermocks.NewMockReceiver(s.ctrl)
	s.controller.EXPECT().Handler().Return(s.handler)
	s.controller.EXPECT().ReverseProxies().Return(nil)
	s.handler.EXPECT().Path().Return(handlerPath).AnyTimes()
	s.namespaces = map[string]interface{}{
		"eth":      handler.NewEthNamespace(s.receiver),
		"debug":    handler.NewDebugNamespace(s.receiver),
		"net":      handler.NewNetNamespace(s.receiver),
		"arbtrace": handler.NewArbtraceNamespace(s.receiver),
		"bor":      handler.NewBorNamespace(s.receiver),
	}
	s.handler.EXPECT().Namespaces().Return(s.namespaces)
	s.handler.EXPECT().
		PrepareContext(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, request json.RawMessage) (context.Context, error) {
			return ctx, nil
		}).
		AnyTimes()
	s.app = testapp.New(
		s.T(),
		testapp.WithConfig(cfg),
		Module,
		fx.Provide(func() controller.Controller { return s.controller }),
		fx.Populate(&s.server),
	)
	s.testServer = httptest.NewServer(s.server)
}

func (s *ServerTestSuite) TearDownTest() {
	s.testServer.Close()
	s.app.Close()
}

func (s *ServerTestSuite) TestCall() {
	require := testutil.Require(s.T())

	expected := "0x00000000000000000000000000000000000000000000058153137e54f048f08b"
	s.receiver.EXPECT().
		Call(gomock.Any(), gomock.Any(), "latest").
		Return(json.Marshal(expected))

	request := fixtures.MustReadFile("server/eth_call.json")
	response, err := post(s.testServer, request)
	require.NoError(err)
	defer response.Body.Close()
	require.Equal(http.StatusOK, response.StatusCode)

	var rpcResponse jsonrpc.Response
	err = json.NewDecoder(response.Body).Decode(&rpcResponse)
	require.NoError(err)
	require.Nil(rpcResponse.Error)
	require.Equal("2.0", rpcResponse.JSONRPC)
	require.Equal(uint(1), rpcResponse.ID)

	var actual string
	err = rpcResponse.Unmarshal(&actual)
	require.NoError(err)
	require.Equal(expected, actual)
}

func (s *ServerTestSuite) TestBlockNumber() {
	require := testutil.Require(s.T())

	expected := "0xa7d8d3"
	s.receiver.EXPECT().
		BlockNumber(gomock.Any()).
		Return(json.Marshal(expected))

	request := fixtures.MustReadFile("server/eth_blockNumber.json")
	response, err := post(s.testServer, request)
	require.NoError(err)
	defer response.Body.Close()
	require.Equal(http.StatusOK, response.StatusCode)

	var rpcResponse jsonrpc.Response
	err = json.NewDecoder(response.Body).Decode(&rpcResponse)
	require.NoError(err)
	require.Nil(rpcResponse.Error)
	require.Equal("2.0", rpcResponse.JSONRPC)
	require.Equal(uint(1), rpcResponse.ID)

	var actual string
	err = rpcResponse.Unmarshal(&actual)
	require.NoError(err)
	require.Equal(expected, actual)
}

func (s *ServerTestSuite) TestGetBlockByNumber() {
	require := testutil.Require(s.T())

	expected := fixtures.MustReadJson("controller/ethereum/eth_block_1.json")
	s.receiver.EXPECT().
		GetBlockByNumber(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(expected, nil)

	request := fixtures.MustReadFile("server/eth_getBlockByNumber.json")
	response, err := post(s.testServer, request)
	require.NoError(err)
	defer response.Body.Close()
	require.Equal(http.StatusOK, response.StatusCode)

	var rpcResponse jsonrpc.Response
	err = json.NewDecoder(response.Body).Decode(&rpcResponse)
	require.NoError(err)
	require.Nil(rpcResponse.Error)
	require.Equal("2.0", rpcResponse.JSONRPC)
	require.Equal(uint(1), rpcResponse.ID)
	require.Equal(string(expected), string(rpcResponse.Result))
}

func (s *ServerTestSuite) TestGetBlockByNumberReceiverError() {
	require := testutil.Require(s.T())

	s.receiver.EXPECT().
		GetBlockByNumber(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, xerrors.New("test error"))

	request := fixtures.MustReadFile("server/eth_getBlockByNumber.json")
	response, err := post(s.testServer, request)
	require.NoError(err)
	defer response.Body.Close()
	require.Equal(http.StatusOK, response.StatusCode)

	var rpcResponse jsonrpc.Response
	err = json.NewDecoder(response.Body).Decode(&rpcResponse)
	require.NoError(err)
	require.Nil(rpcResponse.Result)
	require.Error(rpcResponse.Error)
	require.Equal(-32000, rpcResponse.Error.Code)
	require.Equal("test error", rpcResponse.Error.Message)
	require.Equal("2.0", rpcResponse.JSONRPC)
	require.Equal(uint(1), rpcResponse.ID)
}

func (s *ServerTestSuite) TestGetBlockByNumberReceiverRPCError() {
	require := testutil.Require(s.T())

	s.receiver.EXPECT().
		GetBlockByNumber(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, jsonrpc.NewRPCError(-32099, "test rpc error"))

	request := fixtures.MustReadFile("server/eth_getBlockByNumber.json")
	response, err := post(s.testServer, request)
	require.NoError(err)
	defer response.Body.Close()
	require.Equal(http.StatusOK, response.StatusCode)

	var rpcResponse jsonrpc.Response
	err = json.NewDecoder(response.Body).Decode(&rpcResponse)
	require.NoError(err)
	require.Nil(rpcResponse.Result)
	require.Error(rpcResponse.Error)
	require.Equal(-32099, rpcResponse.Error.Code)
	require.Equal("test rpc error", rpcResponse.Error.Message)
	require.Equal("2.0", rpcResponse.JSONRPC)
	require.Equal(uint(1), rpcResponse.ID)
}

func (s *ServerTestSuite) TestGetBlockByNumberReceiverPanic() {
	require := testutil.Require(s.T())

	s.receiver.EXPECT().
		GetBlockByNumber(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context) (json.RawMessage, error) {
			panic("test panic")
		})

	request := fixtures.MustReadFile("server/eth_getBlockByNumber.json")
	response, err := post(s.testServer, request)
	require.NoError(err)
	defer response.Body.Close()
	require.Equal(http.StatusOK, response.StatusCode)

	var rpcResponse jsonrpc.Response
	err = json.NewDecoder(response.Body).Decode(&rpcResponse)
	require.NoError(err)
	require.Nil(rpcResponse.Result)
	require.Error(rpcResponse.Error)
	require.Equal(-32000, rpcResponse.Error.Code)
	require.Equal("method handler crashed", rpcResponse.Error.Message)
	require.Equal("2.0", rpcResponse.JSONRPC)
	require.Equal(uint(1), rpcResponse.ID)
}

func (s *ServerTestSuite) TestGetBlockByNumberInvalidBlockNumber() {
	require := testutil.Require(s.T())

	request := fixtures.MustReadFile("server/eth_getBlockByNumberInvalidBlockNumber.json")
	response, err := post(s.testServer, request)
	require.NoError(err)
	defer response.Body.Close()
	require.Equal(http.StatusOK, response.StatusCode)

	var rpcResponse jsonrpc.Response
	err = json.NewDecoder(response.Body).Decode(&rpcResponse)
	require.NoError(err)
	require.Nil(rpcResponse.Result)
	require.Error(rpcResponse.Error)
	require.Equal("2.0", rpcResponse.JSONRPC)
	require.Equal(uint(1), rpcResponse.ID)
	require.Equal(-32602, rpcResponse.Error.Code)
	require.Contains(rpcResponse.Error.Message, "hex string without 0x prefix")
}

func (s *ServerTestSuite) TestGetLogs() {
	require := testutil.Require(s.T())

	expected := fixtures.MustReadJson("controller/ethereum/eth_logs_1.json")
	s.receiver.EXPECT().
		GetLogs(gomock.Any(), gomock.Any()).
		Return(expected, nil)

	request := fixtures.MustReadFile("server/eth_getLogs.json")
	response, err := post(s.testServer, request)
	require.NoError(err)
	defer response.Body.Close()
	require.Equal(http.StatusOK, response.StatusCode)

	var rpcResponse jsonrpc.Response
	err = json.NewDecoder(response.Body).Decode(&rpcResponse)
	require.NoError(err)
	require.Nil(rpcResponse.Error)
	require.Equal("2.0", rpcResponse.JSONRPC)
	require.Equal(uint(1), rpcResponse.ID)
	require.Equal(string(expected), string(rpcResponse.Result))
}

func (s *ServerTestSuite) TestGetTransactionByHash() {
	require := testutil.Require(s.T())

	expected := fixtures.MustReadJson("controller/ethereum/eth_transaction_1.json")
	s.receiver.EXPECT().
		GetTransactionByHash(gomock.Any(), gomock.Any()).
		Return(expected, nil)

	request := fixtures.MustReadFile("server/eth_getTransactionByHash.json")
	response, err := post(s.testServer, request)
	require.NoError(err)
	defer response.Body.Close()
	require.Equal(http.StatusOK, response.StatusCode)

	var rpcResponse jsonrpc.Response
	err = json.NewDecoder(response.Body).Decode(&rpcResponse)
	require.NoError(err)
	require.Nil(rpcResponse.Error)
	require.Equal("2.0", rpcResponse.JSONRPC)
	require.Equal(uint(1), rpcResponse.ID)
	require.Equal(string(expected), string(rpcResponse.Result))
}

func (s *ServerTestSuite) TestGetTransactionByHashInvalidHash() {
	require := testutil.Require(s.T())

	request := fixtures.MustReadFile("server/eth_getTransactionByHashInvalidHash.json")
	response, err := post(s.testServer, request)
	require.NoError(err)
	defer response.Body.Close()
	require.Equal(http.StatusOK, response.StatusCode)

	var rpcResponses []jsonrpc.Response
	err = json.NewDecoder(response.Body).Decode(&rpcResponses)
	require.NoError(err)
	require.Equal(3, len(rpcResponses))
	for i, rpcResponse := range rpcResponses {
		require.Nil(rpcResponse.Result)
		require.Error(rpcResponse.Error)
		require.Equal("2.0", rpcResponse.JSONRPC)
		require.Equal(uint(1+i), rpcResponse.ID)
	}
	require.Equal(-32602, rpcResponses[0].Error.Code)
	require.Contains(rpcResponses[0].Error.Message, "json: cannot unmarshal hex string of odd length into Go value of type common.Hash")
	require.Equal(-32602, rpcResponses[1].Error.Code)
	require.Contains(rpcResponses[1].Error.Message, "hex string has length 66, want 64 for common.Hash")
	require.Equal(-32602, rpcResponses[2].Error.Code)
	require.Contains(rpcResponses[2].Error.Message, "json: cannot unmarshal hex string without 0x prefix into Go value of type common.Hash")
}

func (s *ServerTestSuite) TestTraceBlockByHash() {
	require := testutil.Require(s.T())

	expected := fixtures.MustReadJson("controller/ethereum/eth_trace.json")
	s.receiver.EXPECT().
		TraceBlockByHash(gomock.Any(), common.HexToHash("0x6f6db02f560821eab1e1a978eadcec4feb016aa647758b2a27dced41eadfc6eb"), gomock.Any()).
		Return(expected, nil)

	request := fixtures.MustReadFile("server/debug_traceBlockByHash.json")
	response, err := post(s.testServer, request)
	require.NoError(err)
	defer response.Body.Close()
	require.Equal(http.StatusOK, response.StatusCode)

	var rpcResponse jsonrpc.Response
	err = json.NewDecoder(response.Body).Decode(&rpcResponse)
	require.NoError(err)
	require.Nil(rpcResponse.Error)
	require.Equal("2.0", rpcResponse.JSONRPC)
	require.Equal(uint(1), rpcResponse.ID)
	require.Equal(string(expected), string(rpcResponse.Result))
}

func (s *ServerTestSuite) TestTraceBlockByHash_InvalidHash() {
	require := testutil.Require(s.T())

	request := fixtures.MustReadFile("server/debug_traceBlockByHash_invalidHash.json")
	response, err := post(s.testServer, request)
	require.NoError(err)
	defer response.Body.Close()
	require.Equal(http.StatusOK, response.StatusCode)

	var rpcResponse jsonrpc.Response
	err = json.NewDecoder(response.Body).Decode(&rpcResponse)
	require.NoError(err)
	require.Nil(rpcResponse.Result)
	require.Error(rpcResponse.Error)
	require.Equal("2.0", rpcResponse.JSONRPC)
	require.Equal(uint(1), rpcResponse.ID)
	require.Equal(-32602, rpcResponse.Error.Code)
	require.Contains(rpcResponse.Error.Message, "hex string without 0x prefix")
}

func (s *ServerTestSuite) TestTraceBlockByNumber() {
	require := testutil.Require(s.T())

	expected := fixtures.MustReadJson("controller/ethereum/eth_trace.json")
	height, err := hexutil.DecodeUint64("0xe11130")
	require.NoError(err)
	s.receiver.EXPECT().
		TraceBlockByNumber(gomock.Any(), rpc.BlockNumber(height), gomock.Any()).
		Return(expected, nil)

	request := fixtures.MustReadFile("server/debug_traceBlockByNumber.json")
	response, err := post(s.testServer, request)
	require.NoError(err)
	defer response.Body.Close()
	require.Equal(http.StatusOK, response.StatusCode)

	var rpcResponse jsonrpc.Response
	err = json.NewDecoder(response.Body).Decode(&rpcResponse)
	require.NoError(err)
	require.Nil(rpcResponse.Error)
	require.Equal("2.0", rpcResponse.JSONRPC)
	require.Equal(uint(1), rpcResponse.ID)
	require.Equal(string(expected), string(rpcResponse.Result))
}

func (s *ServerTestSuite) TestTraceBlockByNumber_InvalidNumber() {
	require := testutil.Require(s.T())

	request := fixtures.MustReadFile("server/debug_traceBlockByNumber_invalidNumber.json")
	response, err := post(s.testServer, request)
	defer response.Body.Close()
	require.NoError(err)
	require.Equal(http.StatusOK, response.StatusCode)

	var rpcResponse jsonrpc.Response
	err = json.NewDecoder(response.Body).Decode(&rpcResponse)
	require.NoError(err)
	require.Nil(rpcResponse.Result)
	require.Error(rpcResponse.Error)
	require.Equal("2.0", rpcResponse.JSONRPC)
	require.Equal(uint(1), rpcResponse.ID)
	require.Equal(-32602, rpcResponse.Error.Code)
	require.Contains(rpcResponse.Error.Message, "hex string without 0x prefix")
}

func (s *ServerTestSuite) TestBlock_InvalidNumber() {
	require := testutil.Require(s.T())

	request := fixtures.MustReadFile("server/arbtrace_block_invalidNumber.json")
	response, err := post(s.testServer, request)
	defer response.Body.Close()
	require.NoError(err)
	require.Equal(http.StatusOK, response.StatusCode)

	var rpcResponse jsonrpc.Response
	err = json.NewDecoder(response.Body).Decode(&rpcResponse)
	require.NoError(err)
	require.Nil(rpcResponse.Result)
	require.Error(rpcResponse.Error)
	require.Equal("2.0", rpcResponse.JSONRPC)
	require.Equal(uint(1), rpcResponse.ID)
	require.Equal(-32602, rpcResponse.Error.Code)
	require.Contains(rpcResponse.Error.Message, "hex string without 0x prefix")
}

func (s *ServerTestSuite) TestGetTransactionReceipt() {
	require := testutil.Require(s.T())

	expected := fixtures.MustReadJson("controller/ethereum/eth_transactionreceipt_1.json")
	s.receiver.EXPECT().
		GetTransactionReceipt(gomock.Any(), gomock.Any()).
		Return(expected, nil)

	request := fixtures.MustReadFile("server/eth_getTransactionReceipt.json")
	response, err := post(s.testServer, request)
	require.NoError(err)
	defer response.Body.Close()
	require.Equal(http.StatusOK, response.StatusCode)

	var rpcResponse jsonrpc.Response
	err = json.NewDecoder(response.Body).Decode(&rpcResponse)
	require.NoError(err)
	require.Nil(rpcResponse.Error)
	require.Equal("2.0", rpcResponse.JSONRPC)
	require.Equal(uint(1), rpcResponse.ID)
	require.Equal(string(expected), string(rpcResponse.Result))
}

func (s *ServerTestSuite) TestBatchGetTransactionReceipt() {
	require := testutil.Require(s.T())

	receipts := [][]byte{
		fixtures.MustReadJson("controller/ethereum/eth_transactionreceipt_1.json"),
		fixtures.MustReadJson("controller/ethereum/eth_transactionreceipt_2.json"),
	}
	s.receiver.EXPECT().
		GetTransactionReceipt(gomock.Any(), common.HexToHash("0x633982a26e0cfba940613c52b31c664fe977e05171e35f62da2426596007e249")).
		Return(receipts[0], nil)
	s.receiver.EXPECT().
		GetTransactionReceipt(gomock.Any(), common.HexToHash("0x3a7d521b20b5684e0e9ec14aeebe8ccab67137f7d5c2589efb55b0625fcc9c6d")).
		Return(receipts[1], nil)

	request := fixtures.MustReadFile("server/batch_eth_getTransactionReceipt.json")
	response, err := post(s.testServer, request)
	require.NoError(err)
	defer response.Body.Close()
	require.Equal(http.StatusOK, response.StatusCode)

	var rpcResponses []jsonrpc.Response
	err = json.NewDecoder(response.Body).Decode(&rpcResponses)
	require.NoError(err)
	require.Equal(2, len(rpcResponses))
	for i, rpcResponse := range rpcResponses {
		require.Nil(rpcResponse.Error)
		require.Equal("2.0", rpcResponse.JSONRPC)
		require.Equal(uint(1+i), rpcResponse.ID)
		expected := string(receipts[i])
		require.Equal(expected, string(rpcResponse.Result))
	}
}

func (s *ServerTestSuite) TestBatchCallAll() {
	require := testutil.Require(s.T())

	ethCallExpected := "0x00000000000000000000000000000000000000000000058153137e54f048f08b"
	s.receiver.EXPECT().
		Call(gomock.Any(), gomock.Any(), "latest").
		Return(json.Marshal(ethCallExpected))

	ethBlockNumberExpected, err := json.Marshal("0xa7d8d3")
	require.NoError(err)
	s.receiver.EXPECT().
		BlockNumber(gomock.Any()).
		Return(ethBlockNumberExpected, nil)

	ethGetBlockByNumberExpected := fixtures.MustReadJson("controller/ethereum/eth_block_1.json")
	s.receiver.EXPECT().
		GetBlockByNumber(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(ethGetBlockByNumberExpected, nil)

	ethGetLogsExpected := fixtures.MustReadJson("controller/ethereum/eth_logs_1.json")
	s.receiver.EXPECT().
		GetLogs(gomock.Any(), gomock.Any()).
		Return(ethGetLogsExpected, nil)

	ethGetTransactionByHashExpected := fixtures.MustReadJson("controller/ethereum/eth_transaction_1.json")
	s.receiver.EXPECT().
		GetTransactionByHash(gomock.Any(), gomock.Any()).
		Return(ethGetTransactionByHashExpected, nil)

	ethGetTransactionReceiptExpected := fixtures.MustReadJson("controller/ethereum/eth_transactionreceipt_1.json")
	s.receiver.EXPECT().
		GetTransactionReceipt(gomock.Any(), gomock.Any()).
		Return(ethGetTransactionReceiptExpected, nil)

	expected := [][]byte{[]byte("\"" + ethCallExpected + "\""), ethBlockNumberExpected, ethGetBlockByNumberExpected, ethGetLogsExpected, ethGetTransactionByHashExpected, ethGetTransactionReceiptExpected}

	request := fixtures.MustReadFile("server/batch_eth_all.json")
	response, err := post(s.testServer, request)
	require.NoError(err)
	defer response.Body.Close()
	require.Equal(http.StatusOK, response.StatusCode)

	var rpcResponses []jsonrpc.Response
	err = json.NewDecoder(response.Body).Decode(&rpcResponses)
	require.NoError(err)
	require.Equal(6, len(rpcResponses))
	for i, rpcResponse := range rpcResponses {
		require.Nil(rpcResponse.Error)
		require.Equal("2.0", rpcResponse.JSONRPC)
		require.Equal(uint(1+i), rpcResponse.ID)
		exp := string(expected[i])
		require.Equal(exp, string(rpcResponse.Result))
	}
}

func (s *ServerTestSuite) TestGetAuthor() {
	require := testutil.Require(s.T())

	expected, err := json.Marshal("0xasda")
	require.NoError(err)
	height, err := hexutil.DecodeUint64("0xe11130")
	require.NoError(err)
	s.receiver.EXPECT().
		GetAuthor(gomock.Any(), rpc.BlockNumber(height)).
		Return(expected, nil)

	request := fixtures.MustReadFile("server/bor_getAuthor.json")
	response, err := post(s.testServer, request)
	require.NoError(err)
	defer response.Body.Close()
	require.Equal(http.StatusOK, response.StatusCode)

	var rpcResponse jsonrpc.Response
	err = json.NewDecoder(response.Body).Decode(&rpcResponse)
	require.NoError(err)
	require.Nil(rpcResponse.Error)
	require.Equal("2.0", rpcResponse.JSONRPC)
	require.Equal(uint(1), rpcResponse.ID)
	require.Equal(string(expected), string(rpcResponse.Result))
}

func post(server *httptest.Server, body []byte) (*http.Response, error) {
	return postWithHeaders(server, body, nil)
}

func postWithHeaders(server *httptest.Server, body []byte, headers map[string]string) (*http.Response, error) {
	url := server.URL + handlerPath
	request, err := http.NewRequest(http.MethodPost, url, bytes.NewBuffer(body))
	if err != nil {
		return nil, err
	}

	request.Header["Content-Type"] = []string{"application/json"}
	for k, v := range headers {
		request.Header.Set(k, v)
	}

	return http.DefaultClient.Do(request)
}
