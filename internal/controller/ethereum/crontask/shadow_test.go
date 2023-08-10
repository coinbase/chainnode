package crontask

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally/v4"
	"go.uber.org/fx"

	"github.com/coinbase/chainnode/internal/clients/blockchain/jsonrpc"
	jsonrpcmocks "github.com/coinbase/chainnode/internal/clients/blockchain/jsonrpc/mocks"
	"github.com/coinbase/chainnode/internal/controller/ethereum/handler"
	"github.com/coinbase/chainnode/internal/controller/internal"
	"github.com/coinbase/chainnode/internal/utils/fixtures"
	"github.com/coinbase/chainnode/internal/utils/testapp"
	"github.com/coinbase/chainnode/internal/utils/testutil"
)

type ShadowTaskTestSuite struct {
	suite.Suite
	ctrl   *gomock.Controller
	client *jsonrpcmocks.MockClient
	task   internal.CronTask
	scope  tally.TestScope
	app    testapp.TestApp
}

func TestShadowTaskTestSuite(t *testing.T) {
	suite.Run(t, new(ShadowTaskTestSuite))
}

func (s *ShadowTaskTestSuite) SetupTest() {
	s.ctrl = gomock.NewController(s.T())
	s.client = jsonrpcmocks.NewMockClient(s.ctrl)

	var deps struct {
		fx.In
		Task  internal.CronTask
		Scope tally.Scope
	}
	s.app = testapp.New(
		s.T(),
		fx.Provide(NewShadowTask),
		fx.Provide(fx.Annotated{Name: "proxy", Target: func() jsonrpc.Client { return s.client }}),
		fx.Populate(&deps),
	)
	s.task = deps.Task
	s.scope = deps.Scope.(tally.TestScope)
}

func (s *ShadowTaskTestSuite) TearDownTest() {
	s.app.Close()
}

func (s *ShadowTaskTestSuite) TestRun() {
	require := testutil.Require(s.T())

	blockNumber := 0xacc290
	blockTime := time.Unix(0x5fbd2fb9, 0)
	blockNumberResponse := json.RawMessage(fmt.Sprintf(`"%v"`, hexutil.Uint64(blockNumber).String()))
	s.client.EXPECT().
		Call(gomock.Any(), handler.EthBlockNumber, nil).
		Return(&jsonrpc.Response{Result: blockNumberResponse}, nil)

	blockResponse := fixtures.MustReadFile("controller/ethereum/eth_header.json")
	s.client.EXPECT().
		Call(gomock.Any(), handler.EthGetBlockByNumber, jsonrpc.Params{hexutil.Uint64(blockNumber), false}).
		Return(&jsonrpc.Response{Result: blockResponse}, nil)

	err := s.task.Run(context.Background())
	require.NoError(err)

	snapshot := s.scope.Snapshot()
	height := snapshot.Gauges()["chainnode.crontask.shadow.height+"]
	require.NotNil(height)
	require.Equal(float64(blockNumber), height.Value())

	timeSinceLastBlock := snapshot.Gauges()["chainnode.crontask.shadow.time_since_last_block+"]
	require.NotNil(timeSinceLastBlock)
	require.GreaterOrEqual(time.Since(blockTime).Seconds(), timeSinceLastBlock.Value())
}

func (s *ShadowTaskTestSuite) TestRun_EmptyBlock() {
	require := testutil.Require(s.T())

	blockNumber := 0x0
	blockNumberResponse := json.RawMessage(fmt.Sprintf(`"%v"`, hexutil.Uint64(blockNumber).String()))
	s.client.EXPECT().
		Call(gomock.Any(), handler.EthBlockNumber, nil).
		Return(&jsonrpc.Response{Result: blockNumberResponse}, nil)

	blockResponse := []byte("{\"number\":\"0x0\", \"timestamp\":\"0x0\"}")
	s.client.EXPECT().
		Call(gomock.Any(), handler.EthGetBlockByNumber, jsonrpc.Params{hexutil.Uint64(blockNumber), false}).
		Return(&jsonrpc.Response{Result: blockResponse}, nil)

	err := s.task.Run(context.Background())
	require.NoError(err)
}
