package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainnode/internal/api"
	"github.com/coinbase/chainnode/internal/controller"
	"github.com/coinbase/chainnode/internal/controller/ethereum/handler"
	handlermocks "github.com/coinbase/chainnode/internal/controller/ethereum/handler/mocks"
	controllermocks "github.com/coinbase/chainnode/internal/controller/mocks"
	servermocks "github.com/coinbase/chainnode/internal/server/mocks"
	"github.com/coinbase/chainnode/internal/utils/fixtures"
	"github.com/coinbase/chainnode/internal/utils/testapp"
	"github.com/coinbase/chainnode/internal/utils/testutil"
)

type (
	ResponseInterceptorTestSuite struct {
		suite.Suite
		ctrl       *gomock.Controller
		app        testapp.TestApp
		controller *controllermocks.MockController
		handler    *controllermocks.MockHandler
		receiver   *handlermocks.MockReceiver
		namespaces map[string]interface{}
		rpcServer  *servermocks.MockRPCServer
		server     *Server
		testServer *httptest.Server
	}

	TestNamespace struct {
		receiver handler.Receiver
	}
)

func TestResponseInterceptorTestSuite(t *testing.T) {
	suite.Run(t, new(ResponseInterceptorTestSuite))
}

func (s *ResponseInterceptorTestSuite) SetupTest() {
	s.ctrl = gomock.NewController(s.T())
	s.controller = controllermocks.NewMockController(s.ctrl)
	s.handler = controllermocks.NewMockHandler(s.ctrl)
	s.receiver = handlermocks.NewMockReceiver(s.ctrl)
	s.rpcServer = servermocks.NewMockRPCServer(s.ctrl)
	s.controller.EXPECT().Handler().Return(s.handler)
	s.controller.EXPECT().ReverseProxies().Return(nil)
	s.handler.EXPECT().Path().Return(handlerPath).AnyTimes()
	s.namespaces = map[string]interface{}{"test": &TestNamespace{receiver: s.receiver}}
	s.handler.EXPECT().Namespaces().Return(s.namespaces)
	s.rpcServer.EXPECT().RegisterName(gomock.Any(), gomock.Any()).Return(nil)
	s.rpcServer.EXPECT().Stop().Return()
	s.app = testapp.New(
		s.T(),
		Module,
		fx.Provide(func() controller.Controller { return s.controller }),
		fx.Provide(func() RPCServer { return s.rpcServer }),
		fx.Populate(&s.server),
	)
	s.testServer = httptest.NewServer(s.server)
}

func (s *ResponseInterceptorTestSuite) TearDownTest() {
	s.testServer.Close()
	s.app.Close()
}

func (s *ResponseInterceptorTestSuite) TestInterceptorError() {
	require := testutil.Require(s.T())

	s.handler.EXPECT().
		PrepareContext(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, request json.RawMessage) (context.Context, error) {
			return ctx, nil
		})

	s.rpcServer.EXPECT().ServeHTTP(gomock.Any(), gomock.Any()).
		DoAndReturn(func(writer http.ResponseWriter, request *http.Request) {
			writer.WriteHeader(http.StatusForbidden)
		})

	request := fixtures.MustReadFile("server/eth_call.json")
	response, err := post(s.testServer, request)
	require.NoError(err)
	require.Equal(http.StatusForbidden, response.StatusCode)
}

func (s *ResponseInterceptorTestSuite) TestServerError() {
	require := testutil.Require(s.T())

	s.handler.EXPECT().
		PrepareContext(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, request json.RawMessage) (context.Context, error) {
			return nil, xerrors.Errorf("mock error: %w", api.NewServerError(api.StatusCanceled, xerrors.New("mock error")))
		})

	request := fixtures.MustReadFile("server/eth_call.json")
	response, err := post(s.testServer, request)
	require.NoError(err)
	require.Equal(api.StatusCanceled, response.StatusCode)
}
