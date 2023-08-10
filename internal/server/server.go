package server

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"time"

	middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/grpclog"

	"github.com/coinbase/chainnode/internal/api"
	"github.com/coinbase/chainnode/internal/config"
	"github.com/coinbase/chainnode/internal/controller"
	"github.com/coinbase/chainnode/internal/server/rpc"
	"github.com/coinbase/chainnode/internal/utils/constants"
	"github.com/coinbase/chainnode/internal/utils/fxparams"
	"github.com/coinbase/chainnode/internal/utils/log"

	"github.com/coinbase/chainstorage/sdk/services"
)

type (
	ServerParams struct {
		fx.In
		fxparams.Params
		Lifecycle  fx.Lifecycle
		Controller controller.Controller
		Manager    services.SystemManager
		RPCServer  RPCServer `optional:"true"`
	}

	Server struct {
		logger      *zap.Logger
		config      *config.Config
		manager     services.SystemManager
		address     string
		mux         *http.ServeMux
		rpcServer   RPCServer
		interceptor grpc.UnaryServerInterceptor
	}

	RPCServer interface {
		http.Handler
		RegisterName(name string, receiver interface{}) error
		Stop()
	}
)

const (
	// This empty address is returned by manager.Config().HTTPServer() when MockSystemManager is in use.
	emptyAddress = ":"

	shutdownTimeout = 10 * time.Second
)

var (
	_ http.Handler = (*Server)(nil)
	_ RPCServer    = (*rpc.Server)(nil)
)

func NewServer(params ServerParams) (*Server, error) {
	handler := params.Controller.Handler()
	rpcServer := RPCServer(rpc.NewServer(rpc.ServerParams{BatchLimitConfig: params.Config.BatchLimitConfig, Scope: params.Metrics}))
	if params.RPCServer != nil {
		// Used by unit test.
		rpcServer = params.RPCServer
	}

	namespaces := handler.Namespaces()
	for name, namespace := range namespaces {
		if err := rpcServer.RegisterName(name, namespace); err != nil {
			return nil, xerrors.Errorf("failed to register handler: %w", err)
		}
	}

	logger := log.WithPackage(params.Logger)
	address := params.Config.Server.BindAddress
	mux := http.NewServeMux()

	unaryInterceptor := middleware.ChainUnaryServer(
	// XXX: Add your own interceptors here.
	)

	server := &Server{
		logger:      logger,
		config:      params.Config,
		address:     address,
		manager:     params.Manager,
		mux:         mux,
		rpcServer:   rpcServer,
		interceptor: unaryInterceptor,
	}

	server.registerHandler(handler.Path(), rpcServer, handler)
	for _, reverseProxy := range params.Controller.ReverseProxies() {
		server.registerHandler(reverseProxy.Path(), reverseProxy.Handler(), reverseProxy)
	}

	params.Lifecycle.Append(fx.Hook{
		OnStart: server.onStart,
		OnStop:  server.onStop,
	})

	return server, nil
}

func (s *Server) onStart(ctx context.Context) error {
	if s.address != emptyAddress {
		s.logger.Info("starting server", zap.String("address", s.address))
		s.daemonizeServer(s.manager)
	}
	return nil
}

func (s *Server) onStop(ctx context.Context) error {
	s.logger.Info("stopping server")
	s.rpcServer.Stop()
	return nil
}

func (s *Server) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	s.mux.ServeHTTP(writer, request)
}

func (s *Server) registerHandler(
	path string,
	handler http.Handler,
	preHandler controller.PreHandler,
) {
	// Since the method name cannot contain "/", replace it with "_".
	method := strings.ReplaceAll(
		strings.TrimPrefix(path, "/"),
		"/",
		"_",
	)
	serverInfo := &grpc.UnaryServerInfo{
		FullMethod: fmt.Sprintf("/%v/%v", constants.FullServiceName, method),
	}

	s.mux.HandleFunc(path, func(writer http.ResponseWriter, request *http.Request) {
		requestInterceptor := NewRequestInterceptor(request, preHandler)
		req := requestInterceptor.Body()
		ctx := request.Context()

		_, err := s.interceptor(ctx, req, serverInfo, func(ctx context.Context, req interface{}) (interface{}, error) {
			childRequest, err := requestInterceptor.WithContext(ctx)
			if err != nil {
				return nil, err
			}

			childWriter := NewResponseInterceptor(writer)
			handler.ServeHTTP(childWriter, childRequest)

			if statusCode := childWriter.StatusCode(); statusCode >= http.StatusBadRequest {
				return nil, NewResponseInterceptorError(statusCode)
			}

			return nil, nil
		})
		if err != nil {
			// If the error was intercepted from the response writer, we do not need to write an error again.
			var interceptorError *ResponseInterceptorError
			if xerrors.As(err, &interceptorError) {
				return
			}

			// If the error is a ServerError, return its HTTPStatus.
			var serverError *api.ServerError
			if xerrors.As(err, &serverError) {
				http.Error(writer, serverError.Error(), serverError.HTTPStatus())
				return
			}

			// Otherwise, return 503.
			// For example, a panic within the handler would be intercepted as an error here.
			http.Error(writer, err.Error(), http.StatusServiceUnavailable)
			return
		}
	})
}

func (s *Server) daemonizeServer(
	manager services.SystemManager,
) {
	runHTTPServer := func(ctx context.Context) (services.ShutdownFunction, chan error) {
		return s.startServer(ctx, manager.Logger(), s.address)
	}
	manager.ServiceWaitGroup().Add(1)
	go func() {
		defer manager.ServiceWaitGroup().Done()
		services.Daemonize(manager, runHTTPServer, "HTTP Server")
	}()
}

func (s *Server) startServer(ctx context.Context, log *zap.Logger, bindAddress string) (services.ShutdownFunction, chan error) {
	c, cancelFunc := context.WithCancel(ctx)

	log.Info("Starting httprpc server")

	shutdownFunc := func(shutdownContext context.Context) error {
		cancelFunc()
		return nil
	}

	errorChannel := make(chan error)
	go func() {
		errorChannel <- s.Listen(c, bindAddress)
	}()

	return shutdownFunc, errorChannel
}

func (s *Server) Listen(ctx context.Context, bindAddress string) error {
	server := http.Server{
		Addr:              bindAddress,
		Handler:           s.mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		<-ctx.Done()
		sCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		err := server.Shutdown(sCtx)
		grpclog.Warning("error shutting down httprpc server: %v", err)
	}()

	return server.ListenAndServe()
}
