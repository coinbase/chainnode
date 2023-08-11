package rpc

import (
	"context"
	"fmt"
	"io"
	"sync/atomic"

	mapset "github.com/deckarep/golang-set"
	"github.com/ethereum/go-ethereum/log"
	"github.com/uber-go/tally/v4"

	"github.com/coinbase/chainnode/internal/config"
)

const (
	// OptionMethodInvocation is an indication that the codec supports RPC method calls
	OptionMethodInvocation CodecOption = 1 << iota

	// OptionSubscriptions is an indication that the codec supports RPC notifications
	OptionSubscriptions = 1 << iota // support pub sub

	DefaultBatchLimitKeyName = "defaultBatchLimit"

	MetadataApi = "rpc"

	EngineApi = "engine"

	rpcServerScope         = "rpc_server"
	serverRequestMetric    = "request"
	batchRequestSizeMetric = "request.size"
	modeField              = "mode"
	batchMode              = "batch"
	singleMode             = "single"
)

type (
	// CodecOption specifies which type of messages a codec supports.
	//
	// Deprecated: this option is no longer honored by Server.
	CodecOption int

	ServerParams struct {
		BatchLimitConfig config.BatchLimitConfig
		Scope            tally.Scope
	}

	// Server is an RPC server.
	Server struct {
		services         serviceRegistry
		idgen            func() ID
		run              int32
		codecs           mapset.Set
		batchLimitConfig map[string]int
		metrics          serverMetrics
	}

	serverMetrics struct {
		batchRequestCount  tally.Counter
		batchRequestSize   tally.Gauge
		singleRequestCount tally.Counter
	}
)

// #nosec G104 code migrated from https://github.com/ethereum/go-ethereum. Will look into addressing this in the future.
// NewServer creates a new server instance with no registered handlers.
func NewServer(params ServerParams) *Server {
	scope := params.Scope.SubScope(rpcServerScope)
	server := &Server{
		idgen:            randomIDGenerator(),
		codecs:           mapset.NewSet(),
		run:              1,
		batchLimitConfig: transformBatchLimitConfig(params.BatchLimitConfig),
		metrics: serverMetrics{
			batchRequestCount:  scope.Tagged(map[string]string{modeField: batchMode}).Counter(serverRequestMetric),
			batchRequestSize:   scope.Tagged(map[string]string{modeField: batchMode}).Gauge(batchRequestSizeMetric),
			singleRequestCount: scope.Tagged(map[string]string{modeField: singleMode}).Counter(serverRequestMetric),
		}}
	// Register the default service providing meta information about the RPC service such
	// as the services and methods it offers.
	rpcService := &RPCService{server}
	server.RegisterName(MetadataApi, rpcService)
	return server
}

// RegisterName creates a service for the given receiver type under the given name. When no
// methods on the given receiver match the criteria to be either a RPC method or a
// subscription an error is returned. Otherwise a new service is created and added to the
// service collection this server provides to clients.
func (s *Server) RegisterName(name string, receiver interface{}) error {
	return s.services.registerName(name, receiver)
}

// ServeCodec reads incoming requests from codec, calls the appropriate callback and writes
// the response back using the given codec. It will block until the codec is closed or the
// server is stopped. In either case the codec is closed.
//
// Note that codec options are no longer supported.
func (s *Server) ServeCodec(codec ServerCodec, options CodecOption) {
	defer codec.close()

	// Don't serve if server is stopped.
	if atomic.LoadInt32(&s.run) == 0 {
		return
	}

	// Add the codec to the set so it can be closed by Stop.
	s.codecs.Add(codec)
	defer s.codecs.Remove(codec)

	c := initClient(codec, s.idgen, &s.services)
	<-codec.closed()
	c.Close()
}

// #nosec G104 code migrated from https://github.com/ethereum/go-ethereum. Will look into addressing this in the future.
// serveSingleRequest reads and processes a single RPC request from the given codec. This
// is used to serve HTTP connections. Subscriptions and reverse calls are not allowed in
// this mode.
func (s *Server) serveSingleRequest(ctx context.Context, codec ServerCodec) {
	// Don't serve if server is stopped.
	if atomic.LoadInt32(&s.run) == 0 {
		return
	}

	h := newHandler(ctx, codec, s.idgen, &s.services)
	h.allowSubscribe = false
	defer h.close(io.EOF, nil)

	reqs, batch, err := codec.readBatch()
	if err != nil {
		if err != io.EOF {
			codec.writeJSON(ctx, errorMessage(&invalidMessageError{"parse error"}))
		}
		return
	}
	if batch {
		if reject, errMsg := s.rejectBatchRequest(len(reqs)); reject {
			codec.writeJSON(ctx, errorMessage(&invalidRequestError{errMsg}))
			return
		}
		s.metrics.batchRequestCount.Inc(1)
		s.metrics.batchRequestSize.Update(float64(len(reqs)))
		h.handleBatch(reqs)
	} else {
		s.metrics.singleRequestCount.Inc(1)
		h.handleMsg(reqs[0])
	}
}

// Stop stops reading new requests, waits for stopPendingRequestTimeout to allow pending
// requests to finish, then closes all codecs which will cancel pending requests and
// subscriptions.
func (s *Server) Stop() {
	if atomic.CompareAndSwapInt32(&s.run, 1, 0) {
		log.Debug("RPC server shutting down")
		s.codecs.Each(func(c interface{}) bool {
			c.(ServerCodec).close()
			return true
		})
	}
}

func (s *Server) rejectBatchRequest(batchSize int) (bool, string) {
	if batchSize > s.batchLimitConfig[DefaultBatchLimitKeyName] {
		return true, fmt.Sprintf("maximum allowed batch size %v", s.batchLimitConfig[DefaultBatchLimitKeyName])
	}

	return false, ""
}

// RPCService gives meta information about the server.
// e.g. gives information about the loaded modules.
type RPCService struct {
	server *Server
}

// Modules returns the list of RPC services with their version number
func (s *RPCService) Modules() map[string]string {
	s.server.services.mu.Lock()
	defer s.server.services.mu.Unlock()

	modules := make(map[string]string)
	for name := range s.server.services.services {
		modules[name] = "1.0"
	}
	return modules
}

// PeerInfo contains information about the remote end of the network connection.
//
// This is available within RPC method handlers through the context. Call
// PeerInfoFromContext to get information about the client connection related to
// the current method call.
type PeerInfo struct {
	// Transport is name of the protocol used by the client.
	// This can be "http", "ws" or "ipc".
	Transport string

	// Address of client. This will usually contain the IP address and port.
	RemoteAddr string

	// Addditional information for HTTP and WebSocket connections.
	HTTP struct {
		// Protocol version, i.e. "HTTP/1.1". This is not set for WebSocket.
		Version string
		// Header values sent by the client.
		UserAgent string
		Origin    string
		Host      string
	}
}

type peerInfoContextKey struct{}

// PeerInfoFromContext returns information about the client's network connection.
// Use this with the context passed to RPC method handler functions.
//
// The zero value is returned if no connection info is present in ctx.
func PeerInfoFromContext(ctx context.Context) PeerInfo {
	info, _ := ctx.Value(peerInfoContextKey{}).(PeerInfo)
	return info
}

func transformBatchLimitConfig(batchLimitConfig config.BatchLimitConfig) map[string]int {
	transformedBatchLimitConfig := map[string]int{
		DefaultBatchLimitKeyName: batchLimitConfig.DefaultLimit,
	}

	return transformedBatchLimitConfig
}
