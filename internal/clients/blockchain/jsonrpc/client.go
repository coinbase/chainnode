package jsonrpc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/uber-go/tally/v4"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainnode/internal/clients/blockchain/endpoints"
	"github.com/coinbase/chainnode/internal/config"
	"github.com/coinbase/chainnode/internal/server/rpc"

	"github.com/coinbase/chainnode/internal/utils/finalizer"
	"github.com/coinbase/chainnode/internal/utils/fxparams"
	"github.com/coinbase/chainnode/internal/utils/instrument"
	"github.com/coinbase/chainnode/internal/utils/log"
	"github.com/coinbase/chainnode/internal/utils/retry"
)

type (
	Client interface {
		Call(ctx context.Context, method *RequestMethod, params Params) (*Response, error)
		BatchCall(ctx context.Context, method *RequestMethod, batchParams []Params) ([]*Response, error)

		GetEndpointProvider() endpoints.EndpointProvider
	}

	HTTPClient interface {
		Do(req *http.Request) (*http.Response, error)
	}

	ClientParams struct {
		fx.In
		fxparams.Params
		EndpointProviderServer    endpoints.EndpointProvider `name:"server"`
		EndpointProviderProxy     endpoints.EndpointProvider `name:"proxy"`
		EndpointProviderValidator endpoints.EndpointProvider `name:"validator"`
		HTTPClient                HTTPClient                 `optional:"true"` // Injected by unit test.
	}

	ClientResult struct {
		fx.Out
		Server    Client `name:"server"`
		Proxy     Client `name:"proxy"`
		Validator Client `name:"validator"`
	}

	Request struct {
		JSONRPC string      `json:"jsonrpc"`
		Method  string      `json:"method"`
		Params  interface{} `json:"params,omitempty"`
		ID      uint        `json:"id"`
	}

	Response struct {
		JSONRPC string          `json:"jsonrpc"`
		Result  json.RawMessage `json:"result,omitempty"`
		Error   *RPCError       `json:"error,omitempty"`
		ID      uint            `json:"id"`
	}

	RPCError struct {
		Code    int             `json:"code"`
		Message string          `json:"message"`
		Data    json.RawMessage `json:"data"`

		cause error
	}

	HTTPError struct {
		Code     int
		Response string
	}

	RequestMethod struct {
		Name    string
		Timeout time.Duration
	}

	Params []interface{}
)

type (
	clientImpl struct {
		logger           *zap.Logger
		metrics          tally.Scope
		httpClient       HTTPClient
		retry            retry.Retry
		endpointProvider endpoints.EndpointProvider
	}
)

const (
	jsonrpcVersion = "2.0"
	scopeName      = "jsonrpc"
)

var (
	// Use raw error message to align with geth output
	// Ensure that RPCError implements geth.rpc.Error:
	// https://github.com/ethereum/go-ethereum/blob/master/rpc/errors.go#L39
	_ rpc.Error = (*RPCError)(nil)

	// Implements Formatter so that zap writes errorVerbose to Datadog.
	_ fmt.Formatter = (*RPCError)(nil)
)

func New(params ClientParams) (ClientResult, error) {
	clientServer, err := newClient(params, params.EndpointProviderServer)
	if err != nil {
		return ClientResult{}, xerrors.Errorf("failed to create client for server: %w", err)
	}

	clientProxy, err := newClient(params, params.EndpointProviderProxy)
	if err != nil {
		return ClientResult{}, xerrors.Errorf("failed to create client for proxy: %w", err)
	}

	var clientValidator Client
	clientValidator, err = newClient(params, params.EndpointProviderValidator)
	if err != nil {
		return ClientResult{}, xerrors.Errorf("failed to create client for validator: %w", err)
	}

	return ClientResult{
		Server:    clientServer,
		Proxy:     clientProxy,
		Validator: clientValidator,
	}, nil
}

func newClient(params ClientParams, endpointProvider endpoints.EndpointProvider) (Client, error) {
	logger := log.WithPackage(params.Logger)

	httpClient, err := newHTTPClient(params, endpointProvider)
	if err != nil {
		return nil, xerrors.Errorf("failed to create http client: %w", err)
	}

	retryConfig := &params.Config.Chain.Client.Retry
	return &clientImpl{
		logger:           logger,
		metrics:          params.Metrics.SubScope(scopeName),
		httpClient:       httpClient,
		retry:            retryConfig.NewRetry(retry.WithLogger(logger)),
		endpointProvider: endpointProvider,
	}, nil
}

func newHTTPClient(params ClientParams, endpointProvider endpoints.EndpointProvider) (HTTPClient, error) {
	if params.HTTPClient != nil {
		// Injected by unit test.
		return params.HTTPClient, nil
	}

	return endpointProvider.NewHTTPClient()
}

func (c *clientImpl) Call(ctx context.Context, method *RequestMethod, params Params) (*Response, error) {
	endpoint, err := c.endpointProvider.GetEndpoint(ctx)
	if err != nil {
		return nil, xerrors.Errorf("failed to get endpoint for request: %w", err)
	}

	defer c.logDuration(method.Name, endpoint.Name, time.Now())

	request := &Request{
		JSONRPC: jsonrpcVersion,
		Method:  method.Name,
		Params:  params,
		ID:      0,
	}

	var response *Response
	if err := c.instrument(ctx, method.Name, endpoint.Name, func(ctx context.Context) error {
		response = new(Response)
		if err := c.makeHTTPRequest(ctx, method.Timeout, endpoint, request, response); err != nil {
			return xerrors.Errorf("failed to make http request (method=%v, params=%v, endpoint=%v): %w", method, params, endpoint.Name, err)
		}

		return nil
	}); err != nil && response.Error == nil {
		return nil, err
	}

	if response.Error != nil {
		return nil, xerrors.Errorf("received rpc error (method=%v, params=%v, endpoint=%v): %w", method, params, endpoint.Name, response.Error)
	}

	return response, nil
}

func (c *clientImpl) BatchCall(ctx context.Context, method *RequestMethod, batchParams []Params) ([]*Response, error) {
	endpoint, err := c.endpointProvider.GetEndpoint(ctx)
	if err != nil {
		return nil, xerrors.Errorf("failed to get endpoint for request: %w", err)
	}

	defer c.logDuration(method.Name, endpoint.Name, time.Now())

	batchRequests := make([]*Request, len(batchParams))
	for i, params := range batchParams {
		batchRequests[i] = &Request{
			JSONRPC: jsonrpcVersion,
			Method:  method.Name,
			Params:  params,
			ID:      uint(i),
		}
	}

	finalBatchResponses := make([]*Response, len(batchParams))
	if err := c.instrument(ctx, method.Name, endpoint.Name, func(ctx context.Context) error {
		var batchResponses []Response
		if err := c.makeHTTPRequest(ctx, method.Timeout, endpoint, batchRequests, &batchResponses); err != nil {
			return xerrors.Errorf(
				"failed to make http request (method=%v, params=%v, endpoint=%v): %w",
				method, c.formatParams(batchParams), endpoint.Name, err,
			)
		}

		if len(batchParams) != len(batchResponses) {
			return xerrors.Errorf(
				"received wrong number of responses (method=%v, params=%v, endpoint=%v, want=%v, got=%v)",
				method, c.formatParams(batchParams), endpoint.Name, len(batchParams), len(batchResponses),
			)
		}

		// The responses may be out of order.
		// Reorder them to match `batchParams`.
		for i := range batchResponses {
			response := &batchResponses[i]

			id := int(response.ID)
			if id >= len(finalBatchResponses) {
				return xerrors.Errorf(
					"received unexpected response id (method=%v, params=%v, endpoint=%v, id=%v)",
					method, c.formatParams(batchParams), endpoint.Name, id,
				)
			}

			if response.Error != nil {
				return xerrors.Errorf(
					"received rpc error (method=%v, params=%v, endpoint=%v): %w",
					method, c.formatParams(batchParams), endpoint.Name, response.Error,
				)
			}

			if response.IsNullOrEmpty() {
				// Retry the batch call if any of the responses is null.
				return retry.Retryable(xerrors.Errorf(
					"received a null response (method=%v, params=%v, endpoint=%v, index=%v, response=%v)",
					method, c.formatParams(batchParams), endpoint.Name, i, string(response.Result),
				))
			}

			finalBatchResponses[id] = response
		}

		return nil
	}); err != nil {
		return nil, err
	}

	for i := range finalBatchResponses {
		if finalBatchResponses[i] == nil {
			return nil, xerrors.Errorf(
				"missing response (method=%v, params=%v, endpoint=%v, id=%v)",
				method, c.formatParams(batchParams), endpoint.Name, i,
			)
		}
	}

	return finalBatchResponses, nil
}

func (c *clientImpl) GetEndpointProvider() endpoints.EndpointProvider {
	return c.endpointProvider
}

func (c *clientImpl) makeHTTPRequest(ctx context.Context, timeout time.Duration, endpoint *config.Endpoint, data interface{}, out interface{}) error {
	url := endpoint.Url
	user := endpoint.User
	password := endpoint.Password

	requestBody, err := json.Marshal(data)
	if err != nil {
		return xerrors.Errorf("failed to marshal request: %w", err)
	}

	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(requestBody))
	if err != nil {
		err = c.sanitizedError(err)
		return xerrors.Errorf("failed to create request: %w", err)
	}

	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")

	if user != "" && password != "" {
		request.SetBasicAuth(user, password)
	}

	response, err := c.httpClient.Do(request)
	if err != nil {
		err = c.sanitizedError(err)
		return retry.Retryable(xerrors.Errorf("failed to send http request: %w", err))
	}

	finalizer := finalizer.WithCloser(response.Body)
	defer finalizer.Finalize()

	responseBody, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return retry.Retryable(xerrors.Errorf("failed to read http response: %w", err))
	}

	if response.StatusCode != http.StatusOK {
		errHTTP := xerrors.Errorf("received http error: %w", &HTTPError{
			Code:     response.StatusCode,
			Response: string(responseBody),
		})

		// unmarshal responseBody if possible in order to preserve the error code. silence error if there is any.
		// for bitcoin 500 cases, it still returns a deserializable response with non-empty error
		if err := json.Unmarshal(responseBody, out); err != nil {
			c.logger.Warn("failed to decode response", zap.String("response", string(responseBody)), zap.Error(err))
		}

		if response.StatusCode == 429 {
			return retry.RateLimit(errHTTP)
		}

		if response.StatusCode >= 500 {
			return retry.Retryable(errHTTP)
		}

		return errHTTP
	}

	if err := json.Unmarshal(responseBody, out); err != nil {
		return xerrors.Errorf("failed to decode response %v: %w", string(responseBody), err)
	}

	return finalizer.Close()
}

func (c *clientImpl) logDuration(method string, endpoint string, start time.Time) {
	c.logger.Debug(
		"jsonrpc.request",
		zap.String("method", method),
		zap.String("endpoint", endpoint),
		zap.Duration("duration", time.Since(start)),
	)
}

func (c *clientImpl) instrument(ctx context.Context, method string, endpoint string, fn instrument.OperationFn) error {
	tags := map[string]string{
		"method":   method,
		"endpoint": endpoint,
	}
	scope := c.metrics.Tagged(tags)
	call := instrument.NewCall(
		scope,
		"request",
		instrument.WithRetry(c.retry),
		instrument.WithTracer("jsonrpc.request", tags),
	)
	return call.Instrument(ctx, fn)
}

func (c *clientImpl) formatParams(params []Params) string {
	const maxParams = 10
	builder := strings.Builder{}
	builder.WriteRune('[')
	for i, param := range params {
		if i > 0 {
			builder.WriteRune(',')
		}

		if i == maxParams {
			builder.WriteString(fmt.Sprintf("...(%v items)", len(params)))
			break
		}

		builder.WriteString(fmt.Sprintf("%+v", param))
	}

	builder.WriteRune(']')
	return builder.String()
}

func NewRPCError(code int, message string) *RPCError {
	return &RPCError{
		Code:    code,
		Message: message,
	}
}

// Error provides the error message seen by RPC clients.
// In order to keep the error message consistent with the node,
// do not annotate the message with extra information.
func (e *RPCError) Error() string {
	return e.Message
}

// ErrorCode is used by "github.com/coinbase/chainnode/internal/server/rpc" to determine the error code.
func (e *RPCError) ErrorCode() int {
	return e.Code
}

func (e *RPCError) ErrorData() interface{} {
	return e.Data
}

// Format provides the "errorVerbose" field in Datadog.
// The stack trace, if available, is taken from the cause.
func (e *RPCError) Format(state fmt.State, verb rune) {
	if verb == 'v' && state.Flag('+') {
		_, _ = fmt.Fprintf(state, "RPCError %v: %v\n", e.Code, e.Message)
		if e.cause != nil {
			_, _ = fmt.Fprintf(state, "Caused by: %+v\n", e.cause)
		}
	}
}

// WithCause annotates the error with the cause of the error.
// The stack trace will be written to the "errorVerbose" field in Datadog.
func (e *RPCError) WithCause(cause error) *RPCError {
	clone := *e
	clone.cause = cause
	return &clone
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("HTTPError %v: %v", e.Code, e.Response)
}

func (r *Response) Unmarshal(out interface{}) error {
	return json.Unmarshal(r.Result, out)
}

func (r *Response) IsNullOrEmpty() bool {
	if len(r.Result) == 0 ||
		bytes.Equal(r.Result, []byte("{}")) ||
		bytes.Equal(r.Result, []byte("null")) {
		return true
	}

	return false
}

func (c *clientImpl) sanitizedError(err error) error {
	var uerr *url.Error
	if xerrors.As(err, &uerr) {
		// url.Error includes the url in the error message, which may contain the API key.
		err = uerr.Err
	}
	return err
}
