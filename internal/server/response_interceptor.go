package server

import (
	"net/http"

	"golang.org/x/xerrors"
	"google.golang.org/grpc/status"

	"github.com/coinbase/chainnode/internal/api"
)

type (
	ResponseInterceptor struct {
		http.ResponseWriter

		statusCode int
	}

	ResponseInterceptorError struct {
		serverErr *api.ServerError
	}
)

var (
	errRPCServer = xerrors.New("rpc server error")
)

func NewResponseInterceptor(writer http.ResponseWriter) *ResponseInterceptor {
	return &ResponseInterceptor{
		ResponseWriter: writer,
		statusCode:     http.StatusOK,
	}
}

// WriteHeader intercepts the response status code.
func (i *ResponseInterceptor) WriteHeader(statusCode int) {
	i.statusCode = statusCode
	i.ResponseWriter.WriteHeader(statusCode)
}

func (i *ResponseInterceptor) StatusCode() int {
	return i.statusCode
}

func NewResponseInterceptorError(statusCode int) error {
	return &ResponseInterceptorError{
		serverErr: api.NewServerError(statusCode, errRPCServer),
	}
}

func (e *ResponseInterceptorError) Error() string {
	return e.serverErr.Error()
}

func (e *ResponseInterceptorError) GRPCStatus() *status.Status {
	return e.serverErr.GRPCStatus()
}
