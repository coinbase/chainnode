package api

import (
	"fmt"
	"net/http"

	"golang.org/x/xerrors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type (
	ServerError struct {
		statusCode int
		cause      error
	}

	grpcError interface {
		GRPCStatus() *status.Status
	}
)

const (
	// StatusCanceled is returned when a client cancels the request while it is being processed.
	// Ref: https://www.webfx.com/web-development/glossary/http-status-codes/what-is-a-499-status-code/
	StatusCanceled = 499

	// StatusTooManyForwardedAddresses is returned when the HTTP request includes the X-Forwarded-For header with too many IP addresses.
	// See https://http.dev/463
	StatusTooManyForwardedAddresses = 463
)

var (
	ErrNotImplemented         = xerrors.New("not implemented")
	ErrNotAllowed             = xerrors.New("not allowed")
	ErrInvalidHttpHeaderValue = xerrors.New("invalid http header value")

	_ grpcError     = (*ServerError)(nil)
	_ fmt.Formatter = (*ServerError)(nil)
)

func NewServerError(statusCode int, cause error) *ServerError {
	return &ServerError{
		statusCode: statusCode,
		cause:      cause,
	}
}

func (e *ServerError) Error() string {
	msg := fmt.Sprintf("%v %v", e.statusCode, http.StatusText(e.statusCode))
	if e.cause != nil {
		msg = fmt.Sprintf("%v\n%v", msg, e.cause.Error())
	}

	return msg
}

// HTTPStatus is used as the status code of the HTTP response.
func (e *ServerError) HTTPStatus() int {
	return e.statusCode
}

func (e *ServerError) GRPCStatus() *status.Status {
	var code codes.Code
	switch e.statusCode {
	case http.StatusNotFound:
		code = codes.NotFound
	case http.StatusUnauthorized:
		code = codes.Unauthenticated
	case StatusCanceled:
		code = codes.Canceled
	case StatusTooManyForwardedAddresses:
		code = codes.Internal
	default:
		if e.statusCode >= http.StatusBadRequest && e.statusCode < http.StatusInternalServerError {
			// Map the rest of 4xx to InvalidArgument, e.g.
			// - StatusBadRequest if "failed to read request"
			// - StatusMethodNotAllowed if method is PUT or DELETE
			// - StatusUnsupportedMediaType if content type is invalid
			code = codes.InvalidArgument
		} else {
			code = codes.Internal
		}
	}
	return status.New(code, e.Error())
}

// Format provides the "errorVerbose" field in Datadog.
func (e *ServerError) Format(state fmt.State, verb rune) {
	if verb == 'v' && state.Flag('+') {
		_, _ = fmt.Fprintf(state, "%v\n", e.Error())
		if e.cause != nil {
			_, _ = fmt.Fprintf(state, "%+v\n", e.cause)
		}
	}
}
