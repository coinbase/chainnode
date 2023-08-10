package server

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"io/ioutil"
	"net/http"

	"golang.org/x/xerrors"

	"github.com/coinbase/chainnode/internal/api"
	"github.com/coinbase/chainnode/internal/controller"
)

type (
	RequestInterceptor struct {
		request    *http.Request
		preHandler controller.PreHandler
		body       []byte
		err        error
	}

	readCloser struct {
		reader io.Reader
		closer io.Closer
	}
)

func NewRequestInterceptor(request *http.Request, preHandler controller.PreHandler) *RequestInterceptor {
	body, err := ioutil.ReadAll(request.Body)
	if err != nil {
		interceptorErr := api.NewServerError(
			http.StatusBadRequest,
			xerrors.Errorf("failed to read request: %w", err),
		)
		return &RequestInterceptor{
			request:    request,
			preHandler: preHandler,
			err:        interceptorErr,
		}
	}

	// Replace body so that it can be read again.
	request.Body = readCloser{
		reader: bytes.NewBuffer(body),
		closer: request.Body,
	}

	return &RequestInterceptor{
		request:    request,
		preHandler: preHandler,
		body:       body,
	}
}

func (i *RequestInterceptor) Body() json.RawMessage {
	// Cast to json.RawMessage so that the body can be logged as a JSON
	// instead of string (i.e. no extraneous quote escaping).
	return i.body
}

func (i *RequestInterceptor) WithContext(ctx context.Context) (*http.Request, error) {
	if i.err != nil {
		return nil, i.err
	}

	ctx, err := i.preHandler.PrepareContext(ctx, i.Body())
	if err != nil {
		return nil, err
	}

	return i.request.WithContext(ctx), nil
}

func (c readCloser) Read(p []byte) (int, error) {
	return c.reader.Read(p)
}

func (c readCloser) Close() error {
	return c.closer.Close()
}
