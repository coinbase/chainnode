package internal

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httputil"
	"net/url"

	"golang.org/x/xerrors"
)

type (
	ReverseProxy interface {
		PreHandler
		Path() string
		Handler() http.Handler
	}

	reverseProxy struct {
		path    string
		handler http.Handler
	}
)

func NewReverseProxy(path string, target string, header http.Header) (ReverseProxy, error) {
	targetURL, err := url.Parse(target)
	if err != nil {
		return nil, xerrors.Errorf("failed to parse target url: %w", err)
	}

	handler := httputil.NewSingleHostReverseProxy(targetURL)

	directorImpl := handler.Director
	handler.Director = func(request *http.Request) {
		// The request header may contain sensitive information.
		// Use a hardcoded header instead.
		// Note that the header should be cloned since this function may be called concurrently.
		request.Header = header.Clone()

		// No need to forward RemoteAddr.
		request.RemoteAddr = ""

		directorImpl(request)
	}

	handler.ModifyResponse = func(response *http.Response) error {
		// A reverse proxy should never return 401.
		// Map 401 into 503 for monitoring purposes.
		if response.StatusCode == http.StatusUnauthorized {
			response.StatusCode = http.StatusServiceUnavailable
		}

		return nil
	}

	return &reverseProxy{
		path:    path,
		handler: handler,
	}, nil
}

func (p *reverseProxy) PrepareContext(ctx context.Context, request json.RawMessage) (context.Context, error) {
	return ctx, nil
}

func (p *reverseProxy) Path() string {
	return p.path
}

func (p *reverseProxy) Handler() http.Handler {
	return p.handler
}
