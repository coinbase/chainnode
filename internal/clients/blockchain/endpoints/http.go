package endpoints

import (
	"net/http"
	"time"

	tracehttp "gopkg.in/DataDog/dd-trace-go.v1/contrib/net/http"
)

type (
	ClientOption func(opts *clientOptions)

	clientOptions struct {
		timeout         time.Duration
		idleConnTimeout time.Duration
		maxConns        int
	}
)

const (
	defaultTimeout  = 30 * time.Second
	defaultMaxConns = 100
)

func newHTTPClient(opts ...ClientOption) (*http.Client, error) {
	options := &clientOptions{
		timeout:  defaultTimeout,
		maxConns: defaultMaxConns,
	}
	for _, opt := range opts {
		opt(options)
	}

	httpClient, err := newDefaultClient(options)
	if err != nil {
		return nil, err
	}

	return tracehttp.WrapClient(httpClient), nil
}

func newDefaultClient(options *clientOptions) (*http.Client, error) {
	transport := http.DefaultTransport.(*http.Transport).Clone()

	transport.MaxIdleConns = options.maxConns
	transport.MaxIdleConnsPerHost = options.maxConns // If not set, MaxIdleConnsPerHost defaults to 2.

	return &http.Client{
		Timeout:   options.timeout,
		Transport: transport,
	}, nil
}
