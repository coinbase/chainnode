package endpoints

import (
	"context"
	"testing"

	"go.uber.org/zap/zaptest"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainnode/internal/config"
	"github.com/coinbase/chainnode/internal/utils/testutil"
)

func TestFailoverManager(t *testing.T) {
	require := testutil.Require(t)
	logger := zaptest.NewLogger(t)

	proxyEndpointGroup := &config.EndpointGroup{
		Endpoints: []config.Endpoint{
			{
				Name:   "foo",
				Weight: 1,
			},
		},
		EndpointsFailover: []config.Endpoint{
			{
				Name:   "bar",
				Weight: 1,
			},
		},
	}

	proxy, err := newEndpointProvider(logger, proxyEndpointGroup, proxyEndpointGroupName)
	require.NoError(err)

	validatorEndpointGroup := &config.EndpointGroup{
		Endpoints: []config.Endpoint{
			{
				Name:   "baz",
				Weight: 1,
			},
		},
		EndpointsFailover: []config.Endpoint{
			{
				Name:   "qux",
				Weight: 1,
			},
		},
	}

	validator, err := newEndpointProvider(logger, validatorEndpointGroup, validatorEndpointGroupName)
	require.NoError(err)

	mgr := NewFailoverManager(FailoverManagerParams{
		Proxy:     proxy,
		Validator: validator,
	})

	ctx := context.Background()
	for i := 0; i < smallNumPicks; i++ {
		endpoint, err := proxy.GetEndpoint(ctx)
		require.NoError(err)
		require.Equal("foo", endpoint.Name)

		endpoint, err = validator.GetEndpoint(ctx)
		require.NoError(err)
		require.Equal("baz", endpoint.Name)
	}

	ctx, err = mgr.WithFailoverContext(ctx)
	require.NoError(err)
	for i := 0; i < smallNumPicks; i++ {
		endpoint, err := proxy.GetEndpoint(ctx)
		require.NoError(err)
		require.Equal("bar", endpoint.Name)

		endpoint, err = validator.GetEndpoint(ctx)
		require.NoError(err)
		require.Equal("qux", endpoint.Name)
	}
}

func TestFailoverManager_ProxyUnavailable(t *testing.T) {
	require := testutil.Require(t)
	logger := zaptest.NewLogger(t)

	proxyEndpointGroup := &config.EndpointGroup{
		Endpoints: []config.Endpoint{
			{
				Name:   "foo",
				Weight: 1,
			},
		},
	}

	proxy, err := newEndpointProvider(logger, proxyEndpointGroup, proxyEndpointGroupName)
	require.NoError(err)

	validatorEndpointGroup := &config.EndpointGroup{
		Endpoints: []config.Endpoint{
			{
				Name:   "baz",
				Weight: 1,
			},
		},
		EndpointsFailover: []config.Endpoint{
			{
				Name:   "qux",
				Weight: 1,
			},
		},
	}

	validator, err := newEndpointProvider(logger, validatorEndpointGroup, validatorEndpointGroupName)
	require.NoError(err)

	mgr := NewFailoverManager(FailoverManagerParams{
		Proxy:     proxy,
		Validator: validator,
	})

	ctx := context.Background()
	_, err = mgr.WithFailoverContext(ctx)
	require.Error(err)
	require.True(xerrors.Is(err, ErrFailoverUnavailable))
}

func TestFailoverManager_ValidatorUnavailable(t *testing.T) {
	require := testutil.Require(t)
	logger := zaptest.NewLogger(t)

	proxyEndpointGroup := &config.EndpointGroup{
		Endpoints: []config.Endpoint{
			{
				Name:   "foo",
				Weight: 1,
			},
		},
		EndpointsFailover: []config.Endpoint{
			{
				Name:   "bar",
				Weight: 1,
			},
		},
	}

	proxy, err := newEndpointProvider(logger, proxyEndpointGroup, proxyEndpointGroupName)
	require.NoError(err)

	validatorEndpointGroup := &config.EndpointGroup{
		Endpoints: []config.Endpoint{
			{
				Name:   "baz",
				Weight: 1,
			},
		},
	}

	validator, err := newEndpointProvider(logger, validatorEndpointGroup, validatorEndpointGroupName)
	require.NoError(err)

	mgr := NewFailoverManager(FailoverManagerParams{
		Proxy:     proxy,
		Validator: validator,
	})

	ctx := context.Background()
	_, err = mgr.WithFailoverContext(ctx)
	require.Error(err)
	require.True(xerrors.Is(err, ErrFailoverUnavailable))
}
