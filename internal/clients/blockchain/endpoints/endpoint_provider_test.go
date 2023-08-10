package endpoints

import (
	"context"
	"fmt"
	"math"
	"sort"
	"testing"

	"go.uber.org/zap/zaptest"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainnode/internal/config"
	"github.com/coinbase/chainnode/internal/utils/testutil"
)

const (
	largeNumPicks = 100000
	smallNumPicks = 10
)

func TestEndpointProvider(t *testing.T) {
	require := testutil.Require(t)

	logger := zaptest.NewLogger(t)
	endpoints := make([]config.Endpoint, 5)
	totalWeights := uint32(0)
	ctx := context.Background()
	for i := 0; i < len(endpoints); i++ {
		endpoint := config.Endpoint{
			Url:    fmt.Sprintf("url%d", i),
			Weight: ^uint8(0) - uint8(i*2),
		}
		totalWeights += uint32(endpoint.Weight)
		endpoints[i] = endpoint
	}

	ep, err := newEndpointProvider(logger, &config.EndpointGroup{
		Endpoints: endpoints,
	},
		"foo",
	)
	require.NoError(err)
	allEndpoints := ep.GetAllEndpoints()
	require.Equal(len(endpoints), len(allEndpoints))

	pickStats := make(map[string]int)
	for i := 0; i < largeNumPicks; i++ {
		pick, err := ep.GetEndpoint(context.TODO())
		require.NoError(err)
		pickStats[pick.Url] += 1
	}
	for _, endpoint := range endpoints {
		expectedPickProbability := float64(endpoint.Weight) / float64(totalWeights)
		actualPickedProbability := float64(pickStats[endpoint.Url]) * 1.0 / float64(largeNumPicks)
		require.True(
			math.Abs(expectedPickProbability-actualPickedProbability) < 0.01,
			"endpoint %v: expectedPickProbability:%3f, actualPickedProbability:%3f",
			endpoint,
			expectedPickProbability,
			actualPickedProbability,
		)
	}

	require.False(ep.FailoverEnabled(ctx))
}

func TestEndpointProvider_WithFailoverContext(t *testing.T) {
	require := testutil.Require(t)
	logger := zaptest.NewLogger(t)
	ctx := context.Background()

	endpointGroup := &config.EndpointGroup{
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

	ep, err := newEndpointProvider(logger, endpointGroup, "proxy")
	require.NoError(err)

	allEndpoints := ep.GetAllEndpoints()
	require.Equal(2, len(allEndpoints))
	require.Equal("foo", allEndpoints[0].Name)
	require.Equal("bar", allEndpoints[1].Name)

	require.Equal([]string{"foo"}, getActiveEndpoints(ctx, ep))
	for i := 0; i < smallNumPicks; i++ {
		endpoint, err := ep.GetEndpoint(ctx)
		require.NoError(err)
		require.Equal("foo", endpoint.Name)
	}
	require.False(ep.FailoverEnabled(context.Background()))

	ctx, err = ep.WithFailoverContext(ctx)
	require.Equal([]string{"bar"}, getActiveEndpoints(ctx, ep))
	require.NoError(err)
	for i := 0; i < smallNumPicks; i++ {
		endpoint, err := ep.GetEndpoint(ctx)
		require.NoError(err)
		require.Equal("bar", endpoint.Name)
	}
	require.True(ep.FailoverEnabled(ctx))
}

func TestEndpointProviderEmptyEndpoints(t *testing.T) {
	require := testutil.Require(t)
	logger := zaptest.NewLogger(t)
	ep, err := newEndpointProvider(logger, &config.EndpointGroup{}, "empty")
	require.NoError(err)
	_, err = ep.GetEndpoint(context.Background())
	require.Error(err)
	ctx := context.Background()

	ep, err = newEndpointProvider(logger, &config.EndpointGroup{
		Endpoints: []config.Endpoint{
			{
				Name:   "foo",
				Weight: 1,
			},
		},
		UseFailover: true,
	}, "proxy")
	require.NoError(err)
	require.Equal([]string{}, getActiveEndpoints(ctx, ep))
	_, err = ep.GetEndpoint(ctx)
	require.Error(err)

	ep, err = newEndpointProvider(logger, &config.EndpointGroup{
		Endpoints: []config.Endpoint{
			{
				Name:   "foo",
				Weight: 1,
			},
		},
	}, "proxy")
	require.NoError(err)
	_, err = ep.GetEndpoint(ctx)
	require.NoError(err)
	_, err = ep.WithFailoverContext(ctx)
	require.Equal([]string{"foo"}, getActiveEndpoints(ctx, ep))
	require.Error(err)
	require.True(xerrors.Is(err, ErrFailoverUnavailable))
}

func TestEndpointProviderUseFailover(t *testing.T) {
	require := testutil.Require(t)
	logger := zaptest.NewLogger(t)
	ctx := context.Background()
	ep, err := newEndpointProvider(logger, &config.EndpointGroup{
		Endpoints: []config.Endpoint{
			{
				Name:   "primary",
				Url:    "https://primary",
				Weight: 1,
			},
		},
		EndpointsFailover: []config.Endpoint{
			{
				Name:   "failover",
				Url:    "https://failover",
				Weight: 1,
			},
		},
		UseFailover: true,
	},
		"foo",
	)
	require.NoError(err)
	require.True(ep.FailoverEnabled(ctx))
	require.Equal([]*config.Endpoint{
		{
			Name:   "failover",
			Url:    "https://failover",
			Weight: 1,
		},
		{
			Name:   "primary",
			Url:    "https://primary",
			Weight: 1,
		},
	}, ep.GetAllEndpoints())
	require.Equal([]*config.Endpoint{
		{
			Name:   "failover",
			Url:    "https://failover",
			Weight: 1,
		},
	}, ep.GetActiveEndpoints(context.Background()))

	ctx, err = ep.WithFailoverContext(ctx)
	require.NoError(err)
	require.True(ep.HasFailoverContext(ctx))
	require.False(ep.FailoverEnabled(ctx))
}

func getActiveEndpoints(ctx context.Context, ep EndpointProvider) []string {
	endpoints := ep.GetActiveEndpoints(ctx)
	res := make([]string, len(endpoints))
	for i, endpoint := range endpoints {
		res[i] = endpoint.Name
	}

	sort.Strings(res)
	return res
}
