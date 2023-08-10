package endpoints

import (
	"context"
	"math/rand"
	"net/http"

	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainnode/internal/config"
	"github.com/coinbase/chainnode/internal/utils/log"
	"github.com/coinbase/chainnode/internal/utils/picker"
)

type (
	// EndpointProvider encapsulates the client-side routing logic.
	// By default, the request is routed to one of the primary endpoints based on their weights.
	// When the failover context is seen, the request is instead routed to one of the secondary endpoints.
	// In most cases, the primary endpoints are referring to the `EndpointGroup.Endpoints` config,
	// while the secondary endpoints are referring to the `EndpointGroup.EndpointsFailover` config.
	// But when `EndpointGroup.UseFailover` is turned on, the two list are swapped.
	EndpointProvider interface {
		GetEndpoint(ctx context.Context) (*config.Endpoint, error)
		GetAllEndpoints() []*config.Endpoint
		NewHTTPClient() (*http.Client, error)
		GetActiveEndpoints(ctx context.Context) []*config.Endpoint
		// WithFailoverContext automatic failover to the current secondary cluster
		WithFailoverContext(ctx context.Context) (context.Context, error)
		HasFailoverContext(ctx context.Context) bool
		// FailoverEnabled whether the failover_endpoints defined in config are in use
		FailoverEnabled(ctx context.Context) bool
	}

	EndpointProviderParams struct {
		fx.In
		Config *config.Config
		Logger *zap.Logger
	}

	EndpointProviderResult struct {
		fx.Out
		Server    EndpointProvider `name:"server"`
		Proxy     EndpointProvider `name:"proxy"`
		Validator EndpointProvider `name:"validator"`
	}

	endpointProvider struct {
		name            string
		logger          *zap.Logger
		failoverEnabled bool

		primaryPicker      picker.Picker
		primaryEndpoints   []*config.Endpoint
		secondaryPicker    picker.Picker
		secondaryEndpoints []*config.Endpoint
	}

	contextKey string
)

const (
	serverEndpointGroupName    = "server"
	proxyEndpointGroupName     = "proxy"
	validatorEndpointGroupName = "validator"
	contextKeyFailover         = "failover:"
)

var (
	ErrFailoverUnavailable = xerrors.New("no endpoint is available for failover")
)

func NewEndpointProvider(params EndpointProviderParams) (EndpointProviderResult, error) {
	serverEndpointGroup := &config.EndpointGroup{
		Endpoints: []config.Endpoint{
			{
				Name:   params.Config.Chain.Client.ServerName,
				Url:    params.Config.Chain.Client.ServerAddress + params.Config.Chain.Client.ServerHandle,
				Weight: 1,
			},
		},
	}
	endpointProviderServer, err := newEndpointProvider(params.Logger, serverEndpointGroup, serverEndpointGroupName)
	if err != nil {
		return EndpointProviderResult{}, xerrors.Errorf("failed to create endpoint provider: %w", err)
	}

	endpointProviderProxy, err := newEndpointProvider(params.Logger, &params.Config.Chain.Client.Primary, proxyEndpointGroupName)
	if err != nil {
		return EndpointProviderResult{}, xerrors.Errorf("failed to create endpoint provider: %w", err)
	}

	endpointProviderValidator, err := newEndpointProvider(params.Logger, &params.Config.Chain.Client.Validator, validatorEndpointGroupName)
	if err != nil {
		return EndpointProviderResult{}, xerrors.Errorf("failed to create endpoint provider: %w", err)
	}

	return EndpointProviderResult{
		Server:    endpointProviderServer,
		Proxy:     endpointProviderProxy,
		Validator: endpointProviderValidator,
	}, nil
}

func newEndpointProvider(logger *zap.Logger, endpointGroup *config.EndpointGroup, name string) (EndpointProvider, error) {
	primaryEndpoints, primaryPicker, err := newEndpoints(endpointGroup.Endpoints)
	if err != nil {
		return nil, xerrors.Errorf("failed to create primary endpoint provider: %w", err)
	}

	secondaryEndpoints, secondaryPicker, err := newEndpoints(endpointGroup.EndpointsFailover)
	if err != nil {
		return nil, xerrors.Errorf("failed to create secondary endpoint provider: %w", err)
	}

	failoverEnabled := false
	if endpointGroup.UseFailover {
		logger.Warn("using failover endpoints")
		failoverEnabled = true
		primaryEndpoints, secondaryEndpoints = secondaryEndpoints, primaryEndpoints
		primaryPicker, secondaryPicker = secondaryPicker, primaryPicker
	}

	if len(primaryEndpoints) == 0 {
		return &endpointProvider{}, nil
	}

	return &endpointProvider{
		name:               name,
		logger:             log.WithPackage(logger),
		failoverEnabled:    failoverEnabled,
		primaryPicker:      primaryPicker,
		primaryEndpoints:   primaryEndpoints,
		secondaryPicker:    secondaryPicker,
		secondaryEndpoints: secondaryEndpoints,
	}, nil
}

func newEndpoints(
	endpoints []config.Endpoint,
) ([]*config.Endpoint, picker.Picker, error) {
	// Shuffle the endpoints to prevent overloading the first endpoint right after a deployment.
	shuffledEndpoints := make([]*config.Endpoint, len(endpoints))
	for i := range endpoints {
		shuffledEndpoints[i] = &endpoints[i]
	}
	rand.Shuffle(len(shuffledEndpoints), func(i, j int) {
		shuffledEndpoints[i], shuffledEndpoints[j] = shuffledEndpoints[j], shuffledEndpoints[i]
	})

	choices := make([]*picker.Choice, len(shuffledEndpoints))
	for i, endpoint := range shuffledEndpoints {
		choices[i] = &picker.Choice{
			Item:   endpoint,
			Weight: int(endpoint.Weight),
		}
	}
	picker := picker.New(choices)
	return shuffledEndpoints, picker, nil
}

func (e *endpointProvider) NewHTTPClient() (*http.Client, error) {
	return newHTTPClient()
}

func (e *endpointProvider) GetEndpoint(ctx context.Context) (*config.Endpoint, error) {
	activeEndpoints, activePicker := e.getActiveEndpoints(ctx)
	if len(activeEndpoints) == 0 {
		return nil, xerrors.New("no endpoint is available")
	}

	pick := activePicker.Next().(*config.Endpoint)
	return pick, nil
}

func (e *endpointProvider) GetAllEndpoints() []*config.Endpoint {
	return append(e.primaryEndpoints, e.secondaryEndpoints...)
}

func (e *endpointProvider) GetActiveEndpoints(ctx context.Context) []*config.Endpoint {
	endpoints, _ := e.getActiveEndpoints(ctx)
	return endpoints
}

func (e *endpointProvider) FailoverEnabled(ctx context.Context) bool {
	return (e.failoverEnabled && !e.HasFailoverContext(ctx)) ||
		(!e.failoverEnabled && e.HasFailoverContext(ctx))
}

func (e *endpointProvider) WithFailoverContext(ctx context.Context) (context.Context, error) {
	if len(e.secondaryEndpoints) == 0 {
		return nil, ErrFailoverUnavailable
	}

	return context.WithValue(ctx, e.getFailoverContextKey(), struct{}{}), nil
}

func (e *endpointProvider) getActiveEndpoints(ctx context.Context) ([]*config.Endpoint, picker.Picker) {
	activeEndpoints := e.primaryEndpoints
	activePicker := e.primaryPicker
	if e.HasFailoverContext(ctx) {
		activeEndpoints = e.secondaryEndpoints
		activePicker = e.secondaryPicker
	}

	return activeEndpoints, activePicker
}

func (e *endpointProvider) HasFailoverContext(ctx context.Context) bool {
	return ctx.Value(e.getFailoverContextKey()) != nil
}

func (e *endpointProvider) getFailoverContextKey() contextKey {
	return contextKey(contextKeyFailover + e.name)
}
