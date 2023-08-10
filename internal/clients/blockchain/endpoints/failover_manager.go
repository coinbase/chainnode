package endpoints

import (
	"context"

	"go.uber.org/fx"
	"golang.org/x/xerrors"
)

type (
	FailoverManager interface {
		WithFailoverContext(ctx context.Context) (context.Context, error)
	}

	FailoverManagerParams struct {
		fx.In
		Proxy     EndpointProvider `name:"proxy"`
		Validator EndpointProvider `name:"validator"`
	}

	failoverManager struct {
		proxy     EndpointProvider
		validator EndpointProvider
	}
)

func NewFailoverManager(params FailoverManagerParams) FailoverManager {
	return &failoverManager{
		proxy:     params.Proxy,
		validator: params.Validator,
	}
}

func (m *failoverManager) WithFailoverContext(ctx context.Context) (context.Context, error) {
	ctx, err := m.proxy.WithFailoverContext(ctx)
	if err != nil {
		return nil, xerrors.Errorf("failed to failover the proxy endpoint group: %w", err)
	}

	ctx, err = m.validator.WithFailoverContext(ctx)
	if err != nil {
		return nil, xerrors.Errorf("failed to failover the validator endpoint group: %w", err)
	}

	return ctx, nil
}
