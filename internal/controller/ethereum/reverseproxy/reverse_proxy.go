package reverseproxy

import (
	"net/http"
	"net/url"

	"go.uber.org/fx"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainnode/internal/clients/blockchain/endpoints"
	"github.com/coinbase/chainnode/internal/controller/internal"
	"github.com/coinbase/chainnode/internal/utils/fxparams"

	"github.com/coinbase/chainstorage/sdk/services"
)

type (
	Params struct {
		fx.In
		fxparams.Params
		Manager          services.SystemManager
		EndpointProvider endpoints.EndpointProvider `name:"proxy"`
	}
)

var (
	requestHeader = http.Header{
		"Content-Type": []string{"application/json"},
	}
)

func NewReverseProxies(params Params) ([]internal.ReverseProxy, error) {
	cfgs := params.Config.Controller.ReverseProxy
	if len(cfgs) == 0 {
		return nil, nil
	}

	endpoint, err := params.EndpointProvider.GetEndpoint(params.Manager.ServiceContext())
	if err != nil {
		// Ignore this error in the unit test.
		if params.Config.IsUnitTest() {
			return nil, nil
		}

		return nil, xerrors.Errorf("failed to get endpoint: %w", err)
	}

	reverseProxies := make([]internal.ReverseProxy, len(cfgs))
	for i, cfg := range cfgs {
		base, err := url.Parse(endpoint.Url)
		if err != nil {
			return nil, xerrors.Errorf("failed to parse base url: %w", err)
		}

		ref, err := url.Parse(cfg.Target)
		if err != nil {
			return nil, xerrors.Errorf("failed to parse ref url: %w", err)
		}

		// TODO: use url.JoinPath instead of url.ResolveReference after upgrading go to 1.19.
		target := base.ResolveReference(ref)

		reverseProxy, err := internal.NewReverseProxy(cfg.Path, target.String(), requestHeader)
		if err != nil {
			return nil, xerrors.Errorf("failed to create reverse proxy: %w", err)
		}

		reverseProxies[i] = reverseProxy
	}

	return reverseProxies, nil
}
