package internal

import (
	"context"

	"github.com/coinbase/chainnode/internal/api"
)

type (
	Indexer interface {
		Index(ctx context.Context, tag uint32, event *api.Event, block *api.Block) error
	}
)
