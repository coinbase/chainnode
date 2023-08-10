package internal

import (
	"context"
	"encoding/json"
)

type PreHandler interface {
	PrepareContext(ctx context.Context, request json.RawMessage) (context.Context, error)
}
