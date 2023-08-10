package blob

import (
	"go.uber.org/fx"
)

var Module = fx.Provide(
	NewBlobStorage,
)
