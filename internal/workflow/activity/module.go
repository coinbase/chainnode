package activity

import (
	"go.uber.org/fx"
)

var Module = fx.Options(
	fx.Provide(NewCheckpointReader),
	fx.Provide(NewCheckpointWriter),
	fx.Provide(NewCheckpointSynchronizer),
	fx.Provide(NewStreamer),
	fx.Provide(NewTransformer),
)
