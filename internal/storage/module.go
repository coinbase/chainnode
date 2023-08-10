package storage

import (
	"go.uber.org/fx"

	"github.com/coinbase/chainnode/internal/storage/blob"

	"github.com/coinbase/chainnode/internal/storage/checkpoint"
	"github.com/coinbase/chainnode/internal/storage/collection"
	"github.com/coinbase/chainnode/internal/storage/ethereum"
	"github.com/coinbase/chainnode/internal/storage/internal"
	"github.com/coinbase/chainnode/internal/storage/s3"
)

type (
	CheckpointStorage = checkpoint.CheckpointStorage
	EthereumStorage   = ethereum.Storage
)

var (
	Module = fx.Options(
		blob.Module,
		checkpoint.Module,
		collection.Module,
		ethereum.Module,
		s3.Module,
	)

	ErrItemNotFound    = internal.ErrItemNotFound
	ErrRequestCanceled = internal.ErrRequestCanceled
)

func WithEmptyStorage() fx.Option {
	return fx.Options(
		collection.WithEmptyTable(),
		s3.WithEmptyClient(),
	)
}
