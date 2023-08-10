package collection

import (
	"go.uber.org/fx"
)

type (
	emptyTableOption struct{}

	emptyDDBTable struct {
		ddbTable
	}
)

var _ ddbTable = (*emptyDDBTable)(nil)

// WithEmptyTable injects an empty implementation of the ddbTable interface.
// You may use this option when the storage is not referenced in your test case.
func WithEmptyTable() fx.Option {
	return fx.Provide(func() *emptyTableOption { return &emptyTableOption{} })
}
