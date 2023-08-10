package ethereum

import (
	"fmt"
	"testing"

	"go.temporal.io/sdk/testsuite"
	"go.uber.org/fx"

	xapi "github.com/coinbase/chainnode/internal/api/ethereum"
	"github.com/coinbase/chainnode/internal/cadence"
	"github.com/coinbase/chainnode/internal/clients"
	"github.com/coinbase/chainnode/internal/controller/internal"
	"github.com/coinbase/chainnode/internal/storage"
	"github.com/coinbase/chainnode/internal/utils/testapp"
	"github.com/coinbase/chainnode/internal/utils/testutil"
)

type (
	emptyCheckpointer struct {
		internal.Checkpointer
	}

	EthereumControllerTestSuite struct {
		testsuite.WorkflowTestSuite
		t *testing.T
	}
)

func (s EthereumControllerTestSuite) T() *testing.T {
	return s.t
}

func TestController(t *testing.T) {
	require := testutil.Require(t)
	var deps struct {
		fx.In
		Controller internal.Controller `name:"ethereum"`
	}
	ts := &EthereumControllerTestSuite{t: t}
	env := cadence.NewTestEnv(ts)
	testapp.New(
		t,
		Module,
		clients.Module,
		storage.Module,
		storage.WithEmptyStorage(),
		cadence.WithTestEnv(env),
		fx.Provide(func() internal.Checkpointer { return emptyCheckpointer{} }),
		fx.Populate(&deps),
	)
	require.NotNil(deps.Controller)
	require.NotNil(deps.Controller.Handler())
	require.NotNil(deps.Controller.Checkpointer())
	indexers := make(map[internal.Indexer]bool)
	for _, collection := range xapi.ParentCollections {
		indexer, err := deps.Controller.Indexer(collection)
		require.NoError(err)
		require.NotNil(indexer)
		require.False(indexers[indexer], fmt.Sprintf("duplicate indexer for %v", collection))
		indexers[indexer] = true
	}

	for _, collection := range xapi.ChildCollections {
		indexer, err := deps.Controller.Indexer(collection)
		require.Error(err)
		require.Nil(indexer)
		require.Contains(err.Error(), "not implemented")

		parentCollection, ok := xapi.ChildToParentCollectionMap[collection]
		require.True(ok)
		require.NotNil(parentCollection)

		parentIndexer, err := deps.Controller.Indexer(parentCollection)
		require.True(indexers[parentIndexer])
	}

	require.Equal(len(xapi.ChildCollections), len(xapi.ChildToParentCollectionMap))
}
