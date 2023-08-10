package activity

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/testsuite"
	"go.uber.org/fx"

	"github.com/coinbase/chainnode/internal/api"
	xapi "github.com/coinbase/chainnode/internal/api/ethereum"
	"github.com/coinbase/chainnode/internal/cadence"
	"github.com/coinbase/chainnode/internal/controller"
	controllermocks "github.com/coinbase/chainnode/internal/controller/mocks"
	"github.com/coinbase/chainnode/internal/utils/testapp"
	"github.com/coinbase/chainnode/internal/utils/testutil"
)

type CheckpointReaderTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
	env              *cadence.TestEnv
	ctrl             *gomock.Controller
	checkpointer     *controllermocks.MockCheckpointer
	controller       *controllermocks.MockController
	checkpointReader *CheckpointReader
	app              testapp.TestApp
}

func TestCheckpointReaderTestSuite(t *testing.T) {
	suite.Run(t, new(CheckpointReaderTestSuite))
}

func (s *CheckpointReaderTestSuite) SetupTest() {
	s.env = cadence.NewTestActivityEnv(s)
	s.ctrl = gomock.NewController(s.T())
	s.checkpointer = controllermocks.NewMockCheckpointer(s.ctrl)
	s.controller = controllermocks.NewMockController(s.ctrl)
	s.controller.EXPECT().Checkpointer().Return(s.checkpointer)
	s.app = testapp.New(
		s.T(),
		Module,
		cadence.WithTestEnv(s.env),
		fx.Provide(func() controller.Controller { return s.controller }),
		fx.Populate(&s.checkpointReader),
	)
}

func (s *CheckpointReaderTestSuite) TearDownTest() {
	s.app.Close()
	s.ctrl.Finish()
	s.env.AssertExpectations(s.T())
}

func (s *CheckpointReaderTestSuite) TestHappyCase() {
	const (
		collection = xapi.CollectionBlocks
		tag        = uint32(1)
		sequence   = api.Sequence(10100)
		height     = uint64(10_000)
	)

	require := testutil.Require(s.T())
	checkpoint := &api.Checkpoint{
		Collection: collection,
		Tag:        tag,
		Sequence:   sequence,
		Height:     height,
	}
	s.checkpointer.EXPECT().
		Get(gomock.Any(), collection, tag).Return(checkpoint, nil)

	ctx := s.env.BackgroundContext()
	response, err := s.checkpointReader.Execute(ctx, &CheckpointReaderRequest{
		Tag:        tag,
		Collection: collection,
	})
	require.NoError(err)
	require.Equal(checkpoint, response.Checkpoint)
}
