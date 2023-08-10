package workflow

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/fx"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainnode/internal/api"
	"github.com/coinbase/chainnode/internal/cadence"
	"github.com/coinbase/chainnode/internal/clients"
	"github.com/coinbase/chainnode/internal/config"
	"github.com/coinbase/chainnode/internal/controller"
	"github.com/coinbase/chainnode/internal/storage"
	"github.com/coinbase/chainnode/internal/utils/testapp"
	"github.com/coinbase/chainnode/internal/utils/testutil"
	"github.com/coinbase/chainnode/internal/workflow/activity"
)

const (
	ingestorWorkflow = "workflow.ingestor"

	coordinatorMetricSequence = int64(123)
	coordinatorMetricHeight   = uint64(234)
	coordinatorMetricGap      = int64(2)
)

type CoordinatorTestSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite
	env         *cadence.TestEnv
	ctrl        *gomock.Controller
	coordinator *Coordinator
	app         testapp.TestApp
	collections []api.Collection
}

func TestCoordinatorTestSuite(t *testing.T) {
	suite.Run(t, new(CoordinatorTestSuite))
}

func (s *CoordinatorTestSuite) SetupTest() {
	require := testutil.Require(s.T())

	// Override config to speed up the test.
	cfg, err := config.New()
	require.NoError(err)
	cfg.Workflows.Coordinator.CheckpointInterval = time.Millisecond * 500
	cfg.Workflows.Coordinator.SynchronizerInterval = time.Millisecond * 100
	s.env = cadence.NewTestEnv(s)
	s.ctrl = gomock.NewController(s.T())
	s.app = testapp.New(
		s.T(),
		Module,
		clients.Module,
		controller.Module,
		storage.Module,
		storage.WithEmptyStorage(),
		testapp.WithConfig(cfg),
		cadence.WithTestEnv(s.env),
		fx.Populate(&s.coordinator),
	)
	collections := make([]api.Collection, 0)
	for _, ingestor := range cfg.Workflows.Coordinator.Ingestors {
		if ingestor.Synchronized {
			collections = append(collections, api.Collection(ingestor.Collection))
		}
	}
	s.collections = collections
}

func (s *CoordinatorTestSuite) TearDownTest() {
	s.app.Close()
	s.ctrl.Finish()
	s.env.AssertExpectations(s.T())
}

func (s *CoordinatorTestSuite) TestHappyCase() {
	require := testutil.Require(s.T())

	cfg := s.app.Config()
	eventTag := uint32(3)
	cfg.Workflows.Coordinator.EventTag = eventTag
	tag := cfg.Tag.Stable
	for _, ingestor := range cfg.Workflows.Coordinator.Ingestors {
		s.env.OnWorkflow(ingestorWorkflow, mock.Anything, &IngestorRequest{
			Collection:     api.Collection(ingestor.Collection),
			Tag:            tag,
			EventTag:       eventTag,
			CheckpointSize: ingestor.CheckpointSize,
			BatchSize:      ingestor.BatchSize,
			MiniBatchSize:  ingestor.MiniBatchSize,
			Parallelism:    ingestor.Parallelism,
		}).Return(func(ctx workflow.Context, request *IngestorRequest) error {
			// Block until the child workflow is canceled.
			ctx.Done().Receive(ctx, nil)
			return nil
		})
	}

	backoff := cfg.Workflows.Coordinator.SynchronizerInterval
	s.env.OnActivity(activity.ActivityCheckpointSynchronizer, mock.Anything, &activity.CheckpointSynchronizerRequest{
		Tag:         tag,
		Collections: s.collections,
		Backoff:     backoff,
		EventTag:    eventTag,
	}).Run(func(args mock.Arguments) {
		time.Sleep(backoff)
		s.env.SetStartTime(s.env.Now().Add(backoff))
	}).Return(&activity.CheckpointSynchronizerResponse{
		Sequence: coordinatorMetricSequence,
		Height:   coordinatorMetricHeight,
		Gap:      coordinatorMetricGap,
	}, nil)

	_, err := s.coordinator.Execute(context.Background(), &CoordinatorRequest{})
	require.Error(err)
	require.True(workflow.IsContinueAsNewError(err), err.Error())
}

func (s *CoordinatorTestSuite) TestResponseEmpty() {
	require := testutil.Require(s.T())

	cfg := s.app.Config()
	tag := cfg.Tag.Stable
	eventTag := uint32(3)
	cfg.Workflows.Coordinator.EventTag = eventTag
	for _, ingestor := range cfg.Workflows.Coordinator.Ingestors {
		s.env.OnWorkflow(ingestorWorkflow, mock.Anything, &IngestorRequest{
			Collection:     api.Collection(ingestor.Collection),
			Tag:            tag,
			EventTag:       eventTag,
			CheckpointSize: ingestor.CheckpointSize,
			BatchSize:      ingestor.BatchSize,
			MiniBatchSize:  ingestor.MiniBatchSize,
			Parallelism:    ingestor.Parallelism,
		}).Return(func(ctx workflow.Context, request *IngestorRequest) error {
			// Block until the child workflow is canceled.
			ctx.Done().Receive(ctx, nil)
			return nil
		})
	}

	backoff := cfg.Workflows.Coordinator.SynchronizerInterval
	s.env.OnActivity(activity.ActivityCheckpointSynchronizer, mock.Anything, &activity.CheckpointSynchronizerRequest{
		Tag:         tag,
		Collections: s.collections,
		Backoff:     backoff,
		EventTag:    eventTag,
	}).Run(func(args mock.Arguments) {
		time.Sleep(backoff)
		s.env.SetStartTime(s.env.Now().Add(backoff))
	}).Return(&activity.CheckpointSynchronizerResponse{}, nil)

	_, err := s.coordinator.Execute(context.Background(), &CoordinatorRequest{})
	require.Error(err)
	require.True(workflow.IsContinueAsNewError(err), err.Error())
}

func (s *CoordinatorTestSuite) TestChildWorkflowFailed() {
	require := testutil.Require(s.T())

	cfg := s.app.Config()
	tag := cfg.Tag.Stable
	eventTag := uint32(3)
	cfg.Workflows.Coordinator.EventTag = eventTag
	for _, ingestor := range cfg.Workflows.Coordinator.Ingestors {
		s.env.OnWorkflow(ingestorWorkflow, mock.Anything, &IngestorRequest{
			Collection:     api.Collection(ingestor.Collection),
			Tag:            tag,
			EventTag:       eventTag,
			CheckpointSize: ingestor.CheckpointSize,
			BatchSize:      ingestor.BatchSize,
			MiniBatchSize:  ingestor.MiniBatchSize,
			Parallelism:    ingestor.Parallelism,
		}).Return(func(ctx workflow.Context, request *IngestorRequest) error {
			err := workflow.Sleep(ctx, time.Millisecond*300)
			require.NoError(err)
			return xerrors.New("mock ingestor error")
		})
	}

	s.env.OnActivity(activity.ActivityCheckpointSynchronizer, mock.Anything, &activity.CheckpointSynchronizerRequest{
		Tag:         tag,
		Collections: s.collections,
		Backoff:     cfg.Workflows.Coordinator.SynchronizerInterval,
		EventTag:    eventTag,
	}).Return(&activity.CheckpointSynchronizerResponse{
		Sequence: coordinatorMetricSequence,
		Height:   coordinatorMetricHeight,
		Gap:      coordinatorMetricGap,
	}, nil)

	_, err := s.coordinator.Execute(context.Background(), &CoordinatorRequest{})
	require.Error(err)
	require.Contains(err.Error(), "mock ingestor error")
}

func (s *CoordinatorTestSuite) TestChildWorkflowAlreadyExecuted_RetryFailed() {
	require := testutil.Require(s.T())

	cfg := s.app.Config()
	tag := cfg.Tag.Stable
	eventTag := uint32(3)
	cfg.Workflows.Coordinator.EventTag = eventTag
	cfg.Workflows.Coordinator.CheckpointInterval = time.Second * 3
	cfg.Workflows.Coordinator.SynchronizerInterval = time.Second * 1
	for _, ingestor := range cfg.Workflows.Coordinator.Ingestors {
		s.env.OnWorkflow(ingestorWorkflow, mock.Anything, &IngestorRequest{
			Collection:     api.Collection(ingestor.Collection),
			Tag:            tag,
			EventTag:       eventTag,
			CheckpointSize: ingestor.CheckpointSize,
			BatchSize:      ingestor.BatchSize,
			MiniBatchSize:  ingestor.MiniBatchSize,
			Parallelism:    ingestor.Parallelism,
		}).Return(func(ctx workflow.Context, request *IngestorRequest) error {
			err := workflow.Sleep(ctx, time.Millisecond*300)
			require.NoError(err)
			return xerrors.New("workflow execution already started")
		})
	}

	s.env.OnActivity(activity.ActivityCheckpointSynchronizer, mock.Anything, &activity.CheckpointSynchronizerRequest{
		Tag:         tag,
		Collections: s.collections,
		Backoff:     cfg.Workflows.Coordinator.SynchronizerInterval,
		EventTag:    eventTag,
	}).Return(&activity.CheckpointSynchronizerResponse{
		Sequence: coordinatorMetricSequence,
		Height:   coordinatorMetricHeight,
		Gap:      coordinatorMetricGap,
	}, nil)

	_, err := s.coordinator.Execute(context.Background(), &CoordinatorRequest{})
	require.Error(err)
	require.Contains(err.Error(), "workflow execution already started")
}

func (s *CoordinatorTestSuite) TestChildWorkflowAlreadyExecuted_RetrySucceeded() {
	require := testutil.Require(s.T())

	cfg := s.app.Config()
	tag := cfg.Tag.Stable
	eventTag := uint32(3)
	cfg.Workflows.Coordinator.EventTag = eventTag
	cfg.Workflows.Coordinator.SynchronizerInterval = time.Second * 3
	for i, ingestor := range cfg.Workflows.Coordinator.Ingestors {
		if i == 0 {
			attempts := 0
			s.env.OnWorkflow(ingestorWorkflow, mock.Anything, &IngestorRequest{
				Collection:     api.Collection(ingestor.Collection),
				Tag:            tag,
				EventTag:       eventTag,
				CheckpointSize: ingestor.CheckpointSize,
				BatchSize:      ingestor.BatchSize,
				MiniBatchSize:  ingestor.MiniBatchSize,
				Parallelism:    ingestor.Parallelism,
			}).Return(func(ctx workflow.Context, request *IngestorRequest) error {
				attempts++
				if attempts == 1 {
					err := workflow.Sleep(ctx, time.Millisecond*300)
					require.NoError(err)
					return xerrors.New("workflow execution already started")
				}

				ctx.Done().Receive(ctx, nil)
				return nil
			})
		} else {
			s.env.OnWorkflow(ingestorWorkflow, mock.Anything, &IngestorRequest{
				Collection:     api.Collection(ingestor.Collection),
				Tag:            tag,
				EventTag:       eventTag,
				CheckpointSize: ingestor.CheckpointSize,
				BatchSize:      ingestor.BatchSize,
				MiniBatchSize:  ingestor.MiniBatchSize,
				Parallelism:    ingestor.Parallelism,
			}).Return(func(ctx workflow.Context, request *IngestorRequest) error {
				ctx.Done().Receive(ctx, nil)
				return nil
			})
		}
	}

	backoff := cfg.Workflows.Coordinator.SynchronizerInterval
	s.env.OnActivity(activity.ActivityCheckpointSynchronizer, mock.Anything, &activity.CheckpointSynchronizerRequest{
		Tag:         tag,
		Collections: s.collections,
		Backoff:     backoff,
		EventTag:    eventTag,
	}).Run(func(args mock.Arguments) {
		time.Sleep(backoff)
		s.env.SetStartTime(s.env.Now().Add(backoff))
	}).Return(&activity.CheckpointSynchronizerResponse{
		Sequence: coordinatorMetricSequence,
		Height:   coordinatorMetricHeight,
		Gap:      coordinatorMetricGap,
	}, nil)

	_, err := s.coordinator.Execute(context.Background(), &CoordinatorRequest{})
	require.Error(err)
	require.True(workflow.IsContinueAsNewError(err), err.Error())
}

func (s *CoordinatorTestSuite) TestChildWorkflowExitedPrematurely() {
	require := testutil.Require(s.T())

	cfg := s.app.Config()
	tag := cfg.Tag.Stable
	eventTag := uint32(3)
	cfg.Workflows.Coordinator.EventTag = eventTag
	for _, ingestor := range cfg.Workflows.Coordinator.Ingestors {
		s.env.OnWorkflow(ingestorWorkflow, mock.Anything, &IngestorRequest{
			Collection:     api.Collection(ingestor.Collection),
			Tag:            tag,
			EventTag:       eventTag,
			CheckpointSize: ingestor.CheckpointSize,
			BatchSize:      ingestor.BatchSize,
			MiniBatchSize:  ingestor.MiniBatchSize,
			Parallelism:    ingestor.Parallelism,
		}).Return(func(ctx workflow.Context, request *IngestorRequest) error {
			err := workflow.Sleep(ctx, time.Millisecond*300)
			require.NoError(err)
			return nil
		})
	}

	s.env.OnActivity(activity.ActivityCheckpointSynchronizer, mock.Anything, &activity.CheckpointSynchronizerRequest{
		Tag:         tag,
		Collections: s.collections,
		Backoff:     cfg.Workflows.Coordinator.SynchronizerInterval,
		EventTag:    eventTag,
	}).Return(&activity.CheckpointSynchronizerResponse{
		Sequence: coordinatorMetricSequence,
		Height:   coordinatorMetricHeight,
		Gap:      coordinatorMetricGap,
	}, nil)

	_, err := s.coordinator.Execute(context.Background(), &CoordinatorRequest{})
	require.Error(err)
	require.Contains(err.Error(), "child workflow exited prematurely")
}

func (s *CoordinatorTestSuite) TestSequenceOverride() {
	require := testutil.Require(s.T())

	cfg := s.app.Config()
	eventTag := uint32(3)
	cfg.Workflows.Coordinator.EventTag = eventTag
	tag := cfg.Tag.Stable
	sequence := api.Sequence(12312)
	for _, ingestor := range cfg.Workflows.Coordinator.Ingestors {
		s.env.OnWorkflow(ingestorWorkflow, mock.Anything, &IngestorRequest{
			Collection:     api.Collection(ingestor.Collection),
			Sequence:       sequence,
			Tag:            tag,
			EventTag:       eventTag,
			CheckpointSize: ingestor.CheckpointSize,
			BatchSize:      ingestor.BatchSize,
			MiniBatchSize:  ingestor.MiniBatchSize,
			Parallelism:    ingestor.Parallelism,
		}).Return(func(ctx workflow.Context, request *IngestorRequest) error {
			// Block until the child workflow is canceled.
			ctx.Done().Receive(ctx, nil)
			return nil
		})
	}

	backoff := cfg.Workflows.Coordinator.SynchronizerInterval
	s.env.OnActivity(activity.ActivityCheckpointSynchronizer, mock.Anything, &activity.CheckpointSynchronizerRequest{
		Tag:         tag,
		Collections: s.collections,
		Backoff:     backoff,
		EventTag:    eventTag,
	}).Run(func(args mock.Arguments) {
		time.Sleep(backoff)
		s.env.SetStartTime(s.env.Now().Add(backoff))
	}).Return(&activity.CheckpointSynchronizerResponse{
		Sequence: coordinatorMetricSequence,
		Height:   coordinatorMetricHeight,
		Gap:      coordinatorMetricGap,
	}, nil)

	_, err := s.coordinator.Execute(context.Background(), &CoordinatorRequest{
		Sequence: sequence,
	})
	require.Error(err)
	require.True(workflow.IsContinueAsNewError(err), err.Error())
}
