package ethereum

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainnode/internal/api"
	"github.com/coinbase/chainnode/internal/config"
	"github.com/coinbase/chainnode/internal/storage/blob"
	"github.com/coinbase/chainnode/internal/storage/collection"
	"github.com/coinbase/chainnode/internal/storage/internal"
	"github.com/coinbase/chainnode/internal/storage/s3"
	"github.com/coinbase/chainnode/internal/utils/testapp"
	"github.com/coinbase/chainnode/internal/utils/testutil"
)

type arbtraceBlockStorageTestSuite struct {
	suite.Suite
	storage ArbtraceBlockStorage
	cfg     *config.Config
	app     testapp.TestApp
}

func (s *arbtraceBlockStorageTestSuite) SetupTest() {
	require := testutil.Require(s.T())

	var storage ArbtraceBlockStorage
	cfg, err := config.New()
	require.NoError(err)
	s.cfg = cfg

	s.app = testapp.New(
		s.T(),
		testapp.WithIntegration(),
		Module,
		blob.Module,
		collection.Module,
		s3.Module,
		testapp.WithConfig(s.cfg),
		fx.Populate(&storage),
	)
	s.storage = storage
}

func (s *arbtraceBlockStorageTestSuite) TearDownTest() {
	if s.app != nil {
		s.app.Close()
	}
}

func TestIntegrationArbtraceBlockStorageTestSuite(t *testing.T) {
	suite.Run(t, new(arbtraceBlockStorageTestSuite))
}

func (s *arbtraceBlockStorageTestSuite) TestPersistAndGetTrace() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	tag := uint32(1)
	sequence := api.Sequence(12312)
	height := uint64(123456)
	hash := "0x12121"
	data := []byte("attempt 1")
	blockTime := "2020-11-24T16:07:21Z"

	// first attempt
	expectedFirstTrace, err := testutil.MakeEthereumTrace(tag, sequence, height, hash, data, blockTime)
	require.NoError(err)
	err = s.storage.PersistArbtraceBlock(ctx, expectedFirstTrace)
	require.NoError(err)

	persisted, err := s.storage.GetArbtraceBlock(ctx, tag, height, sequence)
	require.NoError(err)
	require.False(persisted.UpdatedAt.IsZero())
	expectedFirstTrace.UpdatedAt = persisted.UpdatedAt
	require.Equal(expectedFirstTrace, persisted)

	// use a larger sequence
	persisted, err = s.storage.GetArbtraceBlock(ctx, tag, height, sequence+1)
	require.NoError(err)
	require.Equal(expectedFirstTrace, persisted)

	// second attempt
	sequence = sequence + 100000000
	hash = "updated_hash"
	data = []byte("attempt 2")
	expectedSecondTrace, err := testutil.MakeEthereumTrace(tag, sequence, height, hash, data, blockTime)
	require.NoError(err)
	err = s.storage.PersistArbtraceBlock(ctx, expectedSecondTrace)
	require.NoError(err)

	persisted, err = s.storage.GetArbtraceBlock(ctx, tag, height, sequence)
	require.NoError(err)
	require.False(persisted.UpdatedAt.IsZero())
	expectedSecondTrace.UpdatedAt = persisted.UpdatedAt
	require.Equal(expectedSecondTrace, persisted)

	// third write attempt with old sequence
	oldSequence := sequence - 10000100
	oldHash := "updated_hash_2"
	oldData := []byte("attempt 3")
	oldBlockTime := "2020-11-22T14:07:21Z"
	expectedThirdTrace, err := testutil.MakeEthereumTrace(tag, oldSequence, height, oldHash, oldData, oldBlockTime)
	require.NoError(err)
	err = s.storage.PersistArbtraceBlock(ctx, expectedThirdTrace)
	require.NoError(err)

	read, err := s.storage.GetArbtraceBlock(ctx, tag, height, sequence)
	require.NoError(err)
	require.Equal(expectedSecondTrace, read)
}

func (s *arbtraceBlockStorageTestSuite) TestPersistAndGetTrace_LargeFile() {
	require := testutil.Require(s.T())

	// override the max data size
	s.cfg.AWS.DynamoDB.MaxDataSize = 1

	ctx := context.Background()
	tag := uint32(1)
	sequence := api.Sequence(12312)
	height := uint64(123456)
	hash := "0x12121"
	data := testutil.MakeFile(10)
	blockTime := "2020-11-24T16:07:21Z"

	// override the max data size
	s.cfg.AWS.DynamoDB.MaxDataSize = 1

	expectedFirstTrace, err := testutil.MakeEthereumTrace(tag, sequence, height, hash, data, blockTime)
	require.NoError(err)
	err = s.storage.PersistArbtraceBlock(ctx, expectedFirstTrace)
	require.NoError(err)

	persisted, err := s.storage.GetArbtraceBlock(ctx, tag, height, sequence)
	require.NoError(err)
	require.False(persisted.UpdatedAt.IsZero())
	expectedFirstTrace.UpdatedAt = persisted.UpdatedAt
	require.Equal(expectedFirstTrace, persisted)
}

func (s *arbtraceBlockStorageTestSuite) TestGetTrace_NotExists() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	tag := uint32(2)
	sequence := api.Sequence(12312)
	height := uint64(123456)
	trace, err := s.storage.GetArbtraceBlock(ctx, tag, height, sequence)
	require.Error(err)
	require.True(xerrors.Is(err, internal.ErrItemNotFound))
	require.Nil(trace)
}

func (s *arbtraceBlockStorageTestSuite) TestPersistTrace_NilTrace() {
	require := testutil.Require(s.T())

	err := s.storage.PersistArbtraceBlock(context.Background(), nil)
	require.Error(err)
}
