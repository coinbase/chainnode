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

type traceByNumberStorageTestSuite struct {
	suite.Suite
	storage TraceByNumberStorage
	cfg     *config.Config
	app     testapp.TestApp
}

func (s *traceByNumberStorageTestSuite) SetupTest() {
	require := testutil.Require(s.T())

	var storage TraceByNumberStorage
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

func (s *traceByNumberStorageTestSuite) TearDownTest() {
	if s.app != nil {
		s.app.Close()
	}
}

func TestIntegrationTraceByNumberStorageTestSuite(t *testing.T) {
	suite.Run(t, new(traceByNumberStorageTestSuite))
}

func (s *traceByNumberStorageTestSuite) TestPersistAndGetTrace_UploadNotEnforced() {
	s.testPersistAndGetTrace(false)
}

func (s *traceByNumberStorageTestSuite) TestPersistAndGetTrace_UploadEnforced() {
	s.testPersistAndGetTrace(true)
}

func (s *traceByNumberStorageTestSuite) testPersistAndGetTrace(uploadEnforced bool) {
	require := testutil.Require(s.T())

	s.cfg.Storage.TraceUploadEnforced = uploadEnforced

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
	err = s.storage.PersistTraceByNumber(ctx, expectedFirstTrace)
	require.NoError(err)

	persisted, err := s.storage.GetTraceByNumber(ctx, tag, height, sequence)
	require.NoError(err)
	require.False(persisted.UpdatedAt.IsZero())
	expectedFirstTrace.UpdatedAt = persisted.UpdatedAt
	require.Equal(expectedFirstTrace, persisted)

	// use a larger sequence
	persisted, err = s.storage.GetTraceByNumber(ctx, tag, height, sequence+1)
	require.NoError(err)
	require.Equal(expectedFirstTrace, persisted)

	// second attempt
	sequence = sequence + 100000000
	hash = "updated_hash"
	data = []byte("attempt 2")
	expectedSecondTrace, err := testutil.MakeEthereumTrace(tag, sequence, height, hash, data, blockTime)
	require.NoError(err)
	err = s.storage.PersistTraceByNumber(ctx, expectedSecondTrace)
	require.NoError(err)

	persisted, err = s.storage.GetTraceByNumber(ctx, tag, height, sequence)
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
	err = s.storage.PersistTraceByNumber(ctx, expectedThirdTrace)
	require.NoError(err)

	read, err := s.storage.GetTraceByNumber(ctx, tag, height, sequence)
	require.NoError(err)
	require.Equal(expectedSecondTrace, read)
}

func (s *traceByNumberStorageTestSuite) TestPersistAndGetTrace_LargeFile() {
	require := testutil.Require(s.T())

	// override the max data size
	s.cfg.AWS.DynamoDB.MaxDataSize = 1
	s.cfg.Storage.TraceUploadEnforced = false

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
	err = s.storage.PersistTraceByNumber(ctx, expectedFirstTrace)
	require.NoError(err)

	persisted, err := s.storage.GetTraceByNumber(ctx, tag, height, sequence)
	require.NoError(err)
	require.False(persisted.UpdatedAt.IsZero())
	expectedFirstTrace.UpdatedAt = persisted.UpdatedAt
	require.Equal(expectedFirstTrace, persisted)
}

func (s *traceByNumberStorageTestSuite) TestGetTrace_NotExists() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	tag := uint32(2)
	sequence := api.Sequence(12312)
	height := uint64(123456)
	trace, err := s.storage.GetTraceByNumber(ctx, tag, height, sequence)
	require.Error(err)
	require.True(xerrors.Is(err, internal.ErrItemNotFound))
	require.Nil(trace)
}

func (s *traceByNumberStorageTestSuite) TestPersistTrace_NilTrace() {
	require := testutil.Require(s.T())

	err := s.storage.PersistTraceByNumber(context.Background(), nil)
	require.Error(err)
}
