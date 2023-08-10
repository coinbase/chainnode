package ethereum

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainnode/internal/api"
	xapi "github.com/coinbase/chainnode/internal/api/ethereum"
	"github.com/coinbase/chainnode/internal/storage/collection"
	cmocks "github.com/coinbase/chainnode/internal/storage/collection/mocks"
	"github.com/coinbase/chainnode/internal/storage/ethereum/models"
	"github.com/coinbase/chainnode/internal/utils/compression"
	"github.com/coinbase/chainnode/internal/utils/testapp"
	"github.com/coinbase/chainnode/internal/utils/testutil"
)

const (
	traceByNumberPartitionKeyFixture = "1#traces-by-number#123456"
	traceByNumberSortKeyFixture      = "0000000000003018"
	traceByNumberTagFixture          = uint32(1)
	traceByNumberSequenceFixture     = api.Sequence(12312)
	traceByNumberHeightFixture       = uint64(123456)
	traceByNumberHashFixture         = "0x123"
	traceByNumberBlockTimeFixture    = "2020-11-24T16:07:21Z"
)

type TraceByNumberStorageTestSuite struct {
	suite.Suite
	ctrl              *gomock.Controller
	collectionStorage *cmocks.MockCollectionStorage
	storage           TraceByNumberStorage
	app               testapp.TestApp
	blockTimeFixture  time.Time
}

func TestTraceByNumberStorageTestSuite(t *testing.T) {
	suite.Run(t, new(TraceByNumberStorageTestSuite))
}

func (s *TraceByNumberStorageTestSuite) SetupTest() {
	s.ctrl = gomock.NewController(s.T())
	s.collectionStorage = cmocks.NewMockCollectionStorage(s.ctrl)
	s.collectionStorage.EXPECT().
		WithCollection(xapi.CollectionTracesByNumber).
		Return(s.collectionStorage)
	logsStorage := cmocks.NewMockCollectionStorage(s.ctrl)

	s.app = testapp.New(
		s.T(),
		fx.Provide(newTraceByNumberStorage),
		fx.Provide(fx.Annotated{Name: "collection", Target: func() collection.CollectionStorage { return s.collectionStorage }}),
		fx.Provide(fx.Annotated{Name: "logs", Target: func() collection.CollectionStorage { return logsStorage }}),
		fx.Populate(&s.storage),
	)
	s.blockTimeFixture = testutil.MustTime(traceByNumberBlockTimeFixture)
}

func (s *TraceByNumberStorageTestSuite) TearDownTest() {
	s.app.Close()
	s.ctrl.Finish()
}

func (s *TraceByNumberStorageTestSuite) TestPersistTrace() {
	require := testutil.Require(s.T())
	ctx := context.Background()

	data := []byte("asdqwd")
	compressed, err := compression.Compress(data, compression.CompressionGzip)
	require.NoError(err)

	trace := &xapi.Trace{
		Tag:       traceByNumberTagFixture,
		Height:    traceByNumberHeightFixture,
		Hash:      traceByNumberHashFixture,
		Sequence:  traceByNumberSequenceFixture,
		Data:      data,
		BlockTime: s.blockTimeFixture,
	}

	entry := &models.TraceByNumberDDBEntry{
		BaseItem: collection.NewBaseItem(
			traceByNumberPartitionKeyFixture,
			traceByNumberSortKeyFixture,
			traceByNumberTagFixture,
		).WithData(compressed),
		Hash:      traceByNumberHashFixture,
		Height:    traceByNumberHeightFixture,
		BlockTime: traceByNumberBlockTimeFixture,
	}

	s.collectionStorage.EXPECT().
		UploadToBlobStorage(gomock.Any(), gomock.Any(), false).Return(nil)
	s.collectionStorage.EXPECT().WriteItem(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, item interface{}) error {
			actual, ok := item.(*models.TraceByNumberDDBEntry)
			require.True(ok)
			testutil.MustTime(actual.UpdatedAt)
			entry.UpdatedAt = actual.UpdatedAt
			require.Equal(entry, actual)
			return nil
		})

	err = s.storage.PersistTraceByNumber(ctx, trace)
	require.NoError(err)
}

func (s *TraceByNumberStorageTestSuite) TestPersistTrace_NoBlockTime() {
	require := testutil.Require(s.T())
	ctx := context.Background()

	data := []byte("asdqwd")
	compressed, err := compression.Compress(data, compression.CompressionGzip)
	require.NoError(err)

	trace := &xapi.Trace{
		Tag:      traceByNumberTagFixture,
		Height:   traceByNumberHeightFixture,
		Hash:     traceByNumberHashFixture,
		Sequence: traceByNumberSequenceFixture,
		Data:     data,
	}

	entry := &models.TraceByNumberDDBEntry{
		BaseItem: collection.NewBaseItem(
			traceByNumberPartitionKeyFixture,
			traceByNumberSortKeyFixture,
			traceByNumberTagFixture,
		).WithData(compressed),
		Hash:   traceByNumberHashFixture,
		Height: traceByNumberHeightFixture,
	}

	s.collectionStorage.EXPECT().
		UploadToBlobStorage(gomock.Any(), gomock.Any(), false).Return(nil)
	s.collectionStorage.EXPECT().WriteItem(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, item interface{}) error {
			actual, ok := item.(*models.TraceByNumberDDBEntry)
			require.True(ok)
			testutil.MustTime(actual.UpdatedAt)
			entry.UpdatedAt = actual.UpdatedAt
			require.Equal(entry, actual)
			return nil
		})

	err = s.storage.PersistTraceByNumber(ctx, trace)
	require.NoError(err)
}

func (s *TraceByNumberStorageTestSuite) TestPersistTrace_LargeFile() {
	require := testutil.Require(s.T())
	ctx := context.Background()

	data := []byte("asdqwd")
	objectKey := "1/traces-by-number/123456/0x123"

	trace := &xapi.Trace{
		Tag:       traceByNumberTagFixture,
		Height:    traceByNumberHeightFixture,
		Hash:      traceByNumberHashFixture,
		Sequence:  traceByNumberSequenceFixture,
		Data:      data,
		BlockTime: s.blockTimeFixture,
	}

	entry := &models.TraceByNumberDDBEntry{
		BaseItem: collection.NewBaseItem(
			traceByNumberPartitionKeyFixture,
			traceByNumberSortKeyFixture,
			traceByNumberTagFixture,
		).WithObjectKey(objectKey),
		Hash:      traceByNumberHashFixture,
		Height:    traceByNumberHeightFixture,
		BlockTime: traceByNumberBlockTimeFixture,
	}

	s.collectionStorage.EXPECT().
		UploadToBlobStorage(gomock.Any(), gomock.Any(), false).
		DoAndReturn(func(ctx context.Context, entry collection.Item, enforceUpload bool) error {
			err := entry.SetData(nil)
			require.NoError(err)
			err = entry.SetObjectKey(objectKey)
			require.NoError(err)
			return nil
		})
	s.collectionStorage.EXPECT().WriteItem(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, item interface{}) error {
			actual, ok := item.(*models.TraceByNumberDDBEntry)
			require.True(ok)
			testutil.MustTime(actual.UpdatedAt)
			entry.UpdatedAt = actual.UpdatedAt
			require.Equal(entry, actual)
			return nil
		})

	err := s.storage.PersistTraceByNumber(ctx, trace)
	require.NoError(err)
}

func (s *TraceByNumberStorageTestSuite) TestPersistTrace_UploadEnforced() {
	require := testutil.Require(s.T())
	ctx := context.Background()

	data := []byte("asdqwd")
	objectKey := "1/traces-by-number/123456/0x123"

	s.app.Config().Storage.TraceUploadEnforced = true
	trace := &xapi.Trace{
		Tag:       traceByNumberTagFixture,
		Height:    traceByNumberHeightFixture,
		Hash:      traceByNumberHashFixture,
		Sequence:  traceByNumberSequenceFixture,
		Data:      data,
		BlockTime: s.blockTimeFixture,
	}

	entry := &models.TraceByNumberDDBEntry{
		BaseItem: collection.NewBaseItem(
			traceByNumberPartitionKeyFixture,
			traceByNumberSortKeyFixture,
			traceByNumberTagFixture,
		).WithObjectKey(objectKey),
		Hash:      traceByNumberHashFixture,
		Height:    traceByNumberHeightFixture,
		BlockTime: traceByNumberBlockTimeFixture,
	}

	s.collectionStorage.EXPECT().
		UploadToBlobStorage(gomock.Any(), gomock.Any(), true).
		DoAndReturn(func(ctx context.Context, entry collection.Item, enforceUpload bool) error {
			err := entry.SetData(nil)
			require.NoError(err)
			err = entry.SetObjectKey(objectKey)
			require.NoError(err)
			return nil
		})
	s.collectionStorage.EXPECT().WriteItem(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, item interface{}) error {
			actual, ok := item.(*models.TraceByNumberDDBEntry)
			require.True(ok)
			testutil.MustTime(actual.UpdatedAt)
			entry.UpdatedAt = actual.UpdatedAt
			require.Equal(entry, actual)
			return nil
		})

	err := s.storage.PersistTraceByNumber(ctx, trace)
	require.NoError(err)
}

func (s *TraceByNumberStorageTestSuite) TestPersistTrace_LargeFileUploadError() {
	require := testutil.Require(s.T())
	ctx := context.Background()

	data := []byte("asdqwd")

	trace := &xapi.Trace{
		Tag:       traceByNumberTagFixture,
		Height:    traceByNumberHeightFixture,
		Hash:      traceByNumberHashFixture,
		Sequence:  traceByNumberSequenceFixture,
		Data:      data,
		BlockTime: s.blockTimeFixture,
	}

	s.collectionStorage.EXPECT().
		UploadToBlobStorage(gomock.Any(), gomock.Any(), false).
		Return(xerrors.Errorf("failed to upload"))

	err := s.storage.PersistTraceByNumber(ctx, trace)
	require.Error(err)
}

func (s *TraceByNumberStorageTestSuite) TestGetTraces() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	data := []byte("asdqwd")
	compressed, err := compression.Compress(data, compression.CompressionGzip)
	require.NoError(err)
	entry := &models.TraceByNumberDDBEntry{
		BaseItem: collection.NewBaseItem(
			traceByNumberPartitionKeyFixture,
			traceByNumberSortKeyFixture,
			traceByNumberTagFixture,
		).WithData(compressed),
		Height:    traceByNumberHeightFixture,
		Hash:      traceByNumberHashFixture,
		BlockTime: traceByNumberBlockTimeFixture,
	}

	expectedTraces := &xapi.Trace{
		Tag:       traceByNumberTagFixture,
		Sequence:  traceByNumberSequenceFixture,
		Height:    traceByNumberHeightFixture,
		Hash:      traceByNumberHashFixture,
		Data:      data,
		BlockTime: s.blockTimeFixture,
		UpdatedAt: testutil.MustTime(entry.UpdatedAt),
	}

	entries := make([]interface{}, 1)
	entries[0] = entry

	s.collectionStorage.EXPECT().QueryItemByMaxSortKey(
		gomock.Any(),
		traceByNumberPartitionKeyFixture,
		traceByNumberSortKeyFixture,
		models.TraceByNumberDDBEntry{},
		nil,
	).Return(&collection.QueryItemsResult{Items: entries, LastEvaluatedKey: nil}, nil)

	trace, err := s.storage.GetTraceByNumber(ctx, traceByNumberTagFixture, traceByNumberHeightFixture, traceByNumberSequenceFixture)
	require.NoError(err)
	require.Equal(expectedTraces, trace)
}

func (s *TraceByNumberStorageTestSuite) TestGetTrace_NoBlockTime() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	data := []byte("asdqwd")
	compressed, err := compression.Compress(data, compression.CompressionGzip)
	require.NoError(err)
	entry := &models.TraceByNumberDDBEntry{
		BaseItem: collection.NewBaseItem(
			traceByNumberPartitionKeyFixture,
			traceByNumberSortKeyFixture,
			traceByNumberTagFixture,
		).WithData(compressed),
		Height: traceByNumberHeightFixture,
		Hash:   traceByNumberHashFixture,
	}

	expectedTraces := &xapi.Trace{
		Tag:       traceByNumberTagFixture,
		Sequence:  traceByNumberSequenceFixture,
		Height:    traceByNumberHeightFixture,
		Hash:      traceByNumberHashFixture,
		Data:      data,
		UpdatedAt: testutil.MustTime(entry.UpdatedAt),
	}

	entries := make([]interface{}, 1)
	entries[0] = entry

	s.collectionStorage.EXPECT().QueryItemByMaxSortKey(
		gomock.Any(),
		traceByNumberPartitionKeyFixture,
		traceByNumberSortKeyFixture,
		models.TraceByNumberDDBEntry{},
		nil,
	).Return(&collection.QueryItemsResult{Items: entries, LastEvaluatedKey: nil}, nil)

	trace, err := s.storage.GetTraceByNumber(ctx, traceByNumberTagFixture, traceByNumberHeightFixture, traceByNumberSequenceFixture)
	require.NoError(err)
	require.Equal(expectedTraces, trace)
}

func (s *TraceByNumberStorageTestSuite) TestGetTrace_InvalidBlockTime() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	data := []byte("asdqwd")
	compressed, err := compression.Compress(data, compression.CompressionGzip)
	require.NoError(err)
	entry := &models.TraceByNumberDDBEntry{
		BaseItem: collection.NewBaseItem(
			traceByNumberPartitionKeyFixture,
			traceByNumberSortKeyFixture,
			traceByNumberTagFixture,
		).WithData(compressed),
		Height:    traceByNumberHeightFixture,
		Hash:      traceByNumberHashFixture,
		BlockTime: "invalid",
	}

	entries := make([]interface{}, 1)
	entries[0] = entry

	s.collectionStorage.EXPECT().QueryItemByMaxSortKey(
		gomock.Any(),
		traceByNumberPartitionKeyFixture,
		traceByNumberSortKeyFixture,
		models.TraceByNumberDDBEntry{},
		nil,
	).Return(&collection.QueryItemsResult{Items: entries, LastEvaluatedKey: nil}, nil)

	trace, err := s.storage.GetTraceByNumber(ctx, traceByNumberTagFixture, traceByNumberHeightFixture, traceByNumberSequenceFixture)
	require.Error(err)
	require.Nil(trace)
}

func (s *TraceByNumberStorageTestSuite) TestGetTrace_LargeFile() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	data := []byte("asdqwd")
	compressed, err := compression.Compress(data, compression.CompressionGzip)
	require.NoError(err)
	objectKey := "s3-location"
	entry := &models.TraceByNumberDDBEntry{
		BaseItem: collection.NewBaseItem(
			traceByNumberPartitionKeyFixture,
			traceByNumberSortKeyFixture,
			traceByNumberTagFixture,
		).WithObjectKey(objectKey),
		Height:    traceByNumberHeightFixture,
		Hash:      traceByNumberHashFixture,
		BlockTime: traceByNumberBlockTimeFixture,
	}

	expectedTrace := &xapi.Trace{
		Tag:       traceByNumberTagFixture,
		Sequence:  traceByNumberSequenceFixture,
		Height:    traceByNumberHeightFixture,
		Hash:      traceByNumberHashFixture,
		Data:      data,
		BlockTime: s.blockTimeFixture,
		UpdatedAt: testutil.MustTime(entry.UpdatedAt),
	}

	entries := make([]interface{}, 1)
	entries[0] = entry

	s.collectionStorage.EXPECT().QueryItemByMaxSortKey(
		gomock.Any(),
		traceByNumberPartitionKeyFixture,
		traceByNumberSortKeyFixture,
		models.TraceByNumberDDBEntry{},
		nil,
	).Return(&collection.QueryItemsResult{Items: entries, LastEvaluatedKey: nil}, nil)

	s.collectionStorage.EXPECT().
		DownloadFromBlobStorage(gomock.Any(), entry).
		DoAndReturn(func(ctx context.Context, entry collection.Item) error {
			err := entry.SetData(compressed)
			require.NoError(err)
			return nil
		})

	trace, err := s.storage.GetTraceByNumber(ctx, traceByNumberTagFixture, traceByNumberHeightFixture, traceByNumberSequenceFixture)
	require.NoError(err)
	require.Equal(expectedTrace, trace)
}

func (s *TraceByNumberStorageTestSuite) TestGetTrace_LargeFileDownloadFailure() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	objectKey := "s3-location"
	entry := &models.TraceByNumberDDBEntry{
		BaseItem: collection.NewBaseItem(
			traceByNumberPartitionKeyFixture,
			traceByNumberSortKeyFixture,
			traceByNumberTagFixture,
		).WithObjectKey(objectKey),
		Height:    traceByNumberHeightFixture,
		Hash:      traceByNumberHashFixture,
		BlockTime: traceByNumberBlockTimeFixture,
	}

	entries := make([]interface{}, 1)
	entries[0] = entry

	s.collectionStorage.EXPECT().QueryItemByMaxSortKey(
		gomock.Any(),
		traceByNumberPartitionKeyFixture,
		traceByNumberSortKeyFixture,
		models.TraceByNumberDDBEntry{},
		nil,
	).Return(&collection.QueryItemsResult{Items: entries, LastEvaluatedKey: nil}, nil)

	s.collectionStorage.EXPECT().DownloadFromBlobStorage(gomock.Any(), entry).
		Return(xerrors.Errorf("failed to download"))

	trace, err := s.storage.GetTraceByNumber(ctx, traceByNumberTagFixture, traceByNumberHeightFixture, traceByNumberSequenceFixture)
	require.Error(err)
	require.Nil(trace)
}
