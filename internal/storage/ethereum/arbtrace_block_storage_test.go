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
	arbtraceBlockPartitionKeyFixture = "1#arbtrace-block#123456"
	arbtraceBlockSortKeyFixture      = "0000000000003018"
	arbtraceBlockTagFixture          = uint32(1)
	arbtraceBlockSequenceFixture     = api.Sequence(12312)
	arbtraceBlockHeightFixture       = uint64(123456)
	arbtraceBlockHashFixture         = "0x123"
	arbtraceBlockBlockTimeFixture    = "2020-11-24T16:07:21Z"
)

type ArbtraceBlockStorageTestSuite struct {
	suite.Suite
	ctrl              *gomock.Controller
	collectionStorage *cmocks.MockCollectionStorage
	storage           ArbtraceBlockStorage
	app               testapp.TestApp
	blockTimeFixture  time.Time
}

func TestArbtraceBlockStorageTestSuite(t *testing.T) {
	suite.Run(t, new(ArbtraceBlockStorageTestSuite))
}

func (s *ArbtraceBlockStorageTestSuite) SetupTest() {
	s.ctrl = gomock.NewController(s.T())
	s.collectionStorage = cmocks.NewMockCollectionStorage(s.ctrl)
	s.collectionStorage.EXPECT().
		WithCollection(xapi.CollectionArbtraceBlock).
		Return(s.collectionStorage)
	logsStorage := cmocks.NewMockCollectionStorage(s.ctrl)

	s.app = testapp.New(
		s.T(),
		fx.Provide(newArbtraceBlockStorage),
		fx.Provide(fx.Annotated{Name: "collection", Target: func() collection.CollectionStorage { return s.collectionStorage }}),
		fx.Provide(fx.Annotated{Name: "logs", Target: func() collection.CollectionStorage { return logsStorage }}),
		fx.Populate(&s.storage),
	)
	s.blockTimeFixture = testutil.MustTime(arbtraceBlockBlockTimeFixture)
}

func (s *ArbtraceBlockStorageTestSuite) TearDownTest() {
	s.app.Close()
	s.ctrl.Finish()
}

func (s *ArbtraceBlockStorageTestSuite) TestPersistTrace() {
	require := testutil.Require(s.T())
	ctx := context.Background()

	data := []byte("asdqwd")
	compressed, err := compression.Compress(data, compression.CompressionGzip)
	require.NoError(err)

	trace := &xapi.Trace{
		Tag:       arbtraceBlockTagFixture,
		Height:    arbtraceBlockHeightFixture,
		Hash:      arbtraceBlockHashFixture,
		Sequence:  arbtraceBlockSequenceFixture,
		Data:      data,
		BlockTime: s.blockTimeFixture,
	}

	entry := &models.ArbtraceBlockDDBEntry{
		BaseItem: collection.NewBaseItem(
			arbtraceBlockPartitionKeyFixture,
			arbtraceBlockSortKeyFixture,
			arbtraceBlockTagFixture,
		).WithData(compressed),
		Hash:      arbtraceBlockHashFixture,
		Height:    arbtraceBlockHeightFixture,
		BlockTime: arbtraceBlockBlockTimeFixture,
	}

	s.collectionStorage.EXPECT().
		UploadToBlobStorage(gomock.Any(), gomock.Any(), false).Return(nil)
	s.collectionStorage.EXPECT().WriteItem(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, item interface{}) error {
			actual, ok := item.(*models.ArbtraceBlockDDBEntry)
			require.True(ok)
			testutil.MustTime(actual.UpdatedAt)
			entry.UpdatedAt = actual.UpdatedAt
			require.Equal(entry, actual)
			return nil
		})

	err = s.storage.PersistArbtraceBlock(ctx, trace)
	require.NoError(err)
}

func (s *ArbtraceBlockStorageTestSuite) TestPersistTrace_NoBlockTime() {
	require := testutil.Require(s.T())
	ctx := context.Background()

	data := []byte("asdqwd")
	compressed, err := compression.Compress(data, compression.CompressionGzip)
	require.NoError(err)

	trace := &xapi.Trace{
		Tag:      arbtraceBlockTagFixture,
		Height:   arbtraceBlockHeightFixture,
		Hash:     arbtraceBlockHashFixture,
		Sequence: arbtraceBlockSequenceFixture,
		Data:     data,
	}

	entry := &models.ArbtraceBlockDDBEntry{
		BaseItem: collection.NewBaseItem(
			arbtraceBlockPartitionKeyFixture,
			arbtraceBlockSortKeyFixture,
			arbtraceBlockTagFixture,
		).WithData(compressed),
		Hash:   arbtraceBlockHashFixture,
		Height: arbtraceBlockHeightFixture,
	}

	s.collectionStorage.EXPECT().
		UploadToBlobStorage(gomock.Any(), gomock.Any(), false).Return(nil)
	s.collectionStorage.EXPECT().WriteItem(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, item interface{}) error {
			actual, ok := item.(*models.ArbtraceBlockDDBEntry)
			require.True(ok)
			testutil.MustTime(actual.UpdatedAt)
			entry.UpdatedAt = actual.UpdatedAt
			require.Equal(entry, actual)
			return nil
		})

	err = s.storage.PersistArbtraceBlock(ctx, trace)
	require.NoError(err)
}

func (s *ArbtraceBlockStorageTestSuite) TestPersistTrace_LargeFile() {
	require := testutil.Require(s.T())
	ctx := context.Background()

	data := []byte("asdqwd")
	objectKey := "1/arbtrace-block/123456/0x123"

	trace := &xapi.Trace{
		Tag:       arbtraceBlockTagFixture,
		Height:    arbtraceBlockHeightFixture,
		Hash:      arbtraceBlockHashFixture,
		Sequence:  arbtraceBlockSequenceFixture,
		Data:      data,
		BlockTime: s.blockTimeFixture,
	}

	entry := &models.ArbtraceBlockDDBEntry{
		BaseItem: collection.NewBaseItem(
			arbtraceBlockPartitionKeyFixture,
			arbtraceBlockSortKeyFixture,
			arbtraceBlockTagFixture,
		).WithObjectKey(objectKey),
		Hash:      arbtraceBlockHashFixture,
		Height:    arbtraceBlockHeightFixture,
		BlockTime: arbtraceBlockBlockTimeFixture,
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
			actual, ok := item.(*models.ArbtraceBlockDDBEntry)
			require.True(ok)
			testutil.MustTime(actual.UpdatedAt)
			entry.UpdatedAt = actual.UpdatedAt
			require.Equal(entry, actual)
			return nil
		})

	err := s.storage.PersistArbtraceBlock(ctx, trace)
	require.NoError(err)
}

func (s *ArbtraceBlockStorageTestSuite) TestPersistTrace_LargeFileUploadError() {
	require := testutil.Require(s.T())
	ctx := context.Background()

	data := []byte("asdqwd")

	trace := &xapi.Trace{
		Tag:       arbtraceBlockTagFixture,
		Height:    arbtraceBlockHeightFixture,
		Hash:      arbtraceBlockHashFixture,
		Sequence:  arbtraceBlockSequenceFixture,
		Data:      data,
		BlockTime: s.blockTimeFixture,
	}

	s.collectionStorage.EXPECT().
		UploadToBlobStorage(gomock.Any(), gomock.Any(), false).
		Return(xerrors.Errorf("failed to upload"))

	err := s.storage.PersistArbtraceBlock(ctx, trace)
	require.Error(err)
}

func (s *ArbtraceBlockStorageTestSuite) TestGetTraces() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	data := []byte("asdqwd")
	compressed, err := compression.Compress(data, compression.CompressionGzip)
	require.NoError(err)
	entry := &models.ArbtraceBlockDDBEntry{
		BaseItem: collection.NewBaseItem(
			arbtraceBlockPartitionKeyFixture,
			arbtraceBlockSortKeyFixture,
			arbtraceBlockTagFixture,
		).WithData(compressed),
		Hash:      arbtraceBlockHashFixture,
		Height:    arbtraceBlockHeightFixture,
		BlockTime: arbtraceBlockBlockTimeFixture,
	}

	expectedTraces := &xapi.Trace{
		Tag:       arbtraceBlockTagFixture,
		Height:    arbtraceBlockHeightFixture,
		Hash:      arbtraceBlockHashFixture,
		Sequence:  arbtraceBlockSequenceFixture,
		Data:      data,
		BlockTime: s.blockTimeFixture,
		UpdatedAt: testutil.MustTime(entry.UpdatedAt),
	}

	entries := make([]interface{}, 1)
	entries[0] = entry

	s.collectionStorage.EXPECT().QueryItemByMaxSortKey(
		gomock.Any(),
		arbtraceBlockPartitionKeyFixture,
		arbtraceBlockSortKeyFixture,
		models.ArbtraceBlockDDBEntry{},
		nil,
	).Return(&collection.QueryItemsResult{Items: entries, LastEvaluatedKey: nil}, nil)

	trace, err := s.storage.GetArbtraceBlock(ctx, arbtraceBlockTagFixture, arbtraceBlockHeightFixture, arbtraceBlockSequenceFixture)
	require.NoError(err)
	require.Equal(expectedTraces, trace)
}

func (s *ArbtraceBlockStorageTestSuite) TestGetTrace_NoBlockTime() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	data := []byte("asdqwd")
	compressed, err := compression.Compress(data, compression.CompressionGzip)
	require.NoError(err)
	entry := &models.ArbtraceBlockDDBEntry{
		BaseItem: collection.NewBaseItem(
			arbtraceBlockPartitionKeyFixture,
			arbtraceBlockSortKeyFixture,
			arbtraceBlockTagFixture,
		).WithData(compressed),
		Hash:   arbtraceBlockHashFixture,
		Height: arbtraceBlockHeightFixture,
	}

	expectedTraces := &xapi.Trace{
		Tag:       arbtraceBlockTagFixture,
		Height:    arbtraceBlockHeightFixture,
		Hash:      arbtraceBlockHashFixture,
		Sequence:  arbtraceBlockSequenceFixture,
		Data:      data,
		UpdatedAt: testutil.MustTime(entry.UpdatedAt),
	}

	entries := make([]interface{}, 1)
	entries[0] = entry

	s.collectionStorage.EXPECT().QueryItemByMaxSortKey(
		gomock.Any(),
		arbtraceBlockPartitionKeyFixture,
		arbtraceBlockSortKeyFixture,
		models.ArbtraceBlockDDBEntry{},
		nil,
	).Return(&collection.QueryItemsResult{Items: entries, LastEvaluatedKey: nil}, nil)

	trace, err := s.storage.GetArbtraceBlock(ctx, arbtraceBlockTagFixture, arbtraceBlockHeightFixture, arbtraceBlockSequenceFixture)
	require.NoError(err)
	require.Equal(expectedTraces, trace)
}

func (s *ArbtraceBlockStorageTestSuite) TestGetTrace_InvalidBlockTime() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	data := []byte("asdqwd")
	compressed, err := compression.Compress(data, compression.CompressionGzip)
	require.NoError(err)
	entry := &models.ArbtraceBlockDDBEntry{
		BaseItem: collection.NewBaseItem(
			arbtraceBlockPartitionKeyFixture,
			arbtraceBlockSortKeyFixture,
			arbtraceBlockTagFixture,
		).WithData(compressed),
		Hash:      arbtraceBlockHashFixture,
		Height:    arbtraceBlockHeightFixture,
		BlockTime: "invalid",
	}

	entries := make([]interface{}, 1)
	entries[0] = entry

	s.collectionStorage.EXPECT().QueryItemByMaxSortKey(
		gomock.Any(),
		arbtraceBlockPartitionKeyFixture,
		arbtraceBlockSortKeyFixture,
		models.ArbtraceBlockDDBEntry{},
		nil,
	).Return(&collection.QueryItemsResult{Items: entries, LastEvaluatedKey: nil}, nil)

	trace, err := s.storage.GetArbtraceBlock(ctx, arbtraceBlockTagFixture, arbtraceBlockHeightFixture, arbtraceBlockSequenceFixture)
	require.Error(err)
	require.Nil(trace)
}

func (s *ArbtraceBlockStorageTestSuite) TestGetTrace_LargeFile() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	data := []byte("asdqwd")
	compressed, err := compression.Compress(data, compression.CompressionGzip)
	require.NoError(err)
	objectKey := "s3-location"
	entry := &models.ArbtraceBlockDDBEntry{
		BaseItem: collection.NewBaseItem(
			arbtraceBlockPartitionKeyFixture,
			arbtraceBlockSortKeyFixture,
			arbtraceBlockTagFixture,
		).WithObjectKey(objectKey),
		Hash:      arbtraceBlockHashFixture,
		Height:    arbtraceBlockHeightFixture,
		BlockTime: arbtraceBlockBlockTimeFixture,
	}

	expectedTrace := &xapi.Trace{
		Tag:       arbtraceBlockTagFixture,
		Height:    arbtraceBlockHeightFixture,
		Hash:      arbtraceBlockHashFixture,
		Sequence:  arbtraceBlockSequenceFixture,
		Data:      data,
		BlockTime: s.blockTimeFixture,
		UpdatedAt: testutil.MustTime(entry.UpdatedAt),
	}

	entries := make([]interface{}, 1)
	entries[0] = entry

	s.collectionStorage.EXPECT().QueryItemByMaxSortKey(
		gomock.Any(),
		arbtraceBlockPartitionKeyFixture,
		arbtraceBlockSortKeyFixture,
		models.ArbtraceBlockDDBEntry{},
		nil,
	).Return(&collection.QueryItemsResult{Items: entries, LastEvaluatedKey: nil}, nil)

	s.collectionStorage.EXPECT().
		DownloadFromBlobStorage(gomock.Any(), entry).
		DoAndReturn(func(ctx context.Context, entry collection.Item) error {
			err := entry.SetData(compressed)
			require.NoError(err)
			return nil
		})

	trace, err := s.storage.GetArbtraceBlock(ctx, arbtraceBlockTagFixture, arbtraceBlockHeightFixture, arbtraceBlockSequenceFixture)
	require.NoError(err)
	require.Equal(expectedTrace, trace)
}

func (s *ArbtraceBlockStorageTestSuite) TestGetTrace_LargeFileDownloadFailure() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	objectKey := "s3-location"
	entry := &models.ArbtraceBlockDDBEntry{
		BaseItem: collection.NewBaseItem(
			arbtraceBlockPartitionKeyFixture,
			arbtraceBlockSortKeyFixture,
			arbtraceBlockTagFixture,
		).WithObjectKey(objectKey),
		Hash:      arbtraceBlockHashFixture,
		Height:    arbtraceBlockHeightFixture,
		BlockTime: arbtraceBlockBlockTimeFixture,
	}

	entries := make([]interface{}, 1)
	entries[0] = entry

	s.collectionStorage.EXPECT().QueryItemByMaxSortKey(
		gomock.Any(),
		arbtraceBlockPartitionKeyFixture,
		arbtraceBlockSortKeyFixture,
		models.ArbtraceBlockDDBEntry{},
		nil,
	).Return(&collection.QueryItemsResult{Items: entries, LastEvaluatedKey: nil}, nil)

	s.collectionStorage.EXPECT().DownloadFromBlobStorage(gomock.Any(), entry).
		Return(xerrors.Errorf("failed to download"))

	trace, err := s.storage.GetArbtraceBlock(ctx, arbtraceBlockTagFixture, arbtraceBlockHeightFixture, arbtraceBlockSequenceFixture)
	require.Error(err)
	require.Nil(trace)
}
