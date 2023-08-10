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
	"github.com/coinbase/chainnode/internal/api/ethereum"
	"github.com/coinbase/chainnode/internal/storage/collection"
	cmocks "github.com/coinbase/chainnode/internal/storage/collection/mocks"
	"github.com/coinbase/chainnode/internal/storage/ethereum/models"
	"github.com/coinbase/chainnode/internal/utils/compression"
	"github.com/coinbase/chainnode/internal/utils/testapp"
	"github.com/coinbase/chainnode/internal/utils/testutil"
)

type BlockStorageByHashWithoutFullTxTestSuite struct {
	suite.Suite
	ctrl              *gomock.Controller
	collectionStorage *cmocks.MockCollectionStorage
	storage           BlockByHashWithoutFullTxStorage
	app               testapp.TestApp
	blockTimeFixture  time.Time
}

const (
	blockByHashWithoutFullTxTagFixture          = uint32(1)
	blockByHashWithoutFullTxSequenceFixture     = api.Sequence(12312)
	blockByHashWithoutFullTxHeightFixture       = uint64(123456)
	blockByHashWithoutFullTxHashFixture         = "0xaa7d33f"
	blockByHashWithoutFullTxPartitionKeyFixture = "1#blocks-by-hash-without-full-tx#0xaa7d33f"
	blockByHashWithoutFullTxSortKeyFixture      = "0000000000003018"
	blockByHashWithoutFullTxBlockTimeFixture    = "2020-11-24T16:07:21Z"
)

func TestBlockStorageByHashWithoutFullTxTestSuite(t *testing.T) {
	suite.Run(t, new(BlockStorageByHashWithoutFullTxTestSuite))
}

func (s *BlockStorageByHashWithoutFullTxTestSuite) SetupTest() {
	s.ctrl = gomock.NewController(s.T())
	s.collectionStorage = cmocks.NewMockCollectionStorage(s.ctrl)
	s.collectionStorage.EXPECT().
		WithCollection(ethereum.CollectionBlocksByHashWithoutFullTx).
		Return(s.collectionStorage)
	logsStorage := cmocks.NewMockCollectionStorage(s.ctrl)

	s.app = testapp.New(
		s.T(),
		fx.Provide(newBlockByHashWithoutFullTxStorage),
		fx.Provide(fx.Annotated{Name: "collection", Target: func() collection.CollectionStorage { return s.collectionStorage }}),
		fx.Provide(fx.Annotated{Name: "logs", Target: func() collection.CollectionStorage { return logsStorage }}),
		fx.Populate(&s.storage),
	)
	s.blockTimeFixture = testutil.MustTime(blockByHashWithoutFullTxBlockTimeFixture)
}

func (s *BlockStorageByHashWithoutFullTxTestSuite) TearDownTest() {
	s.app.Close()
	s.ctrl.Finish()
}

func (s *BlockStorageByHashWithoutFullTxTestSuite) TestPersistBlockByHashWithoutFullTx() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	data := []byte("asdqewdw")

	compressed, err := compression.Compress(data, compression.CompressionGzip)
	require.NoError(err)

	expectedEntry := &models.BlockByHashWithoutFullTxDDBEntry{
		BaseItem: collection.NewBaseItem(
			blockByHashWithoutFullTxPartitionKeyFixture,
			blockByHashWithoutFullTxSortKeyFixture,
			blockByHashWithoutFullTxTagFixture,
		).WithData(compressed),
		Height:    blockByHashWithoutFullTxHeightFixture,
		Hash:      blockByHashWithoutFullTxHashFixture,
		BlockTime: blockByHashWithoutFullTxBlockTimeFixture,
	}

	s.collectionStorage.EXPECT().
		UploadToBlobStorage(gomock.Any(), gomock.Any(), false).Return(nil)
	s.collectionStorage.EXPECT().WriteItem(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx2 context.Context, entry interface{}) error {
			actualEntry, ok := entry.(*models.BlockByHashWithoutFullTxDDBEntry)
			require.True(ok)
			testutil.MustTime(actualEntry.UpdatedAt)
			expectedEntry.UpdatedAt = actualEntry.UpdatedAt
			require.Equal(expectedEntry, actualEntry)
			return nil
		})
	block := ethereum.NewBlock(
		blockByHashWithoutFullTxTagFixture,
		blockByHashWithoutFullTxHeightFixture,
		blockByHashWithoutFullTxHashFixture,
		blockByHashWithoutFullTxSequenceFixture,
		s.blockTimeFixture,
		data,
	)
	err = s.storage.PersistBlockByHashWithoutFullTx(ctx, block)
	require.NoError(err)
}

func (s *BlockStorageByHashWithoutFullTxTestSuite) TestPersistBlockByHashWithoutFullTx_NoBlockTime() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	data := []byte("asdqewdw")

	compressed, err := compression.Compress(data, compression.CompressionGzip)
	require.NoError(err)

	expectedEntry := &models.BlockByHashWithoutFullTxDDBEntry{
		BaseItem: collection.NewBaseItem(
			blockByHashWithoutFullTxPartitionKeyFixture,
			blockByHashWithoutFullTxSortKeyFixture,
			blockByHashWithoutFullTxTagFixture,
		).WithData(compressed),
		Height: blockByHashWithoutFullTxHeightFixture,
		Hash:   blockByHashWithoutFullTxHashFixture,
	}

	s.collectionStorage.EXPECT().
		UploadToBlobStorage(gomock.Any(), gomock.Any(), false).Return(nil)
	s.collectionStorage.EXPECT().WriteItem(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx2 context.Context, entry interface{}) error {
			actualEntry, ok := entry.(*models.BlockByHashWithoutFullTxDDBEntry)
			require.True(ok)
			testutil.MustTime(actualEntry.UpdatedAt)
			expectedEntry.UpdatedAt = actualEntry.UpdatedAt
			require.Equal(expectedEntry, actualEntry)
			return nil
		})
	block := ethereum.NewBlock(
		blockByHashWithoutFullTxTagFixture,
		blockByHashWithoutFullTxHeightFixture,
		blockByHashWithoutFullTxHashFixture,
		blockByHashWithoutFullTxSequenceFixture,
		time.Time{},
		data,
	)
	err = s.storage.PersistBlockByHashWithoutFullTx(ctx, block)
	require.NoError(err)
}

func (s *BlockStorageByHashWithoutFullTxTestSuite) TestPersistBlockByHashWithoutFullTx_LargeFile() {
	require := testutil.Require(s.T())

	ctx := context.Background()

	data := testutil.MakeFile(10)
	objectKey := "s3-location"
	expectedEntry := &models.BlockByHashWithoutFullTxDDBEntry{
		BaseItem: collection.NewBaseItem(
			blockByHashWithoutFullTxPartitionKeyFixture,
			blockByHashWithoutFullTxSortKeyFixture,
			blockByHashWithoutFullTxTagFixture,
		).WithObjectKey(objectKey),
		Height:    blockByHashWithoutFullTxHeightFixture,
		Hash:      blockByHashWithoutFullTxHashFixture,
		BlockTime: blockByHashWithoutFullTxBlockTimeFixture,
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
		DoAndReturn(func(ctx context.Context, entry interface{}) error {
			actualEntry, ok := entry.(*models.BlockByHashWithoutFullTxDDBEntry)
			require.True(ok)
			testutil.MustTime(actualEntry.UpdatedAt)
			expectedEntry.UpdatedAt = actualEntry.UpdatedAt
			require.Equal(expectedEntry, actualEntry)
			return nil
		})

	block := ethereum.NewBlock(
		blockByHashWithoutFullTxTagFixture,
		blockByHashWithoutFullTxHeightFixture,
		blockByHashWithoutFullTxHashFixture,
		blockByHashWithoutFullTxSequenceFixture,
		s.blockTimeFixture,
		data,
	)
	err := s.storage.PersistBlockByHashWithoutFullTx(ctx, block)
	require.NoError(err)
}

func (s *BlockStorageByHashWithoutFullTxTestSuite) TestPersistBlockByHashWithoutFullTx_LargeFileUploadError() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	data := testutil.MakeFile(10)

	s.collectionStorage.EXPECT().
		UploadToBlobStorage(gomock.Any(), gomock.Any(), false).
		Return(xerrors.Errorf("failed to upload"))

	block := ethereum.NewBlock(
		blockByHashWithoutFullTxTagFixture,
		blockByHashWithoutFullTxHeightFixture,
		blockByHashWithoutFullTxHashFixture,
		blockByHashWithoutFullTxSequenceFixture,
		s.blockTimeFixture,
		data,
	)
	err := s.storage.PersistBlockByHashWithoutFullTx(ctx, block)
	require.Error(err)
}

func (s *BlockStorageByHashWithoutFullTxTestSuite) TestGetBlockByHashWithoutFullTx() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	data := []byte("asdqewdw")
	compressed, err := compression.Compress(data, compression.CompressionGzip)
	require.NoError(err)
	blockTime := testutil.MustTime(txBlockTimeFixture)

	expectedBlockEntry := &models.BlockByHashWithoutFullTxDDBEntry{
		BaseItem: collection.NewBaseItem(
			blockByHashWithoutFullTxPartitionKeyFixture,
			blockByHashWithoutFullTxSortKeyFixture,
			blockByHashWithoutFullTxTagFixture,
		).WithData(compressed),
		Height:    blockByHashWithoutFullTxHeightFixture,
		Hash:      blockByHashWithoutFullTxHashFixture,
		BlockTime: blockByHashWithoutFullTxBlockTimeFixture,
	}
	expectedBlock := &ethereum.Block{
		Tag:       blockByHashWithoutFullTxTagFixture,
		Height:    blockByHashWithoutFullTxHeightFixture,
		Hash:      blockByHashWithoutFullTxHashFixture,
		Sequence:  blockByHashWithoutFullTxSequenceFixture,
		Data:      data,
		BlockTime: blockTime,
		UpdatedAt: testutil.MustTime(expectedBlockEntry.UpdatedAt),
	}

	entries := make([]interface{}, 1)
	entries[0] = expectedBlockEntry

	s.collectionStorage.EXPECT().QueryItemByMaxSortKey(
		gomock.Any(),
		blockByHashWithoutFullTxPartitionKeyFixture,
		blockByHashWithoutFullTxSortKeyFixture,
		models.BlockByHashWithoutFullTxDDBEntry{},
		nil,
	).Return(&collection.QueryItemsResult{Items: entries, LastEvaluatedKey: nil}, nil)

	block, err := s.storage.GetBlockByHashWithoutFullTx(ctx, blockByHashWithoutFullTxTagFixture, blockByHashWithoutFullTxHashFixture, blockByHashWithoutFullTxSequenceFixture)
	require.NoError(err)
	require.Equal(expectedBlock, block)
}

func (s *BlockStorageByHashWithoutFullTxTestSuite) TestGetBlockByHashWithoutFullTx_NoBlockTime() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	data := []byte("asdqewdw")
	compressed, err := compression.Compress(data, compression.CompressionGzip)
	require.NoError(err)

	expectedBlockEntry := &models.BlockByHashWithoutFullTxDDBEntry{
		BaseItem: collection.NewBaseItem(
			blockByHashWithoutFullTxPartitionKeyFixture,
			blockByHashWithoutFullTxSortKeyFixture,
			blockByHashWithoutFullTxTagFixture,
		).WithData(compressed),
		Height: blockByHashWithoutFullTxHeightFixture,
		Hash:   blockByHashWithoutFullTxHashFixture,
	}

	entries := make([]interface{}, 1)
	entries[0] = expectedBlockEntry

	expectedBlock := &ethereum.Block{
		Tag:       blockByHashWithoutFullTxTagFixture,
		Height:    blockByHashWithoutFullTxHeightFixture,
		Hash:      blockByHashWithoutFullTxHashFixture,
		Sequence:  blockByHashWithoutFullTxSequenceFixture,
		Data:      data,
		UpdatedAt: testutil.MustTime(expectedBlockEntry.UpdatedAt),
	}

	s.collectionStorage.EXPECT().QueryItemByMaxSortKey(
		gomock.Any(),
		blockByHashWithoutFullTxPartitionKeyFixture,
		blockByHashWithoutFullTxSortKeyFixture,
		models.BlockByHashWithoutFullTxDDBEntry{},
		nil,
	).Return(&collection.QueryItemsResult{Items: entries, LastEvaluatedKey: nil}, nil)

	block, err := s.storage.GetBlockByHashWithoutFullTx(ctx, blockByHashWithoutFullTxTagFixture, blockByHashWithoutFullTxHashFixture, blockByHashWithoutFullTxSequenceFixture)
	require.NoError(err)
	require.Equal(expectedBlock, block)
}

func (s *BlockStorageByHashWithoutFullTxTestSuite) TestGetBlockByHashWithoutFullTx_InvalidBlockTime() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	data := []byte("asdqewdw")
	compressed, err := compression.Compress(data, compression.CompressionGzip)
	require.NoError(err)

	expectedBlockEntry := &models.BlockByHashWithoutFullTxDDBEntry{
		BaseItem: collection.NewBaseItem(
			blockByHashWithoutFullTxPartitionKeyFixture,
			blockByHashWithoutFullTxSortKeyFixture,
			blockByHashWithoutFullTxTagFixture,
		).WithData(compressed),
		Height:    blockByHashWithoutFullTxHeightFixture,
		Hash:      blockByHashWithoutFullTxHashFixture,
		BlockTime: "invalid",
	}

	entries := make([]interface{}, 1)
	entries[0] = expectedBlockEntry

	s.collectionStorage.EXPECT().QueryItemByMaxSortKey(
		gomock.Any(),
		blockByHashWithoutFullTxPartitionKeyFixture,
		blockByHashWithoutFullTxSortKeyFixture,
		models.BlockByHashWithoutFullTxDDBEntry{},
		nil,
	).Return(&collection.QueryItemsResult{Items: entries, LastEvaluatedKey: nil}, nil)

	block, err := s.storage.GetBlockByHashWithoutFullTx(ctx, blockByHashWithoutFullTxTagFixture, blockByHashWithoutFullTxHashFixture, blockByHashWithoutFullTxSequenceFixture)
	require.Error(err)
	require.Nil(block)
}

func (s *BlockStorageByHashWithoutFullTxTestSuite) TestGetBlockByHashWithoutFullTx_LargeFile() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	objectKey := "s3-location"
	data := testutil.MakeFile(10)
	compressed, err := compression.Compress(data, compression.CompressionGzip)
	require.NoError(err)
	blockTime := testutil.MustTime(txBlockTimeFixture)

	expectedBlockEntry := &models.BlockByHashWithoutFullTxDDBEntry{
		BaseItem: collection.NewBaseItem(
			blockByHashWithoutFullTxPartitionKeyFixture,
			blockByHashWithoutFullTxSortKeyFixture,
			blockByHashWithoutFullTxTagFixture,
		).WithObjectKey(objectKey),
		Height:    blockByHashWithoutFullTxHeightFixture,
		Hash:      blockByHashWithoutFullTxHashFixture,
		BlockTime: blockByHashWithoutFullTxBlockTimeFixture,
	}

	entries := make([]interface{}, 1)
	entries[0] = expectedBlockEntry

	expectedBlock := &ethereum.Block{
		Tag:       blockByHashWithoutFullTxTagFixture,
		Height:    blockByHashWithoutFullTxHeightFixture,
		Hash:      blockByHashWithoutFullTxHashFixture,
		Sequence:  blockByHashWithoutFullTxSequenceFixture,
		Data:      data,
		BlockTime: blockTime,
		UpdatedAt: testutil.MustTime(expectedBlockEntry.UpdatedAt),
	}

	s.collectionStorage.EXPECT().QueryItemByMaxSortKey(
		gomock.Any(),
		blockByHashWithoutFullTxPartitionKeyFixture,
		blockByHashWithoutFullTxSortKeyFixture,
		models.BlockByHashWithoutFullTxDDBEntry{},
		nil,
	).Return(&collection.QueryItemsResult{Items: entries, LastEvaluatedKey: nil}, nil)

	s.collectionStorage.EXPECT().
		DownloadFromBlobStorage(gomock.Any(), expectedBlockEntry).
		DoAndReturn(func(ctx context.Context, entry collection.Item) error {
			err := entry.SetData(compressed)
			require.NoError(err)
			return nil
		})

	block, err := s.storage.GetBlockByHashWithoutFullTx(ctx, blockByHashWithoutFullTxTagFixture, blockByHashWithoutFullTxHashFixture, blockByHashWithoutFullTxSequenceFixture)
	require.NoError(err)
	require.Equal(expectedBlock, block)
}

func (s *BlockStorageByHashWithoutFullTxTestSuite) TestGetBlockByHashWithoutFullTx_LargeFileDownloadFailure() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	objectKey := "s3-location"

	expectedBlockEntry := &models.BlockByHashWithoutFullTxDDBEntry{
		BaseItem: collection.NewBaseItem(
			blockByHashWithoutFullTxPartitionKeyFixture,
			blockByHashWithoutFullTxSortKeyFixture,
			blockByHashWithoutFullTxTagFixture,
		).WithObjectKey(objectKey),
		Height:    blockByHashWithoutFullTxHeightFixture,
		Hash:      blockByHashWithoutFullTxHashFixture,
		BlockTime: blockByHashWithoutFullTxBlockTimeFixture,
	}

	entries := make([]interface{}, 1)
	entries[0] = expectedBlockEntry
	s.collectionStorage.EXPECT().QueryItemByMaxSortKey(
		gomock.Any(),
		blockByHashWithoutFullTxPartitionKeyFixture,
		blockByHashWithoutFullTxSortKeyFixture,
		models.BlockByHashWithoutFullTxDDBEntry{},
		nil,
	).Return(&collection.QueryItemsResult{Items: entries, LastEvaluatedKey: nil}, nil)

	s.collectionStorage.EXPECT().DownloadFromBlobStorage(gomock.Any(), expectedBlockEntry).
		Return(xerrors.Errorf("failed to download"))

	block, err := s.storage.GetBlockByHashWithoutFullTx(ctx, blockByHashWithoutFullTxTagFixture, blockByHashWithoutFullTxHashFixture, blockByHashWithoutFullTxSequenceFixture)
	require.Error(err)
	require.Nil(block)
}
