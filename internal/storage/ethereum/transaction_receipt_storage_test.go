package ethereum

import (
	"context"
	"fmt"
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

type transactionReceiptStorageTestSuite struct {
	suite.Suite
	ctrl              *gomock.Controller
	collectionStorage *cmocks.MockCollectionStorage
	storage           TransactionReceiptStorage
	app               testapp.TestApp

	blockTimeFixture time.Time
}

func TestTransactionReceiptStorageTestSuite(t *testing.T) {
	suite.Run(t, new(transactionReceiptStorageTestSuite))
}

const (
	receiptTagFixture          = uint32(1)
	receiptSequenceFixture     = api.Sequence(12312)
	receiptHashFixture         = "0xaa6d"
	receiptPartitionKeyFixture = "1#transaction-receipts#0xaa6d"
	receiptSortKeyFixture      = "0000000000003018"
	receiptBlockTimeFixture    = "2020-11-24T16:07:21Z"
)

func (s *transactionReceiptStorageTestSuite) SetupTest() {
	s.ctrl = gomock.NewController(s.T())
	s.collectionStorage = cmocks.NewMockCollectionStorage(s.ctrl)
	s.collectionStorage.EXPECT().
		WithCollection(ethereum.CollectionTransactionReceipts).
		Return(s.collectionStorage)
	logsStorage := cmocks.NewMockCollectionStorage(s.ctrl)

	s.app = testapp.New(
		s.T(),
		fx.Provide(newTransactionReceiptStorage),
		fx.Provide(fx.Annotated{Name: "collection", Target: func() collection.CollectionStorage { return s.collectionStorage }}),
		fx.Provide(fx.Annotated{Name: "logs", Target: func() collection.CollectionStorage { return logsStorage }}),
		fx.Populate(&s.storage),
	)
	s.blockTimeFixture = testutil.MustTime(receiptBlockTimeFixture)
}

func (s *transactionReceiptStorageTestSuite) TearDownTest() {
	s.app.Close()
	s.ctrl.Finish()
}

func (s *transactionReceiptStorageTestSuite) TestPersistTransactionReceipt() {
	require := testutil.Require(s.T())

	data := []byte("asdqewdw")
	compressed, err := compression.Compress(data, compression.CompressionGzip)
	require.NoError(err)

	baseItem := collection.NewBaseItem(
		receiptPartitionKeyFixture,
		receiptSortKeyFixture,
		receiptTagFixture,
	).WithData(compressed)
	expectedEntry := &models.TransactionReceiptDDBEntry{
		BaseItem:  baseItem,
		Hash:      receiptHashFixture,
		BlockTime: receiptBlockTimeFixture,
	}

	s.collectionStorage.EXPECT().
		UploadToBlobStorage(gomock.Any(), gomock.Any(), false).Return(nil)
	s.collectionStorage.EXPECT().WriteItem(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, entry interface{}) error {
			actualEntry, ok := entry.(*models.TransactionReceiptDDBEntry)
			require.True(ok)
			testutil.MustTime(actualEntry.UpdatedAt)
			expectedEntry.UpdatedAt = actualEntry.UpdatedAt
			require.Equal(expectedEntry, actualEntry)
			return nil
		})
	err = s.storage.PersistTransactionReceipt(context.Background(), &ethereum.TransactionReceipt{
		Tag:       receiptTagFixture,
		Hash:      receiptHashFixture,
		Sequence:  receiptSequenceFixture,
		Data:      data,
		BlockTime: s.blockTimeFixture,
	})
	require.NoError(err)
}

func (s *transactionReceiptStorageTestSuite) TestPersistTransactionReceipt_NoBlockTime() {
	require := testutil.Require(s.T())

	data := []byte("asdqewdw")
	compressed, err := compression.Compress(data, compression.CompressionGzip)
	require.NoError(err)

	baseItem := collection.NewBaseItem(
		receiptPartitionKeyFixture,
		receiptSortKeyFixture,
		receiptTagFixture,
	).WithData(compressed)
	expectedEntry := &models.TransactionReceiptDDBEntry{
		BaseItem: baseItem,
		Hash:     receiptHashFixture,
	}

	s.collectionStorage.EXPECT().
		UploadToBlobStorage(gomock.Any(), gomock.Any(), false).Return(nil)
	s.collectionStorage.EXPECT().WriteItem(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, entry interface{}) error {
			actualEntry, ok := entry.(*models.TransactionReceiptDDBEntry)
			require.True(ok)
			testutil.MustTime(actualEntry.UpdatedAt)
			expectedEntry.UpdatedAt = actualEntry.UpdatedAt
			require.Equal(expectedEntry, actualEntry)
			return nil
		})
	err = s.storage.PersistTransactionReceipt(context.Background(), &ethereum.TransactionReceipt{
		Tag:      receiptTagFixture,
		Hash:     receiptHashFixture,
		Sequence: receiptSequenceFixture,
		Data:     data,
	})
	require.NoError(err)
}

func (s *transactionReceiptStorageTestSuite) TestPersistTransactionReceipts() {
	require := testutil.Require(s.T())

	hashes := []string{"hash1", "hash2"}
	data := [][]byte{[]byte("data-1"), []byte("data-2")}
	compressed := make([][]byte, len(data))
	for i, d := range data {
		c, err := compression.Compress(d, compression.CompressionGzip)
		require.NoError(err)
		compressed[i] = c
	}

	expectedEntries := make([]*models.TransactionReceiptDDBEntry, len(data))
	for i := range data {
		baseItem := collection.NewBaseItem(
			fmt.Sprintf("1#transaction-receipts#%s", hashes[i]),
			receiptSortKeyFixture,
			receiptTagFixture,
		).WithData(compressed[i])
		entry := &models.TransactionReceiptDDBEntry{
			BaseItem:  baseItem,
			Hash:      hashes[i],
			BlockTime: receiptBlockTimeFixture,
		}
		expectedEntries[i] = entry
	}

	receipts := make([]*ethereum.TransactionReceipt, len(data))
	for i := range data {
		receipts[i] = &ethereum.TransactionReceipt{
			Tag:       receiptTagFixture,
			Hash:      hashes[i],
			Sequence:  receiptSequenceFixture,
			Data:      data[i],
			BlockTime: s.blockTimeFixture,
		}
	}

	s.collectionStorage.EXPECT().
		UploadToBlobStorage(gomock.Any(), gomock.Any(), false).Return(nil).Times(2)
	s.collectionStorage.EXPECT().WriteItems(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, entries []interface{}) error {
			for i, entry := range entries {
				expectedEntry := expectedEntries[i]
				actualEntry, ok := entry.(*models.TransactionReceiptDDBEntry)
				require.True(ok)
				testutil.MustTime(actualEntry.UpdatedAt)
				expectedEntry.UpdatedAt = actualEntry.UpdatedAt
				require.Equal(expectedEntry, actualEntry)
			}
			return nil
		})
	err := s.storage.PersistTransactionReceipts(context.Background(), receipts)
	require.NoError(err)
}

func (s *transactionReceiptStorageTestSuite) TestPersistTransactionReceipt_LargeFile() {
	require := testutil.Require(s.T())

	data := []byte("asdqewdw")

	objectKey := "1/transaction-receipts/0xaa6d/12312"
	entryAfterUpload := &models.TransactionReceiptDDBEntry{
		BaseItem:  collection.NewBaseItem(receiptPartitionKeyFixture, receiptSortKeyFixture, receiptTagFixture).WithObjectKey(objectKey),
		Hash:      receiptHashFixture,
		BlockTime: receiptBlockTimeFixture,
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
			actualEntry, ok := entry.(*models.TransactionReceiptDDBEntry)
			require.True(ok)
			testutil.MustTime(actualEntry.UpdatedAt)
			entryAfterUpload.UpdatedAt = actualEntry.UpdatedAt
			require.Equal(entryAfterUpload, actualEntry)
			return nil
		})
	err := s.storage.PersistTransactionReceipt(context.Background(), &ethereum.TransactionReceipt{
		Tag:       receiptTagFixture,
		Hash:      receiptHashFixture,
		Sequence:  receiptSequenceFixture,
		Data:      data,
		BlockTime: s.blockTimeFixture,
	})
	require.NoError(err)
}

func (s *transactionReceiptStorageTestSuite) TestPersistTransactionReceipts_LargeFile() {
	require := testutil.Require(s.T())

	hashes := []string{"hash1", "hash2"}
	data := [][]byte{[]byte("data-1"), []byte("data-2")}
	compressed := make([][]byte, len(data))
	for i, d := range data {
		c, err := compression.Compress(d, compression.CompressionGzip)
		require.NoError(err)
		compressed[i] = c
	}

	entriesAfterUpload := make([]*models.TransactionReceiptDDBEntry, len(data))
	for i := range data {
		baseItem := collection.NewBaseItem(
			fmt.Sprintf("1#transaction-receipts#%s", hashes[i]),
			receiptSortKeyFixture,
			receiptTagFixture,
		).WithObjectKey(fmt.Sprintf("1/transaction-receipts/%s/12312", hashes[i]))
		entry := &models.TransactionReceiptDDBEntry{
			BaseItem:  baseItem,
			Hash:      hashes[i],
			BlockTime: logsBlockTimeFixture,
		}
		entriesAfterUpload[i] = entry
	}

	receipts := make([]*ethereum.TransactionReceipt, len(data))
	for i := range data {
		receipts[i] = &ethereum.TransactionReceipt{
			Tag:       receiptTagFixture,
			Hash:      hashes[i],
			Sequence:  receiptSequenceFixture,
			Data:      data[i],
			BlockTime: s.blockTimeFixture,
		}
	}

	s.collectionStorage.EXPECT().
		UploadToBlobStorage(gomock.Any(), gomock.Any(), false).
		DoAndReturn(func(ctx context.Context, entry collection.Item, enforceUpload bool) error {
			err := entry.SetData(nil)
			require.NoError(err)
			objectKey, err := entry.MakeObjectKey()
			require.NoError(err)
			err = entry.SetObjectKey(objectKey)
			require.NoError(err)
			return nil
		}).Times(2)
	s.collectionStorage.EXPECT().WriteItems(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, entries []interface{}) error {
			for i, entry := range entries {
				expectedEntry := entriesAfterUpload[i]
				actualEntry, ok := entry.(*models.TransactionReceiptDDBEntry)
				require.True(ok)
				testutil.MustTime(actualEntry.UpdatedAt)
				expectedEntry.UpdatedAt = actualEntry.UpdatedAt
				require.Equal(expectedEntry, actualEntry)
			}
			return nil
		})
	err := s.storage.PersistTransactionReceipts(context.Background(), receipts)
	require.NoError(err)
}

func (s *transactionReceiptStorageTestSuite) TestPersistTransactionReceipt_LargeFileUploadError() {
	require := testutil.Require(s.T())

	data := []byte("asdqewdw")

	s.collectionStorage.EXPECT().
		UploadToBlobStorage(gomock.Any(), gomock.Any(), false).
		Return(xerrors.Errorf("failed to upload"))
	err := s.storage.PersistTransactionReceipts(context.Background(), []*ethereum.TransactionReceipt{
		{
			Tag:       receiptTagFixture,
			Hash:      receiptHashFixture,
			Sequence:  receiptSequenceFixture,
			Data:      data,
			BlockTime: s.blockTimeFixture,
		},
	})
	require.Error(err)
}

func (s *transactionReceiptStorageTestSuite) TestGetTransactionReceipt() {
	require := testutil.Require(s.T())

	data := []byte("asdqewdw")
	compressed, err := compression.Compress(data, compression.CompressionGzip)
	require.NoError(err)

	expectedTransactionReceiptEntry := &models.TransactionReceiptDDBEntry{
		BaseItem: collection.NewBaseItem(
			receiptPartitionKeyFixture, receiptSortKeyFixture, receiptTagFixture,
		).WithData(compressed),
		Hash:      receiptHashFixture,
		BlockTime: receiptBlockTimeFixture,
	}

	entries := make([]interface{}, 1)
	entries[0] = expectedTransactionReceiptEntry

	s.collectionStorage.EXPECT().QueryItemByMaxSortKey(
		gomock.Any(),
		receiptPartitionKeyFixture,
		receiptSortKeyFixture,
		models.TransactionReceiptDDBEntry{},
		nil,
	).Return(&collection.QueryItemsResult{Items: entries, LastEvaluatedKey: nil}, nil)

	transaction, err := s.storage.GetTransactionReceipt(context.Background(), receiptTagFixture, receiptHashFixture, receiptSequenceFixture)
	require.NoError(err)
	require.Equal(&ethereum.TransactionReceipt{
		Tag:       receiptTagFixture,
		Hash:      receiptHashFixture,
		Sequence:  receiptSequenceFixture,
		Data:      data,
		BlockTime: s.blockTimeFixture,
		UpdatedAt: testutil.MustTime(expectedTransactionReceiptEntry.UpdatedAt),
	}, transaction)
}

func (s *transactionReceiptStorageTestSuite) TestGetTransactionReceipt_NoBlockTime() {
	require := testutil.Require(s.T())

	data := []byte("asdqewdw")
	compressed, err := compression.Compress(data, compression.CompressionGzip)
	require.NoError(err)

	expectedTransactionReceiptEntry := &models.TransactionReceiptDDBEntry{
		BaseItem: collection.NewBaseItem(
			receiptPartitionKeyFixture, receiptSortKeyFixture, receiptTagFixture,
		).WithData(compressed),
		Hash: receiptHashFixture,
	}

	entries := make([]interface{}, 1)
	entries[0] = expectedTransactionReceiptEntry

	s.collectionStorage.EXPECT().QueryItemByMaxSortKey(
		gomock.Any(),
		receiptPartitionKeyFixture,
		receiptSortKeyFixture,
		models.TransactionReceiptDDBEntry{},
		nil,
	).Return(&collection.QueryItemsResult{Items: entries, LastEvaluatedKey: nil}, nil)

	transaction, err := s.storage.GetTransactionReceipt(context.Background(), receiptTagFixture, receiptHashFixture, receiptSequenceFixture)
	require.NoError(err)
	require.Equal(&ethereum.TransactionReceipt{
		Tag:       receiptTagFixture,
		Hash:      receiptHashFixture,
		Sequence:  receiptSequenceFixture,
		Data:      data,
		UpdatedAt: testutil.MustTime(expectedTransactionReceiptEntry.UpdatedAt),
	}, transaction)
}

func (s *transactionReceiptStorageTestSuite) TestGetTransactionReceipt_InvalidBlockTime() {
	require := testutil.Require(s.T())

	data := []byte("asdqewdw")
	compressed, err := compression.Compress(data, compression.CompressionGzip)
	require.NoError(err)

	expectedTransactionReceiptEntry := &models.TransactionReceiptDDBEntry{
		BaseItem: collection.NewBaseItem(
			receiptPartitionKeyFixture, receiptSortKeyFixture, receiptTagFixture,
		).WithData(compressed),
		Hash:      receiptHashFixture,
		BlockTime: "invalid",
	}

	entries := make([]interface{}, 1)
	entries[0] = expectedTransactionReceiptEntry

	s.collectionStorage.EXPECT().QueryItemByMaxSortKey(
		gomock.Any(),
		receiptPartitionKeyFixture,
		receiptSortKeyFixture,
		models.TransactionReceiptDDBEntry{},
		nil,
	).Return(&collection.QueryItemsResult{Items: entries, LastEvaluatedKey: nil}, nil)

	transaction, err := s.storage.GetTransactionReceipt(context.Background(), receiptTagFixture, receiptHashFixture, receiptSequenceFixture)
	require.Error(err)
	require.Nil(transaction)
}

func (s *transactionReceiptStorageTestSuite) TestGetTransactionReceipt_LargeFile() {
	require := testutil.Require(s.T())

	data := []byte("asdqewdw")
	compressed, err := compression.Compress(data, compression.CompressionGzip)
	require.NoError(err)

	objectKey := "s3-location"
	expectedTransactionReceiptEntry := &models.TransactionReceiptDDBEntry{
		BaseItem:  collection.NewBaseItem(receiptPartitionKeyFixture, receiptSortKeyFixture, receiptTagFixture).WithObjectKey(objectKey),
		Hash:      receiptHashFixture,
		BlockTime: receiptBlockTimeFixture,
	}

	entries := make([]interface{}, 1)
	entries[0] = expectedTransactionReceiptEntry

	s.collectionStorage.EXPECT().QueryItemByMaxSortKey(
		gomock.Any(),
		receiptPartitionKeyFixture,
		receiptSortKeyFixture,
		models.TransactionReceiptDDBEntry{},
		nil,
	).Return(&collection.QueryItemsResult{Items: entries, LastEvaluatedKey: nil}, nil)

	s.collectionStorage.EXPECT().
		DownloadFromBlobStorage(gomock.Any(), expectedTransactionReceiptEntry).
		DoAndReturn(func(ctx context.Context, entry collection.Item) error {
			err := entry.SetData(compressed)
			require.NoError(err)
			return nil
		})

	transaction, err := s.storage.GetTransactionReceipt(context.Background(), receiptTagFixture, receiptHashFixture, receiptSequenceFixture)
	require.NoError(err)
	require.Equal(&ethereum.TransactionReceipt{
		Tag:       receiptTagFixture,
		Hash:      receiptHashFixture,
		Sequence:  receiptSequenceFixture,
		Data:      data,
		BlockTime: s.blockTimeFixture,
		UpdatedAt: testutil.MustTime(expectedTransactionReceiptEntry.UpdatedAt),
	}, transaction)
}

func (s *transactionReceiptStorageTestSuite) TestGetTransactionReceipt_LargeFileDownloadFailure() {
	require := testutil.Require(s.T())

	objectKey := "s3-location"
	expectedTransactionReceiptEntry := &models.TransactionReceiptDDBEntry{
		BaseItem:  collection.NewBaseItem(receiptPartitionKeyFixture, receiptSortKeyFixture, receiptTagFixture).WithObjectKey(objectKey),
		Hash:      receiptHashFixture,
		BlockTime: receiptBlockTimeFixture,
	}

	entries := make([]interface{}, 1)
	entries[0] = expectedTransactionReceiptEntry

	s.collectionStorage.EXPECT().QueryItemByMaxSortKey(
		gomock.Any(),
		receiptPartitionKeyFixture,
		receiptSortKeyFixture,
		models.TransactionReceiptDDBEntry{},
		nil,
	).Return(&collection.QueryItemsResult{Items: entries, LastEvaluatedKey: nil}, nil)

	s.collectionStorage.EXPECT().DownloadFromBlobStorage(gomock.Any(), expectedTransactionReceiptEntry).
		Return(xerrors.Errorf("failed to download"))

	transaction, err := s.storage.GetTransactionReceipt(context.Background(), receiptTagFixture, receiptHashFixture, receiptSequenceFixture)
	require.Error(err)
	require.Nil(transaction)
}
