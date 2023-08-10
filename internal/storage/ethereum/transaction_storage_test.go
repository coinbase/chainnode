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

type transactionStorageTestSuite struct {
	suite.Suite
	ctrl              *gomock.Controller
	collectionStorage *cmocks.MockCollectionStorage
	storage           TransactionStorage
	app               testapp.TestApp
}

const (
	txTagFixture          = uint32(1)
	txSequenceFixture     = api.Sequence(12312)
	txHashFixture         = "0xaa6d"
	txPartitionKeyFixture = "1#transactions-by-hash#0xaa6d"
	txSortKeyFixture      = "0000000000003018"
	txBlockTimeFixture    = "2020-11-24T16:07:21Z"
)

func TestTransactionStorageTestSuite(t *testing.T) {
	suite.Run(t, new(transactionStorageTestSuite))
}

func (s *transactionStorageTestSuite) SetupTest() {
	s.ctrl = gomock.NewController(s.T())
	s.collectionStorage = cmocks.NewMockCollectionStorage(s.ctrl)
	s.collectionStorage.EXPECT().
		WithCollection(ethereum.CollectionTransactions).
		Return(s.collectionStorage)
	logsStorage := cmocks.NewMockCollectionStorage(s.ctrl)

	s.app = testapp.New(
		s.T(),
		fx.Provide(newTransactionStorage),
		fx.Provide(fx.Annotated{Name: "collection", Target: func() collection.CollectionStorage { return s.collectionStorage }}),
		fx.Provide(fx.Annotated{Name: "logs", Target: func() collection.CollectionStorage { return logsStorage }}),
		fx.Populate(&s.storage),
	)
}

func (s *transactionStorageTestSuite) TearDownTest() {
	s.app.Close()
	s.ctrl.Finish()
}

func (s *transactionStorageTestSuite) TestPersistTransactions() {
	require := testutil.Require(s.T())

	hashes := []string{"hash1", "hash2"}
	data := [][]byte{[]byte("data-1"), []byte("data-2")}
	compressed := make([][]byte, len(data))
	for i, d := range data {
		c, err := compression.Compress(d, compression.CompressionGzip)
		require.NoError(err)
		compressed[i] = c
	}
	blockTime := testutil.MustTime("2020-11-24T16:07:21Z")

	expectedEntries := make([]*models.TransactionByHashDDBEntry, len(data))
	for i, _ := range data {
		entry := &models.TransactionByHashDDBEntry{
			BaseItem: collection.NewBaseItem(
				fmt.Sprintf("1#transactions-by-hash#%s", hashes[i]),
				txSortKeyFixture,
				txTagFixture,
			).WithData(compressed[i]),
			Hash:      hashes[i],
			BlockTime: txBlockTimeFixture,
		}
		expectedEntries[i] = entry
	}

	s.collectionStorage.EXPECT().
		UploadToBlobStorage(gomock.Any(), gomock.Any(), false).Return(nil).Times(2)
	s.collectionStorage.EXPECT().WriteItems(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, entries []interface{}) error {
			for i, entry := range entries {
				expectedEntry := expectedEntries[i]
				actualEntry, ok := entry.(*models.TransactionByHashDDBEntry)
				require.True(ok)
				testutil.MustTime(actualEntry.UpdatedAt)
				expectedEntry.UpdatedAt = actualEntry.UpdatedAt
				require.Equal(expectedEntry, actualEntry)
			}
			return nil
		})

	txs := make([]*ethereum.Transaction, len(data))
	for i := range data {
		txs[i] = ethereum.NewTransaction(txTagFixture, hashes[i], txSequenceFixture, data[i], blockTime)
	}

	err := s.storage.PersistTransactions(context.Background(), txs)
	require.NoError(err)
}

func (s *transactionStorageTestSuite) TestPersistTransactions_NoBlockTime() {
	require := testutil.Require(s.T())

	hashes := []string{"hash1", "hash2"}
	data := [][]byte{[]byte("data-1"), []byte("data-2")}
	compressed := make([][]byte, len(data))
	for i, d := range data {
		c, err := compression.Compress(d, compression.CompressionGzip)
		require.NoError(err)
		compressed[i] = c
	}

	expectedEntries := make([]*models.TransactionByHashDDBEntry, len(data))
	for i, _ := range data {
		entry := &models.TransactionByHashDDBEntry{
			BaseItem: collection.NewBaseItem(
				fmt.Sprintf("1#transactions-by-hash#%s", hashes[i]),
				txSortKeyFixture,
				txTagFixture,
			).WithData(compressed[i]),
			Hash: hashes[i],
		}
		expectedEntries[i] = entry
	}

	s.collectionStorage.EXPECT().
		UploadToBlobStorage(gomock.Any(), gomock.Any(), false).Return(nil).Times(2)
	s.collectionStorage.EXPECT().WriteItems(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, entries []interface{}) error {
			for i, entry := range entries {
				expectedEntry := expectedEntries[i]
				actualEntry, ok := entry.(*models.TransactionByHashDDBEntry)
				require.True(ok)
				testutil.MustTime(actualEntry.UpdatedAt)
				expectedEntry.UpdatedAt = actualEntry.UpdatedAt
				require.Equal(expectedEntry, actualEntry)
			}
			return nil
		})

	txs := make([]*ethereum.Transaction, len(data))
	for i := range data {
		txs[i] = ethereum.NewTransaction(txTagFixture, hashes[i], txSequenceFixture, data[i], time.Time{})
	}

	err := s.storage.PersistTransactions(context.Background(), txs)
	require.NoError(err)
}

func (s *transactionStorageTestSuite) TestPersistTransactions_LargeFile() {
	require := testutil.Require(s.T())

	hashes := []string{"hash1", "hash2"}
	data := [][]byte{[]byte("data-1"), []byte("data-2")}
	blockTime := testutil.MustTime("2020-11-24T16:07:21Z")
	compressed := make([][]byte, len(data))
	for i, d := range data {
		c, err := compression.Compress(d, compression.CompressionGzip)
		require.NoError(err)
		compressed[i] = c
	}

	expectedEntries := make([]*models.TransactionByHashDDBEntry, len(data))
	for i, _ := range data {
		entry := &models.TransactionByHashDDBEntry{
			BaseItem: collection.NewBaseItem(
				fmt.Sprintf("1#transactions-by-hash#%s", hashes[i]),
				txSortKeyFixture,
				txTagFixture,
			).WithObjectKey(fmt.Sprintf("1/transactions-by-hash/%s/12312", hashes[i])),
			Hash:      hashes[i],
			BlockTime: txBlockTimeFixture,
		}
		expectedEntries[i] = entry
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
				expectedEntry := expectedEntries[i]
				actualEntry, ok := entry.(*models.TransactionByHashDDBEntry)
				require.True(ok)
				testutil.MustTime(actualEntry.UpdatedAt)
				expectedEntry.UpdatedAt = actualEntry.UpdatedAt
				require.Equal(expectedEntry, actualEntry)
			}
			return nil
		})
	txs := make([]*ethereum.Transaction, len(data))
	for i := range data {
		txs[i] = ethereum.NewTransaction(txTagFixture, hashes[i], txSequenceFixture, data[i], blockTime)
	}
	err := s.storage.PersistTransactions(context.Background(), txs)
	require.NoError(err)
}

func (s *transactionStorageTestSuite) TestPersistTransaction_LargeFileUploadError() {
	require := testutil.Require(s.T())

	data := []byte("asdqewdw")
	blockTime := testutil.MustTime("2020-11-24T16:07:21Z")
	s.collectionStorage.EXPECT().
		UploadToBlobStorage(gomock.Any(), gomock.Any(), false).
		Return(xerrors.Errorf("failed to upload"))

	transaction := ethereum.NewTransaction(txTagFixture, txHashFixture, txSequenceFixture, data, blockTime)
	err := s.storage.PersistTransactions(context.Background(), []*ethereum.Transaction{transaction})
	require.Error(err)
}

func (s *transactionStorageTestSuite) TestGetTransactionByHash() {
	require := testutil.Require(s.T())

	data := []byte("asdqewdw")
	compressed, err := compression.Compress(data, compression.CompressionGzip)
	require.NoError(err)
	blockTime := testutil.MustTime(txBlockTimeFixture)

	expectedTransactionEntry := &models.TransactionByHashDDBEntry{
		BaseItem:  collection.NewBaseItem(txPartitionKeyFixture, txSortKeyFixture, txTagFixture).WithData(compressed),
		Hash:      txHashFixture,
		BlockTime: txBlockTimeFixture,
	}
	entries := make([]interface{}, 1)
	entries[0] = expectedTransactionEntry

	expectedTransaction := &ethereum.Transaction{
		Tag:       txTagFixture,
		Hash:      txHashFixture,
		Sequence:  txSequenceFixture,
		Data:      data,
		BlockTime: blockTime,
		UpdatedAt: testutil.MustTime(expectedTransactionEntry.UpdatedAt),
	}

	s.collectionStorage.EXPECT().QueryItemByMaxSortKey(
		gomock.Any(),
		txPartitionKeyFixture,
		txSortKeyFixture,
		models.TransactionByHashDDBEntry{},
		nil,
	).Return(&collection.QueryItemsResult{Items: entries, LastEvaluatedKey: nil}, nil)
	transaction, err := s.storage.GetTransactionByHash(
		context.Background(), txTagFixture, txHashFixture, txSequenceFixture)
	require.NoError(err)
	require.Equal(expectedTransaction, transaction)
}

func (s *transactionStorageTestSuite) TestGetTransactionByHash_NoBlockTime() {
	require := testutil.Require(s.T())

	data := []byte("asdqewdw")
	compressed, err := compression.Compress(data, compression.CompressionGzip)
	require.NoError(err)

	expectedTransactionEntry := &models.TransactionByHashDDBEntry{
		BaseItem: collection.NewBaseItem(txPartitionKeyFixture, txSortKeyFixture, txTagFixture).WithData(compressed),
		Hash:     txHashFixture,
	}

	entries := make([]interface{}, 1)
	entries[0] = expectedTransactionEntry

	expectedTransaction := &ethereum.Transaction{
		Tag:       txTagFixture,
		Hash:      txHashFixture,
		Sequence:  txSequenceFixture,
		Data:      data,
		UpdatedAt: testutil.MustTime(expectedTransactionEntry.UpdatedAt),
	}

	s.collectionStorage.EXPECT().QueryItemByMaxSortKey(
		gomock.Any(),
		txPartitionKeyFixture,
		txSortKeyFixture,
		models.TransactionByHashDDBEntry{},
		nil,
	).Return(&collection.QueryItemsResult{Items: entries, LastEvaluatedKey: nil}, nil)
	transaction, err := s.storage.GetTransactionByHash(
		context.Background(), txTagFixture, txHashFixture, txSequenceFixture)
	require.NoError(err)
	require.Equal(expectedTransaction, transaction)
}

func (s *transactionStorageTestSuite) TestGetTransactionByHash_InvalidBlockTime() {
	require := testutil.Require(s.T())

	data := []byte("asdqewdw")
	compressed, err := compression.Compress(data, compression.CompressionGzip)
	require.NoError(err)

	expectedTransactionEntry := &models.TransactionByHashDDBEntry{
		BaseItem:  collection.NewBaseItem(txPartitionKeyFixture, txSortKeyFixture, txTagFixture).WithData(compressed),
		Hash:      txHashFixture,
		BlockTime: "invalid",
	}

	entries := make([]interface{}, 1)
	entries[0] = expectedTransactionEntry

	s.collectionStorage.EXPECT().QueryItemByMaxSortKey(
		gomock.Any(),
		txPartitionKeyFixture,
		txSortKeyFixture,
		models.TransactionByHashDDBEntry{},
		nil,
	).Return(&collection.QueryItemsResult{Items: entries, LastEvaluatedKey: nil}, nil)
	transaction, err := s.storage.GetTransactionByHash(
		context.Background(), txTagFixture, txHashFixture, txSequenceFixture)
	require.Error(err)
	require.Nil(transaction)
}

func (s *transactionStorageTestSuite) TestGetTransactionByHash_LargeFile() {
	require := testutil.Require(s.T())

	data := []byte("asdqewdw")
	blockTime := testutil.MustTime(txBlockTimeFixture)
	compressed, err := compression.Compress(data, compression.CompressionGzip)
	require.NoError(err)

	objectKey := "s3-location"
	expectedTransactionEntry := &models.TransactionByHashDDBEntry{
		BaseItem:  collection.NewBaseItem(txPartitionKeyFixture, txSortKeyFixture, txTagFixture).WithObjectKey(objectKey),
		Hash:      txHashFixture,
		BlockTime: txBlockTimeFixture,
	}

	entries := make([]interface{}, 1)
	entries[0] = expectedTransactionEntry

	expectedTransaction := &ethereum.Transaction{
		Tag:       txTagFixture,
		Hash:      txHashFixture,
		Sequence:  txSequenceFixture,
		Data:      data,
		BlockTime: blockTime,
		UpdatedAt: testutil.MustTime(expectedTransactionEntry.UpdatedAt),
	}

	s.collectionStorage.EXPECT().QueryItemByMaxSortKey(
		gomock.Any(),
		txPartitionKeyFixture,
		txSortKeyFixture,
		models.TransactionByHashDDBEntry{},
		nil,
	).Return(&collection.QueryItemsResult{Items: entries, LastEvaluatedKey: nil}, nil)
	s.collectionStorage.EXPECT().
		DownloadFromBlobStorage(gomock.Any(), expectedTransactionEntry).
		DoAndReturn(func(ctx context.Context, entry collection.Item) error {
			err := entry.SetData(compressed)
			require.NoError(err)
			return nil
		})

	transaction, err := s.storage.GetTransactionByHash(
		context.Background(), txTagFixture, txHashFixture, txSequenceFixture)
	require.NoError(err)
	require.Equal(expectedTransaction, transaction)
}

func (s *transactionStorageTestSuite) TestGetTransactionByHash_LargeFileDownloadFailure() {
	require := testutil.Require(s.T())

	objectKey := "s3-location"
	expectedTransactionEntry := &models.TransactionByHashDDBEntry{
		BaseItem:  collection.NewBaseItem(txPartitionKeyFixture, txSortKeyFixture, txTagFixture).WithObjectKey(objectKey),
		Hash:      txHashFixture,
		BlockTime: txBlockTimeFixture,
	}

	entries := make([]interface{}, 1)
	entries[0] = expectedTransactionEntry

	s.collectionStorage.EXPECT().QueryItemByMaxSortKey(
		gomock.Any(),
		txPartitionKeyFixture,
		txSortKeyFixture,
		models.TransactionByHashDDBEntry{},
		nil,
	).Return(&collection.QueryItemsResult{Items: entries, LastEvaluatedKey: nil}, nil)

	s.collectionStorage.EXPECT().DownloadFromBlobStorage(gomock.Any(), gomock.Any()).
		Return(xerrors.Errorf("failed to download"))

	transaction, err := s.storage.GetTransactionByHash(
		context.Background(), txTagFixture, txHashFixture, txSequenceFixture)
	require.Error(err)
	require.Nil(transaction)
}
