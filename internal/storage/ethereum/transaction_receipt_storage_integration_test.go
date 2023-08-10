package ethereum

import (
	"context"
	"testing"

	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainnode/internal/api"
	"github.com/coinbase/chainnode/internal/api/ethereum"
	"github.com/coinbase/chainnode/internal/config"
	"github.com/coinbase/chainnode/internal/storage/blob"
	"github.com/coinbase/chainnode/internal/storage/collection"
	"github.com/coinbase/chainnode/internal/storage/internal"
	"github.com/coinbase/chainnode/internal/storage/s3"
	"github.com/coinbase/chainnode/internal/utils/testapp"
	"github.com/coinbase/chainnode/internal/utils/testutil"
)

type transactionReceiptStorageIntegrationTestSuite struct {
	suite.Suite
	storage TransactionReceiptStorage
	cfg     *config.Config
	app     testapp.TestApp
}

func (s *transactionReceiptStorageIntegrationTestSuite) SetupTest() {
	require := testutil.Require(s.T())

	cfg, err := config.New()
	require.NoError(err)
	s.cfg = cfg
	var storage TransactionReceiptStorage
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

func (s *transactionReceiptStorageIntegrationTestSuite) TearDownTest() {
	if s.app != nil {
		s.app.Close()
	}
}

func TestIntegrationTransactionReceiptStorateTestSuite(t *testing.T) {
	suite.Run(t, new(transactionReceiptStorageIntegrationTestSuite))
}

func (s *transactionReceiptStorageIntegrationTestSuite) TestPersistAndGetTransactionReceipt() {
	require := testutil.Require(s.T())

	tag := uint32(1)
	sequence := api.Sequence(12312)
	ctx := context.Background()
	hash := "0xaa7d33f3dac41109eb305936ac4d09697af243bfbbb44a43049b7c1127d388d6"
	data := []byte("asdqewdw")
	blockTime := testutil.MustTime("2020-11-24T16:07:21Z")

	expectedFirstTx := &ethereum.TransactionReceipt{
		Tag:       tag,
		Hash:      hash,
		Sequence:  sequence,
		Data:      data,
		BlockTime: blockTime,
	}

	err := s.storage.PersistTransactionReceipt(ctx, expectedFirstTx)
	require.NoError(err)

	persisted, err := s.storage.GetTransactionReceipt(ctx, tag, hash, sequence)
	require.NoError(err)
	require.False(persisted.UpdatedAt.IsZero())
	expectedFirstTx.UpdatedAt = persisted.UpdatedAt
	require.Equal(expectedFirstTx, persisted)

	// use a larger sequence
	persisted, err = s.storage.GetTransactionReceipt(ctx, tag, hash, sequence+1)
	require.NoError(err)
	require.Equal(expectedFirstTx, persisted)

	// use a smaller sequence
	receipt, err := s.storage.GetTransactionReceipt(ctx, tag, hash, sequence-1)
	require.Error(err)
	require.True(xerrors.Is(err, internal.ErrItemNotFound))
	require.Nil(receipt)

	// second attempt
	sequence = sequence + 100000000
	data = []byte("attempt 2")
	expectedSecondTx := &ethereum.TransactionReceipt{
		Tag:       tag,
		Hash:      hash,
		Sequence:  sequence,
		Data:      data,
		BlockTime: blockTime,
	}
	err = s.storage.PersistTransactionReceipt(ctx, expectedSecondTx)
	require.NoError(err)
	persisted, err = s.storage.GetTransactionReceipt(ctx, tag, hash, sequence+1)
	require.NoError(err)
	require.False(persisted.UpdatedAt.IsZero())
	expectedSecondTx.UpdatedAt = persisted.UpdatedAt
	require.Equal(expectedSecondTx, persisted)

	// third write attempt with old sequence
	oldSequence := sequence - 10000100
	oldData := []byte("attempt 3")
	oldBlockTime := testutil.MustTime("2020-01-24T16:07:21Z")
	err = s.storage.PersistTransactionReceipt(ctx, &ethereum.TransactionReceipt{
		Tag:       tag,
		Hash:      hash,
		Sequence:  oldSequence,
		Data:      oldData,
		BlockTime: oldBlockTime,
	})
	require.NoError(err)

	read, err := s.storage.GetTransactionReceipt(ctx, tag, hash, sequence+1)
	require.NoError(err)
	require.Equal(persisted, read)
}

func (s *transactionReceiptStorageIntegrationTestSuite) TestPersistAndGetTransactionReceipt_LargeTransactionReceipt() {
	require := testutil.Require(s.T())

	// override the max data size
	s.cfg.AWS.DynamoDB.MaxDataSize = 1

	tag := uint32(1)
	sequence := api.Sequence(12312)
	ctx := context.Background()
	hash := "0xaa7d33f3dac41109eb305936ac4d09697af243bfbbb44a43049b7c1127d388d6"
	data := testutil.MakeFile(10)
	blockTime := testutil.MustTime("2020-11-24T16:07:21Z")
	expectedFirstTx := &ethereum.TransactionReceipt{
		Tag:       tag,
		Hash:      hash,
		Sequence:  sequence,
		Data:      data,
		BlockTime: blockTime,
	}

	err := s.storage.PersistTransactionReceipt(ctx, expectedFirstTx)
	require.NoError(err)

	persisted, err := s.storage.GetTransactionReceipt(ctx, tag, hash, sequence)
	require.NoError(err)
	require.False(persisted.UpdatedAt.IsZero())
	expectedFirstTx.UpdatedAt = persisted.UpdatedAt
	require.Equal(expectedFirstTx, persisted)

	// use a larger sequence
	persisted, err = s.storage.GetTransactionReceipt(ctx, tag, hash, sequence+1)
	require.NoError(err)
	require.Equal(expectedFirstTx, persisted)

	// use a smaller sequence
	receipt, err := s.storage.GetTransactionReceipt(ctx, tag, hash, sequence-1)
	require.Error(err)
	require.True(xerrors.Is(err, internal.ErrItemNotFound))
	require.Nil(receipt)

	// second attempt
	sequence = sequence + 100000000
	data = []byte("attempt 2")
	expectedSecondTx := &ethereum.TransactionReceipt{
		Tag:       tag,
		Hash:      hash,
		Sequence:  sequence,
		Data:      data,
		BlockTime: blockTime,
	}
	err = s.storage.PersistTransactionReceipt(ctx, expectedSecondTx)
	require.NoError(err)
	persisted, err = s.storage.GetTransactionReceipt(ctx, tag, hash, sequence+1)
	require.NoError(err)
	require.False(persisted.UpdatedAt.IsZero())
	expectedSecondTx.UpdatedAt = persisted.UpdatedAt
	require.Equal(expectedSecondTx, persisted)

	// third write attempt with old sequence
	oldSequence := sequence - 10000100
	oldData := []byte("attempt 3")
	err = s.storage.PersistTransactionReceipt(ctx, &ethereum.TransactionReceipt{
		Tag:       tag,
		Hash:      hash,
		Sequence:  oldSequence,
		Data:      oldData,
		BlockTime: blockTime,
	})
	require.NoError(err)

	read, err := s.storage.GetTransactionReceipt(ctx, tag, hash, sequence+1)
	require.NoError(err)
	require.Equal(persisted, read)
}

func (s *transactionReceiptStorageIntegrationTestSuite) TestGetTransactionReceipt_NotExists() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	tag := uint32(2)
	hash := "0xabcd"
	sequence := api.Sequence(125)
	receipt, err := s.storage.GetTransactionReceipt(ctx, tag, hash, sequence)
	require.Error(err)
	require.True(xerrors.Is(err, internal.ErrItemNotFound))
	require.Nil(receipt)
}

func (s *transactionReceiptStorageIntegrationTestSuite) TestPersistTransactionReceipt_NilReceipt() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	err := s.storage.PersistTransactionReceipt(ctx, nil)
	require.Error(err)
}
