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

type blockByHashWithoutFullTxStorageIntegrationTestSuite struct {
	suite.Suite
	storage BlockByHashWithoutFullTxStorage
	cfg     *config.Config
	app     testapp.TestApp
}

func (s *blockByHashWithoutFullTxStorageIntegrationTestSuite) SetupTest() {
	require := testutil.Require(s.T())

	cfg, err := config.New()
	require.NoError(err)
	s.cfg = cfg

	var storage BlockByHashWithoutFullTxStorage
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

func (s *blockByHashWithoutFullTxStorageIntegrationTestSuite) TearDownTest() {
	if s.app != nil {
		s.app.Close()
	}
}

func TestIntegrationBlockByHashWithoutFullTxStorageTestSuite(t *testing.T) {
	suite.Run(t, new(blockByHashWithoutFullTxStorageIntegrationTestSuite))
}

func (s *blockByHashWithoutFullTxStorageIntegrationTestSuite) TestPersistAndGetBlockByHashWithoutFullTx() {
	require := testutil.Require(s.T())

	tag := uint32(1)
	sequence := api.Sequence(12312)
	height := uint64(123456)
	ctx := context.Background()
	hash := "0xaa7d33f3dac41109eb305936ac4d09697af243bfbbb44a43049b7c1127d388d6"
	data := []byte("asdqewdw")
	blockTime := testutil.MustTime("2020-11-24T16:07:21Z")
	// first write attempt
	expectedFirstBlock := ethereum.NewBlock(tag, height, hash, sequence, blockTime, data)
	err := s.storage.PersistBlockByHashWithoutFullTx(ctx, expectedFirstBlock)
	require.NoError(err)

	persisted, err := s.storage.GetBlockByHashWithoutFullTx(ctx, tag, hash, sequence)
	require.NoError(err)
	require.False(persisted.UpdatedAt.IsZero())
	expectedFirstBlock.UpdatedAt = persisted.UpdatedAt
	require.Equal(expectedFirstBlock, persisted)

	// use a larger sequence
	persisted, err = s.storage.GetBlockByHashWithoutFullTx(ctx, tag, hash, sequence+1)
	require.NoError(err)
	require.Equal(expectedFirstBlock, persisted)

	// second write attempt with newer sequence
	sequence = sequence + 100000000
	height = uint64(234)
	data = []byte("attempt 2")
	expectedSecondBlock := ethereum.NewBlock(tag, height, hash, sequence, blockTime, data)
	err = s.storage.PersistBlockByHashWithoutFullTx(ctx, expectedSecondBlock)
	require.NoError(err)

	persisted, err = s.storage.GetBlockByHashWithoutFullTx(ctx, tag, hash, sequence)
	require.NoError(err)
	require.False(persisted.UpdatedAt.IsZero())
	expectedSecondBlock.UpdatedAt = persisted.UpdatedAt
	require.Equal(expectedSecondBlock, persisted)

	// third write attempt with old sequence
	oldSequence := sequence - 10000100
	oldHeight := uint64(23456)
	oldData := []byte("attempt 3")
	oldBlockTime := testutil.MustTime("2022-11-22T13:07:21Z")
	require.NoError(err)
	oldBlock := ethereum.NewBlock(tag, oldHeight, hash, oldSequence, oldBlockTime, oldData)
	err = s.storage.PersistBlockByHashWithoutFullTx(ctx, oldBlock)
	require.NoError(err)

	read, err := s.storage.GetBlockByHashWithoutFullTx(ctx, tag, hash, sequence)
	require.NoError(err)
	// still gets the result from the second attempt
	require.Equal(persisted, read)
}

func (s *blockByHashWithoutFullTxStorageIntegrationTestSuite) TestPersistAndGetBlockByHashWithoutFullTx_LargeBlock() {
	require := testutil.Require(s.T())

	// override the max data size
	s.cfg.AWS.DynamoDB.MaxDataSize = 1

	tag := uint32(1)
	sequence := api.Sequence(12312)
	height := uint64(123456)
	ctx := context.Background()
	hash := "0xaa7d33f3dac41109eb305936ac4d09697af243bfbbb44a43049b7c1127d388d6"
	blockTime := testutil.MustTime("2020-11-24T16:07:21Z")
	data := testutil.MakeFile(10)
	block := ethereum.NewBlock(tag, height, hash, sequence, blockTime, data)
	err := s.storage.PersistBlockByHashWithoutFullTx(ctx, block)
	require.NoError(err)

	persisted, err := s.storage.GetBlockByHashWithoutFullTx(ctx, tag, hash, sequence)
	require.NoError(err)
	require.NotNil(persisted)
	require.Equal(hash, persisted.Hash)
	require.Equal(height, persisted.Height)
	require.Equal(tag, persisted.Tag)
	require.Equal(data, persisted.Data)
	require.Equal(sequence, persisted.Sequence)
	require.Equal(blockTime, persisted.BlockTime)
	require.False(persisted.UpdatedAt.IsZero())
}

func (s *blockByHashWithoutFullTxStorageIntegrationTestSuite) TestGetBlockByHashWithoutFullTx_NotExists() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	tag := uint32(2)
	hash := "0x111"
	sequence := api.Sequence(125)
	block, err := s.storage.GetBlockByHashWithoutFullTx(ctx, tag, hash, sequence)
	require.Error(err)
	require.True(xerrors.Is(err, internal.ErrItemNotFound))
	require.Nil(block)
}

func (s *blockByHashWithoutFullTxStorageIntegrationTestSuite) TestPersistBlockByHashWithoutFullTx_NilBlock() {
	require := testutil.Require(s.T())

	err := s.storage.PersistBlockByHashWithoutFullTx(context.Background(), nil)
	require.Error(err)
}
