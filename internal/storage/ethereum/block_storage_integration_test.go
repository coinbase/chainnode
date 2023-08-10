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

type blockStorageIntegrationTestSuite struct {
	suite.Suite
	storage BlockStorage
	cfg     *config.Config
	app     testapp.TestApp
}

func (s *blockStorageIntegrationTestSuite) SetupTest() {
	require := testutil.Require(s.T())

	cfg, err := config.New()
	require.NoError(err)
	s.cfg = cfg

	var storage BlockStorage
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

func (s *blockStorageIntegrationTestSuite) TearDownTest() {
	if s.app != nil {
		s.app.Close()
	}
}

func TestIntegrationBlobStorageTestSuite(t *testing.T) {
	suite.Run(t, new(blockStorageIntegrationTestSuite))
}

func (s *blockStorageIntegrationTestSuite) TestPersistAndGetBlock() {
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
	err := s.storage.PersistBlock(ctx, expectedFirstBlock)
	require.NoError(err)

	persisted, err := s.storage.GetBlock(ctx, tag, height, sequence)
	require.NoError(err)
	require.False(persisted.UpdatedAt.IsZero())
	expectedFirstBlock.UpdatedAt = persisted.UpdatedAt
	require.Equal(expectedFirstBlock, persisted)

	// use a larger sequence
	persisted, err = s.storage.GetBlock(ctx, tag, height, sequence+1)
	require.NoError(err)
	require.Equal(expectedFirstBlock, persisted)

	expectedFirstMetadata := &ethereum.BlockMetadata{
		Tag:       tag,
		Height:    height,
		Hash:      hash,
		Sequence:  sequence,
		BlockTime: blockTime,
	}
	persistedMetadata, err := s.storage.GetBlockMetadata(ctx, tag, height, sequence)
	require.NoError(err)
	require.False(persistedMetadata.UpdatedAt.IsZero())
	expectedFirstMetadata.UpdatedAt = persistedMetadata.UpdatedAt
	require.Equal(expectedFirstMetadata, persistedMetadata)

	persistedMetadata, err = s.storage.GetBlockMetadata(ctx, tag, height, sequence+1)
	require.NoError(err)
	require.Equal(expectedFirstMetadata, persistedMetadata)

	// second write attempt with newer sequence
	sequence = sequence + 100000000
	hash = "updated_hash"
	data = []byte("attempt 2")
	expectedSecondBlock := ethereum.NewBlock(tag, height, hash, sequence, blockTime, data)
	err = s.storage.PersistBlock(ctx, expectedSecondBlock)
	require.NoError(err)

	persisted, err = s.storage.GetBlock(ctx, tag, height, sequence)
	require.NoError(err)
	require.False(persisted.UpdatedAt.IsZero())
	expectedSecondBlock.UpdatedAt = persisted.UpdatedAt
	require.Equal(expectedSecondBlock, persisted)

	expectedSecondMetadata := &ethereum.BlockMetadata{
		Tag:       tag,
		Height:    height,
		Hash:      hash,
		Sequence:  sequence,
		BlockTime: blockTime,
	}
	persistedMetadata, err = s.storage.GetBlockMetadata(ctx, tag, height, sequence)
	require.NoError(err)
	require.False(persisted.UpdatedAt.IsZero())
	expectedSecondMetadata.UpdatedAt = persistedMetadata.UpdatedAt
	require.Equal(expectedSecondMetadata, persistedMetadata)

	// third write attempt with old sequence
	oldSequence := sequence - 10000100
	oldHash := "updated_hash_2"
	oldData := []byte("attempt 3")
	oldBlockTime := testutil.MustTime("2022-11-22T13:07:21Z")
	require.NoError(err)
	oldBlock := ethereum.NewBlock(tag, height, oldHash, oldSequence, oldBlockTime, oldData)
	err = s.storage.PersistBlock(ctx, oldBlock)
	require.NoError(err)

	read, err := s.storage.GetBlock(ctx, tag, height, sequence)
	require.NoError(err)
	// still gets the result from the second attempt
	require.Equal(persisted, read)

	readMetadata, err := s.storage.GetBlockMetadata(ctx, tag, height, sequence)
	require.NoError(err)
	// still gets the result from the second attempt
	require.Equal(persistedMetadata, readMetadata)
}

func (s *blockStorageIntegrationTestSuite) TestPersistAndGetBlock_LargeBlock() {
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
	err := s.storage.PersistBlock(ctx, block)
	require.NoError(err)

	persisted, err := s.storage.GetBlock(ctx, tag, height, sequence)
	require.NoError(err)
	require.NotNil(persisted)
	require.Equal(hash, persisted.Hash)
	require.Equal(height, persisted.Height)
	require.Equal(tag, persisted.Tag)
	require.Equal(data, persisted.Data)
	require.Equal(sequence, persisted.Sequence)
	require.Equal(blockTime, persisted.BlockTime)
	require.False(persisted.UpdatedAt.IsZero())

	persistedMetadata, err := s.storage.GetBlockMetadata(ctx, tag, height, sequence)
	require.NoError(err)
	require.NotNil(persistedMetadata)
	require.Equal(hash, persistedMetadata.Hash)
	require.Equal(height, persistedMetadata.Height)
	require.Equal(tag, persistedMetadata.Tag)
	require.Equal(sequence, persistedMetadata.Sequence)
	require.Equal(blockTime, persisted.BlockTime)
	require.False(persisted.UpdatedAt.IsZero())
}

func (s *blockStorageIntegrationTestSuite) TestGetBlock_NotExists() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	tag := uint32(2)
	height := uint64(123456)
	sequence := api.Sequence(125)
	block, err := s.storage.GetBlock(ctx, tag, height, sequence)
	require.Error(err)
	require.True(xerrors.Is(err, internal.ErrItemNotFound))
	require.Nil(block)

	metadata, err := s.storage.GetBlockMetadata(ctx, tag, height, sequence)
	require.Error(err)
	require.True(xerrors.Is(err, internal.ErrItemNotFound))
	require.Nil(metadata)
}

func (s *blockStorageIntegrationTestSuite) TestPersistBlock_NilBlock() {
	require := testutil.Require(s.T())

	err := s.storage.PersistBlock(context.Background(), nil)
	require.Error(err)
}
