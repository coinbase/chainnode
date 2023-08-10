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

type blockExtraDataByNumberStorageIntegrationTestSuite struct {
	suite.Suite
	storage BlockExtraDataByNumberStorage
	cfg     *config.Config
	app     testapp.TestApp
}

func (s *blockExtraDataByNumberStorageIntegrationTestSuite) SetupTest() {
	require := testutil.Require(s.T())

	cfg, err := config.New()
	require.NoError(err)
	s.cfg = cfg

	var storage BlockExtraDataByNumberStorage
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

func (s *blockExtraDataByNumberStorageIntegrationTestSuite) TearDownTest() {
	if s.app != nil {
		s.app.Close()
	}
}

func TestIntegrationBlockExtraDataByNumberStorageTestSuite(t *testing.T) {
	suite.Run(t, new(blockExtraDataByNumberStorageIntegrationTestSuite))
}

func (s *blockExtraDataByNumberStorageIntegrationTestSuite) TestPersistAndGet() {
	require := testutil.Require(s.T())

	tag := uint32(1)
	sequence := api.Sequence(12312)
	height := uint64(123456)
	ctx := context.Background()
	hash := "0xaa7d33f3dac41109eb305936ac4d09697af243bfbbb44a43049b7c1127d388d6"
	data := []byte("asdqewdw")
	blockTime := testutil.MustTime("2020-11-24T16:07:21Z")
	// first write attempt
	expectedFirstBlock := ethereum.NewBlockExtraData(tag, height, hash, sequence, blockTime, data)
	err := s.storage.PersistBlockExtraDataByNumber(ctx, expectedFirstBlock)
	require.NoError(err)

	persisted, err := s.storage.GetBlockExtraDataByNumber(ctx, tag, height, sequence)
	require.NoError(err)
	require.False(persisted.UpdatedAt.IsZero())
	expectedFirstBlock.UpdatedAt = persisted.UpdatedAt
	require.Equal(expectedFirstBlock, persisted)

	// use a larger sequence
	persisted, err = s.storage.GetBlockExtraDataByNumber(ctx, tag, height, sequence+1)
	require.NoError(err)
	require.Equal(expectedFirstBlock, persisted)

	// second write attempt with newer sequence
	sequence = sequence + 100000000
	hash = "updated_hash"
	data = []byte("attempt 2")
	expectedSecondBlock := ethereum.NewBlockExtraData(tag, height, hash, sequence, blockTime, data)
	err = s.storage.PersistBlockExtraDataByNumber(ctx, expectedSecondBlock)
	require.NoError(err)

	persisted, err = s.storage.GetBlockExtraDataByNumber(ctx, tag, height, sequence)
	require.NoError(err)
	require.False(persisted.UpdatedAt.IsZero())
	expectedSecondBlock.UpdatedAt = persisted.UpdatedAt
	require.Equal(expectedSecondBlock, persisted)

	// third write attempt with old sequence
	oldSequence := sequence - 10000100
	oldHash := "updated_hash_2"
	oldData := []byte("attempt 3")
	oldBlockTime := testutil.MustTime("2022-11-22T13:07:21Z")
	require.NoError(err)
	oldBlock := ethereum.NewBlockExtraData(tag, height, oldHash, oldSequence, oldBlockTime, oldData)
	err = s.storage.PersistBlockExtraDataByNumber(ctx, oldBlock)
	require.NoError(err)

	read, err := s.storage.GetBlockExtraDataByNumber(ctx, tag, height, sequence)
	require.NoError(err)
	// still gets the result from the second attempt
	require.Equal(persisted, read)
}

func (s *blockExtraDataByNumberStorageIntegrationTestSuite) TestGet_NotExists() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	tag := uint32(2)
	height := uint64(123456)
	sequence := api.Sequence(125)
	block, err := s.storage.GetBlockExtraDataByNumber(ctx, tag, height, sequence)
	require.Error(err)
	require.True(xerrors.Is(err, internal.ErrItemNotFound))
	require.Nil(block)
}

func (s *blockExtraDataByNumberStorageIntegrationTestSuite) TestPersist_NilBlock() {
	require := testutil.Require(s.T())

	err := s.storage.PersistBlockExtraDataByNumber(context.Background(), nil)
	require.Error(err)
}
