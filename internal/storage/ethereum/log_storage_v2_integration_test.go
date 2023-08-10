package ethereum

import (
	"context"
	"fmt"
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

type logStorageV2TestSuite struct {
	suite.Suite
	storage LogStorageV2
	cfg     *config.Config
	app     testapp.TestApp
}

func (s *logStorageV2TestSuite) SetupTest() {
	require := testutil.Require(s.T())

	var storage LogStorageV2
	cfg, err := config.New()
	require.NoError(err)
	// override partition size
	cfg.AWS.DynamoDB.LogsTablePartitionSize = 10
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

func (s *logStorageV2TestSuite) TearDownTest() {
	if s.app != nil {
		s.app.Close()
	}
}

func TestIntegrationLogStorageV2TestSuite(t *testing.T) {
	suite.Run(t, new(logStorageV2TestSuite))
}

func (s *logStorageV2TestSuite) TestPersistLogsAndGetLogs() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	tag := uint32(1)
	sequence := api.Sequence(12312)
	height := uint64(123456)
	hash := "0xc6ef2fc5426d6ad6fd9e2a26abeab0aa2411b7ab17f30a99d3cb96aed1d1055b"
	logsBloom := "0x12efd"
	data := []byte("attempt 1")
	blockTime := "2020-11-24T16:07:21Z"

	// first attempt
	expectedFirstLogs, err := testutil.MakeEthereumLogs(tag, sequence, height, hash, logsBloom, data, blockTime)
	require.NoError(err)
	err = s.storage.PersistLogsV2(ctx, expectedFirstLogs)
	require.NoError(err)
	expectedFirstLogsLite, err := testutil.MakeEthereumLogsLite(tag, sequence, height, logsBloom)
	require.NoError(err)

	persistedLogsLite, err := s.storage.GetLogsLiteByBlockRange(ctx, tag, height, height+1, sequence)
	require.NoError(err)
	require.Len(persistedLogsLite, 1)
	require.Equal(expectedFirstLogsLite, persistedLogsLite[0])

	persisted, err := s.storage.GetLogsV2(ctx, tag, height, sequence)
	require.NoError(err)
	require.False(persisted.UpdatedAt.IsZero())
	expectedFirstLogs.UpdatedAt = persisted.UpdatedAt
	require.Equal(expectedFirstLogs, persisted)

	// use a larger sequence
	persistedLogsLite, err = s.storage.GetLogsLiteByBlockRange(ctx, tag, height, height+1, sequence+1)
	require.NoError(err)
	require.Len(persistedLogsLite, 1)
	require.Equal(expectedFirstLogsLite, persistedLogsLite[0])

	persisted, err = s.storage.GetLogsV2(ctx, tag, height, sequence+1)
	require.NoError(err)
	require.Equal(expectedFirstLogs, persisted)

	// second attempt
	sequence = sequence + 100000000
	hash = "updated_hash"
	data = []byte("attempt 2")
	logsBloom = "0x12efd123"
	expectedSecondLogs, err := testutil.MakeEthereumLogs(tag, sequence, height, hash, logsBloom, data, blockTime)
	require.NoError(err)
	err = s.storage.PersistLogsV2(ctx, expectedSecondLogs)
	require.NoError(err)
	expectedSecondLogsLite, err := testutil.MakeEthereumLogsLite(tag, sequence, height, logsBloom)
	require.NoError(err)

	persistedLogsLite, err = s.storage.GetLogsLiteByBlockRange(ctx, tag, height, height+1, sequence)
	require.NoError(err)
	require.Len(persistedLogsLite, 1)
	require.Equal(expectedSecondLogsLite, persistedLogsLite[0])

	persisted, err = s.storage.GetLogsV2(ctx, tag, height, sequence)
	require.NoError(err)
	require.False(persisted.UpdatedAt.IsZero())
	expectedSecondLogs.UpdatedAt = persisted.UpdatedAt
	require.Equal(expectedSecondLogs, persisted)

	// third write attempt with old sequence
	oldSequence := sequence - 10000100
	oldHash := "updated_hash_2"
	oldData := []byte("attempt 3")
	oldLogsBloom := "0x12efd123123"
	oldBlockTime := "2020-11-22T14:07:21Z"
	expectedThirdLogs, err := testutil.MakeEthereumLogs(tag, oldSequence, height, oldHash, oldLogsBloom, oldData, oldBlockTime)
	require.NoError(err)
	err = s.storage.PersistLogsV2(ctx, expectedThirdLogs)
	require.NoError(err)

	readLogsLite, err := s.storage.GetLogsLiteByBlockRange(ctx, tag, height, height+1, sequence)
	require.NoError(err)
	require.Len(persistedLogsLite, 1)
	require.Equal(expectedSecondLogsLite, readLogsLite[0])

	read, err := s.storage.GetLogsV2(ctx, tag, height, sequence)
	require.NoError(err)
	require.Equal(expectedSecondLogs, read)
}

func (s *logStorageV2TestSuite) TestPersistLogsAndGetLogs_AcrossPartitions() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	tag := uint32(1)
	sequence := api.Sequence(12312)
	hash := "0xc6ef2fc5426d6ad6fd9e2a26abeab0aa2411b7ab17f30a99d3cb96aed1d1055b"
	logsBloom := "0x12efd"
	data := []byte("attempt 1")
	blockTime := "2020-11-24T16:07:21Z"

	// partitionSize is 10
	tests := []struct {
		BeginHeight uint64
		EndHeight   uint64
	}{
		{
			BeginHeight: uint64(46),
			EndHeight:   uint64(70),
		},
		{
			BeginHeight: uint64(76),
			EndHeight:   uint64(101),
		},
		{
			BeginHeight: uint64(5),
			EndHeight:   uint64(9),
		},
		{
			BeginHeight: uint64(10),
			EndHeight:   uint64(20),
		},
	}

	for _, test := range tests {
		beginHeight := test.BeginHeight
		endHeight := test.EndHeight
		name := fmt.Sprintf("[%d, %d)", beginHeight, endHeight)
		s.Run(name, func() {
			beginHeight := test.BeginHeight
			endHeight := test.EndHeight
			logs := make([]*ethereum.Logs, endHeight-beginHeight)
			expectedLogsLite := make([]*ethereum.LogsLite, endHeight-beginHeight)

			var err error
			for i := range logs {
				curHeight := beginHeight + uint64(i)
				logs[i], err = testutil.MakeEthereumLogs(tag, sequence, curHeight, hash, logsBloom, data, blockTime)
				require.NoError(err)
				err = s.storage.PersistLogsV2(ctx, logs[i])
				require.NoError(err)

				expectedLogsLite[i], err = testutil.MakeEthereumLogsLite(tag, sequence, curHeight, logsBloom)
				require.NoError(err)
			}

			persistedLogsLite, err := s.storage.GetLogsLiteByBlockRange(ctx, tag, beginHeight, endHeight, sequence)
			require.NoError(err)
			require.Len(persistedLogsLite, int(endHeight-beginHeight))
			require.Equal(expectedLogsLite, persistedLogsLite)

			persistedLogsLite, err = s.storage.GetLogsLiteByBlockRange(ctx, tag, beginHeight, endHeight, sequence-1)
			require.Error(err)
			require.True(xerrors.Is(err, internal.ErrItemNotFound))
			require.Nil(persistedLogsLite)

			persistedLogsLite, err = s.storage.GetLogsLiteByBlockRange(ctx, tag, endHeight, endHeight+1, sequence)
			require.Error(err)
			require.True(xerrors.Is(err, internal.ErrItemNotFound))
			require.Nil(persistedLogsLite)

			var persisted *ethereum.Logs
			for i := range logs {
				curHeight := beginHeight + uint64(i)

				persisted, err = s.storage.GetLogsV2(ctx, tag, curHeight, sequence)
				require.NoError(err)
				require.NotNil(persisted)
				logs[i].UpdatedAt = persisted.UpdatedAt
				require.Equal(logs[i], persisted)

				persisted, err = s.storage.GetLogsV2(ctx, tag, curHeight, sequence-1)
				require.Error(err)
				require.True(xerrors.Is(err, internal.ErrItemNotFound))
				require.Nil(persisted)
			}

			persisted, err = s.storage.GetLogsV2(ctx, tag, endHeight, sequence)
			require.Error(err)
			require.True(xerrors.Is(err, internal.ErrItemNotFound))
			require.Nil(persisted)
		})

	}
}

func (s *logStorageV2TestSuite) TestGetLogs_NotExists() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	tag := uint32(2)
	sequence := api.Sequence(12312)
	height := uint64(123456)
	logsLite, err := s.storage.GetLogsLiteByBlockRange(ctx, tag, height, height+3, sequence)
	require.Error(err)
	require.True(xerrors.Is(err, internal.ErrItemNotFound))
	require.Nil(logsLite)

	logs, err := s.storage.GetLogsV2(ctx, tag, height, sequence)
	require.Error(err)
	require.True(xerrors.Is(err, internal.ErrItemNotFound))
	require.Nil(logs)
}

func (s *logStorageV2TestSuite) TestPersistLogs_NilLogs() {
	require := testutil.Require(s.T())

	err := s.storage.PersistLogsV2(context.Background(), nil)
	require.Error(err)
}
