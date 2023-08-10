package ethereum

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
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

type LogStorageV2TestSuite struct {
	suite.Suite
	ctrl             *gomock.Controller
	logsStorage      *cmocks.MockCollectionStorage
	storage          LogStorageV2
	app              testapp.TestApp
	blockTimeFixture time.Time
}

const (
	logV2PartitionKeyFixture       = "1#logs-v2#123456"
	logV2SortKeyFixture            = "0000000000003018"
	logsV2IndexPartitionKeyFixture = "1#00000000000000f6"
	logsV2IndexSortKeyFixture      = "000000000001e240"
	logTagFixture                  = uint32(1)
	logSequenceFixture             = api.Sequence(12312)
	logHeightFixture               = uint64(123456)
	logHashFixture                 = "0xc6ef2fc5426d6ad6fd9e2a26abeab0aa2411b7ab17f30a99d3cb96aed1d1055b"
	logsBloomFixture               = "0x12efd"
	logsBlockTimeFixture           = "2020-11-24T16:07:21Z"
)

func TestLogStorageV2TestSuite(t *testing.T) {
	suite.Run(t, new(LogStorageV2TestSuite))
}

func (s *LogStorageV2TestSuite) SetupTest() {
	s.ctrl = gomock.NewController(s.T())
	s.logsStorage = cmocks.NewMockCollectionStorage(s.ctrl)
	s.logsStorage.EXPECT().
		WithCollection(xapi.CollectionLogsV2).
		Return(s.logsStorage)
	collectionStorage := cmocks.NewMockCollectionStorage(s.ctrl)

	s.app = testapp.New(
		s.T(),
		fx.Provide(newLogStorageV2),
		fx.Provide(fx.Annotated{Name: "logs", Target: func() collection.CollectionStorage { return s.logsStorage }}),
		fx.Provide(fx.Annotated{Name: "collection", Target: func() collection.CollectionStorage { return collectionStorage }}),
		fx.Populate(&s.storage),
	)
	s.blockTimeFixture = testutil.MustTime(logsBlockTimeFixture)
}

func (s *LogStorageV2TestSuite) TearDownTest() {
	s.app.Close()
	s.ctrl.Finish()
}

func (s *LogStorageV2TestSuite) TestPersistLogsV2() {
	require := testutil.Require(s.T())
	ctx := context.Background()

	data := []byte("asdqwd")
	compressed, err := compression.Compress(data, compression.CompressionGzip)
	require.NoError(err)

	logs := &xapi.Logs{
		Tag:       logTagFixture,
		Sequence:  logSequenceFixture,
		Height:    logHeightFixture,
		Hash:      logHashFixture,
		LogsBloom: logsBloomFixture,
		Data:      data,
		BlockTime: s.blockTimeFixture,
	}

	entry := &models.LogsV2DDBEntry{
		BaseItem: collection.NewBaseItem(
			logV2PartitionKeyFixture,
			logV2SortKeyFixture,
			logTagFixture,
		).WithData(compressed),
		IndexPartitionKey: logsV2IndexPartitionKeyFixture,
		IndexSortKey:      logsV2IndexSortKeyFixture,
		Height:            logHeightFixture,
		Hash:              logHashFixture,
		LogsBloom:         logsBloomFixture,
		BlockTime:         logsBlockTimeFixture,
	}

	s.logsStorage.EXPECT().
		UploadToBlobStorage(gomock.Any(), gomock.Any(), false).Return(nil)
	s.logsStorage.EXPECT().WriteItem(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, item interface{}) error {
			actual, ok := item.(*models.LogsV2DDBEntry)
			require.True(ok)
			testutil.MustTime(actual.UpdatedAt)
			entry.UpdatedAt = actual.UpdatedAt
			require.Equal(entry, actual)
			return nil
		})

	err = s.storage.PersistLogsV2(ctx, logs)
	require.NoError(err)
}

func (s *LogStorageV2TestSuite) TestPersistLogsV2_NoBlockTime() {
	require := testutil.Require(s.T())
	ctx := context.Background()

	data := []byte("asdqwd")
	compressed, err := compression.Compress(data, compression.CompressionGzip)
	require.NoError(err)

	logs := &xapi.Logs{
		Tag:       logTagFixture,
		Sequence:  logSequenceFixture,
		Height:    logHeightFixture,
		Hash:      logHashFixture,
		LogsBloom: logsBloomFixture,
		Data:      data,
	}

	entry := &models.LogsV2DDBEntry{
		BaseItem: collection.NewBaseItem(
			logV2PartitionKeyFixture,
			logV2SortKeyFixture,
			logTagFixture,
		).WithData(compressed),
		IndexPartitionKey: logsV2IndexPartitionKeyFixture,
		IndexSortKey:      logsV2IndexSortKeyFixture,
		Height:            logHeightFixture,
		Hash:              logHashFixture,
		LogsBloom:         logsBloomFixture,
	}

	s.logsStorage.EXPECT().
		UploadToBlobStorage(gomock.Any(), gomock.Any(), false).Return(nil)
	s.logsStorage.EXPECT().WriteItem(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, item interface{}) error {
			actual, ok := item.(*models.LogsV2DDBEntry)
			require.True(ok)
			testutil.MustTime(actual.UpdatedAt)
			entry.UpdatedAt = actual.UpdatedAt
			require.Equal(entry, actual)
			return nil
		})

	err = s.storage.PersistLogsV2(ctx, logs)
	require.NoError(err)
}

func (s *LogStorageV2TestSuite) TestPersistLogsV2_LargeFile() {
	require := testutil.Require(s.T())
	ctx := context.Background()

	data := []byte("asdqwd")
	objectKey := "1/logs-v2/123456/12312"
	logs := &xapi.Logs{
		Tag:       logTagFixture,
		Sequence:  logSequenceFixture,
		Height:    logHeightFixture,
		Hash:      logHashFixture,
		LogsBloom: logsBloomFixture,
		Data:      data,
		BlockTime: s.blockTimeFixture,
	}

	entryAfterUpload := &models.LogsV2DDBEntry{
		BaseItem: collection.NewBaseItem(
			logV2PartitionKeyFixture,
			logV2SortKeyFixture,
			logTagFixture,
		).WithObjectKey(objectKey),
		IndexPartitionKey: logsV2IndexPartitionKeyFixture,
		IndexSortKey:      logsV2IndexSortKeyFixture,
		Height:            logHeightFixture,
		Hash:              logHashFixture,
		LogsBloom:         logsBloomFixture,
		BlockTime:         logsBlockTimeFixture,
	}

	s.logsStorage.EXPECT().
		UploadToBlobStorage(gomock.Any(), gomock.Any(), false).
		DoAndReturn(func(ctx context.Context, entry collection.Item, enforceUpload bool) error {
			err := entry.SetData(nil)
			require.NoError(err)
			err = entry.SetObjectKey(objectKey)
			require.NoError(err)
			return nil
		})
	s.logsStorage.EXPECT().WriteItem(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, item interface{}) error {
			actual, ok := item.(*models.LogsV2DDBEntry)
			require.True(ok)
			testutil.MustTime(actual.UpdatedAt)
			entryAfterUpload.UpdatedAt = actual.UpdatedAt
			require.Equal(entryAfterUpload, actual)
			return nil
		})

	err := s.storage.PersistLogsV2(ctx, logs)
	require.NoError(err)
}

func (s *LogStorageV2TestSuite) TestPersistLogsV2_LargeFileUploadError() {
	require := testutil.Require(s.T())
	ctx := context.Background()

	data := []byte("asdqwd")

	logs := &xapi.Logs{
		Tag:       logTagFixture,
		Sequence:  logSequenceFixture,
		Height:    logHeightFixture,
		Hash:      logHashFixture,
		LogsBloom: logsBloomFixture,
		Data:      data,
		BlockTime: s.blockTimeFixture,
	}
	s.logsStorage.EXPECT().
		UploadToBlobStorage(gomock.Any(), gomock.Any(), false).
		Return(xerrors.Errorf("failed to upload"))

	err := s.storage.PersistLogsV2(ctx, logs)
	require.Error(err)
}

func (s *LogStorageV2TestSuite) TestGetLogsLiteByBlockRange_SinglePartition() {
	require := testutil.Require(s.T())
	ctx := context.Background()

	beginHeight := uint64(323)
	endHeight := uint64(357)
	numItems := int(endHeight - beginHeight)

	entries := make([]interface{}, numItems+1)
	for i := beginHeight; i < endHeight; i++ {
		entries[i-beginHeight] = &models.LogsLiteDDBEntry{
			PartitionKey: models.MakeLogsV2PartitionKey(logTagFixture, i),
			SortKey:      models.MakeLogsV2SortKey(logSequenceFixture),
			LogsBloom:    fmt.Sprintf("%s-%d", logsBloomFixture, i),
		}
	}

	// append duplicate item with smaller sequence at height 326
	itemWithSmallerSequence := uint64(326)
	entries[numItems] = &models.LogsLiteDDBEntry{
		PartitionKey: models.MakeLogsV2PartitionKey(logTagFixture, itemWithSmallerSequence),
		SortKey:      models.MakeLogsV2SortKey(logSequenceFixture - 1),
		LogsBloom:    fmt.Sprintf("%s-%d-dup", logsBloomFixture, itemWithSmallerSequence),
	}

	attempt := 0
	firstEvaluatedKeys := map[string]*dynamodb.AttributeValue{
		"pk": {
			S: aws.String("foo"),
		},
		"sk": {
			S: aws.String("bar"),
		},
	}

	expectedAttributeValues := map[string]*dynamodb.AttributeValue{
		":0": {
			S: aws.String(logV2SortKeyFixture), // sk
		},
		":1": {
			S: aws.String("1#0000000000000000"), // bbr_pk
		},
		":2": {
			S: aws.String("0000000000000143"), // beginHeight: 323
		},
		":3": {
			S: aws.String("0000000000000164"), // endHeight: 357 - 1
		},
	}
	s.logsStorage.EXPECT().QueryItems(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, request *collection.QueryItemsRequest,
		) (*collection.QueryItemsResult, error) {
			attempt += 1
			require.NotNil(request.KeyConditionExpression)
			require.Equal("(#1 = :1) AND (#2 BETWEEN :2 AND :3)", *request.KeyConditionExpression)
			require.NotNil(request.ExpressionAttributeNames)
			require.NotNil(request.ExpressionAttributeValues)
			require.Equal(expectedAttributeValues, request.ExpressionAttributeValues)
			require.NotNil(request.ProjectionExpression)
			require.NotNil(request.FilterExpression)
			require.Equal("#0 <= :0", *request.FilterExpression)
			require.False(request.ScanIndexForward)
			require.NotEmpty(request.IndexName)
			if attempt == 1 {
				require.Nil(request.ExclusiveStartKey)
			} else {
				require.NotNil(request.ExclusiveStartKey)
				require.Equal(firstEvaluatedKeys, request.ExclusiveStartKey)
			}

			var items []interface{}
			var lastEvaluatedKey map[string]*dynamodb.AttributeValue
			if attempt == 1 {
				items = entries[:numItems/2]
				lastEvaluatedKey = firstEvaluatedKeys
			} else {
				items = entries[numItems/2:]
				lastEvaluatedKey = nil
			}

			return &collection.QueryItemsResult{
				Items:            items,
				LastEvaluatedKey: lastEvaluatedKey,
			}, nil
		}).Times(2)
	logs, err := s.storage.GetLogsLiteByBlockRange(ctx, logTagFixture, beginHeight, endHeight, logSequenceFixture)
	require.NoError(err)
	require.Equal(numItems, len(logs))

	seen := sync.Map{}
	for i := 0; i < numItems; i++ {
		_, ok := seen.LoadOrStore(logs[i].Height, struct{}{})
		require.False(ok)
	}

	for i := beginHeight; i < endHeight; i++ {
		_, ok := seen.LoadOrStore(i, struct{}{})
		require.True(ok, fmt.Sprintf("height=%v not seen", i))
	}
}

func (s *LogStorageV2TestSuite) TestGetLogsLiteByBlockRange_MultiplePartitions() {
	require := testutil.Require(s.T())
	ctx := context.Background()

	beginHeight := uint64(485)
	endHeight := uint64(1010)
	numItems := int(endHeight - beginHeight)

	entries := make([]interface{}, numItems+1)
	for i := beginHeight; i < endHeight; i++ {
		entries[i-beginHeight] = &models.LogsLiteDDBEntry{
			PartitionKey: models.MakeLogsV2PartitionKey(logTagFixture, i),
			SortKey:      models.MakeLogsV2SortKey(logSequenceFixture),
			LogsBloom:    fmt.Sprintf("%s-%d", logsBloomFixture, i),
		}
	}

	// append duplicate item with smaller sequence at height 1008
	itemWithSmallerSequence := uint64(1008)
	entries[numItems] = &models.LogsLiteDDBEntry{
		PartitionKey: models.MakeLogsV2PartitionKey(logTagFixture, itemWithSmallerSequence),
		SortKey:      models.MakeLogsV2SortKey(logSequenceFixture - 1),
		LogsBloom:    fmt.Sprintf("%s-%d-dup", logsBloomFixture, itemWithSmallerSequence),
	}

	s.logsStorage.EXPECT().QueryItems(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, request *collection.QueryItemsRequest,
		) (*collection.QueryItemsResult, error) {
			require.Nil(request.ExclusiveStartKey)
			require.NotNil(request.KeyConditionExpression)
			require.Equal("(#1 = :1) AND (#2 BETWEEN :2 AND :3)", *request.KeyConditionExpression)
			require.NotNil(request.ExpressionAttributeNames)
			require.NotNil(request.ExpressionAttributeValues)
			require.NotNil(request.ProjectionExpression)
			require.NotNil(request.FilterExpression)
			require.False(request.ScanIndexForward)
			require.NotEmpty(request.IndexName)

			// check sk
			require.Equal(logV2SortKeyFixture, *request.ExpressionAttributeValues[":0"].S)

			bbrPk := *request.ExpressionAttributeValues[":1"].S
			beginSk := *request.ExpressionAttributeValues[":2"].S
			endSk := *request.ExpressionAttributeValues[":3"].S
			size := uint64(s.app.Config().AWS.DynamoDB.LogsTablePartitionSize)

			var items []interface{}
			switch bbrPk {
			case "1#0000000000000000":
				require.Equal(api.Index(beginHeight).AsPaddedHex(), beginSk)
				require.Equal(api.Index(size-1).AsPaddedHex(), endSk)
				items = entries[:size-beginHeight]
			case "1#0000000000000001":
				require.Equal(api.Index(size).AsPaddedHex(), beginSk)
				require.Equal(api.Index(2*size-1).AsPaddedHex(), endSk)
				items = entries[size-beginHeight : 2*size-beginHeight]
			case "1#0000000000000002":
				require.Equal(api.Index(2*size).AsPaddedHex(), beginSk)
				require.Equal(api.Index(endHeight-1).AsPaddedHex(), endSk)
				items = entries[2*size-beginHeight:]
			default:
				s.Fail(fmt.Sprintf("invalid pk value=%s", bbrPk))
			}
			return &collection.QueryItemsResult{
				Items:            items,
				LastEvaluatedKey: nil,
			}, nil
		}).Times(3)
	logs, err := s.storage.GetLogsLiteByBlockRange(ctx, logTagFixture, beginHeight, endHeight, logSequenceFixture)
	require.NoError(err)
	require.Equal(numItems, len(logs))

	seen := sync.Map{}
	for i := 0; i < numItems; i++ {
		_, ok := seen.LoadOrStore(logs[i].Height, struct{}{})
		require.False(ok)
	}

	for i := beginHeight; i < endHeight; i++ {
		_, ok := seen.LoadOrStore(i, struct{}{})
		require.True(ok, fmt.Sprintf("height=%v not seen", i))
	}
}

func (s *LogStorageV2TestSuite) TestGetLogsLiteByBlockRange_InvalidRange() {
	require := testutil.Require(s.T())
	ctx := context.Background()

	height := uint64(323)
	logs, err := s.storage.GetLogsLiteByBlockRange(ctx, logTagFixture, height, height, logSequenceFixture)
	require.Error(err)
	require.Contains(err.Error(), "invalid range")
	require.True(xerrors.Is(err, api.ErrNotAllowed))
	require.Nil(logs)

	logs, err = s.storage.GetLogsLiteByBlockRange(ctx, logTagFixture, height, height-1, logSequenceFixture)
	require.Error(err)
	require.Contains(err.Error(), "invalid range")
	require.True(xerrors.Is(err, api.ErrNotAllowed))
	require.Nil(logs)
}

func (s *LogStorageV2TestSuite) TestGetLogs() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	data := []byte("asdqwd")
	compressed, err := compression.Compress(data, compression.CompressionGzip)
	require.NoError(err)
	entry := &models.LogsV2DDBEntry{
		BaseItem: collection.NewBaseItem(
			logV2PartitionKeyFixture,
			logV2SortKeyFixture,
			logTagFixture,
		).WithData(compressed),
		IndexPartitionKey: logsV2IndexPartitionKeyFixture,
		IndexSortKey:      logsV2IndexSortKeyFixture,
		Height:            logHeightFixture,
		Hash:              logHashFixture,
		LogsBloom:         logsBloomFixture,
		BlockTime:         logsBlockTimeFixture,
	}

	expectedLogs := &xapi.Logs{
		Tag:       logTagFixture,
		Sequence:  logSequenceFixture,
		Height:    logHeightFixture,
		LogsBloom: logsBloomFixture,
		Hash:      logHashFixture,
		Data:      data,
		BlockTime: s.blockTimeFixture,
		UpdatedAt: testutil.MustTime(entry.UpdatedAt),
	}

	entries := make([]interface{}, 1)
	entries[0] = entry

	s.logsStorage.EXPECT().QueryItemByMaxSortKey(
		gomock.Any(),
		logV2PartitionKeyFixture,
		logV2SortKeyFixture,
		models.LogsV2DDBEntry{},
		nil,
	).Return(&collection.QueryItemsResult{Items: entries, LastEvaluatedKey: nil}, nil)

	logs, err := s.storage.GetLogsV2(ctx, logTagFixture, logHeightFixture, logSequenceFixture)
	require.NoError(err)
	require.Equal(expectedLogs, logs)
}

func (s *LogStorageV2TestSuite) TestGetLogs_NoBlockTime() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	data := []byte("asdqwd")
	compressed, err := compression.Compress(data, compression.CompressionGzip)
	require.NoError(err)

	entry := &models.LogsV2DDBEntry{
		BaseItem: collection.NewBaseItem(
			logV2PartitionKeyFixture,
			logV2SortKeyFixture,
			logTagFixture,
		).WithData(compressed),
		IndexPartitionKey: logsV2IndexPartitionKeyFixture,
		IndexSortKey:      logsV2IndexSortKeyFixture,
		Height:            logHeightFixture,
		Hash:              logHashFixture,
		LogsBloom:         logsBloomFixture,
	}
	expectedLogs := &xapi.Logs{
		Tag:       logTagFixture,
		Sequence:  logSequenceFixture,
		Height:    logHeightFixture,
		LogsBloom: logsBloomFixture,
		Hash:      logHashFixture,
		Data:      data,
		UpdatedAt: testutil.MustTime(entry.UpdatedAt),
	}

	entries := make([]interface{}, 1)
	entries[0] = entry

	s.logsStorage.EXPECT().QueryItemByMaxSortKey(
		gomock.Any(),
		logV2PartitionKeyFixture,
		logV2SortKeyFixture,
		models.LogsV2DDBEntry{},
		nil,
	).Return(&collection.QueryItemsResult{Items: entries, LastEvaluatedKey: nil}, nil)

	logs, err := s.storage.GetLogsV2(ctx, logTagFixture, logHeightFixture, logSequenceFixture)
	require.NoError(err)
	require.Equal(expectedLogs, logs)
}

func (s *LogStorageV2TestSuite) TestGetLogs_InvalidBlockTime() {
	require := testutil.Require(s.T())

	ctx := context.Background()
	data := []byte("asdqwd")
	compressed, err := compression.Compress(data, compression.CompressionGzip)
	require.NoError(err)

	entry := &models.LogsV2DDBEntry{
		BaseItem: collection.NewBaseItem(
			logV2PartitionKeyFixture,
			logV2SortKeyFixture,
			logTagFixture,
		).WithData(compressed),
		IndexPartitionKey: logsV2IndexPartitionKeyFixture,
		IndexSortKey:      logsV2IndexSortKeyFixture,
		Height:            logHeightFixture,
		Hash:              logHashFixture,
		LogsBloom:         logsBloomFixture,
		BlockTime:         "invalid",
	}

	entries := make([]interface{}, 1)
	entries[0] = entry

	s.logsStorage.EXPECT().QueryItemByMaxSortKey(
		gomock.Any(),
		logV2PartitionKeyFixture,
		logV2SortKeyFixture,
		models.LogsV2DDBEntry{},
		nil,
	).Return(&collection.QueryItemsResult{Items: entries, LastEvaluatedKey: nil}, nil)

	logs, err := s.storage.GetLogsV2(ctx, logTagFixture, logHeightFixture, logSequenceFixture)
	require.Error(err)
	require.Nil(logs)
}

func (s *LogStorageV2TestSuite) TestGetLogs_LargeFile() {
	require := testutil.Require(s.T())
	ctx := context.Background()
	data := []byte("asdqwd")
	objectKey := "s3-location"

	compressed, err := compression.Compress(data, compression.CompressionGzip)
	require.NoError(err)

	entry := &models.LogsV2DDBEntry{
		BaseItem: collection.NewBaseItem(
			logV2PartitionKeyFixture,
			logV2SortKeyFixture,
			logTagFixture,
		).WithObjectKey(objectKey),
		IndexPartitionKey: logsV2IndexPartitionKeyFixture,
		IndexSortKey:      logsV2IndexSortKeyFixture,
		Height:            logHeightFixture,
		Hash:              logHashFixture,
		LogsBloom:         logsBloomFixture,
		BlockTime:         logsBlockTimeFixture,
	}
	expectedLogs := &xapi.Logs{
		Tag:       logTagFixture,
		Sequence:  logSequenceFixture,
		Height:    logHeightFixture,
		LogsBloom: logsBloomFixture,
		Hash:      logHashFixture,
		Data:      data,
		BlockTime: s.blockTimeFixture,
		UpdatedAt: testutil.MustTime(entry.UpdatedAt),
	}

	entries := make([]interface{}, 1)
	entries[0] = entry

	s.logsStorage.EXPECT().QueryItemByMaxSortKey(
		gomock.Any(),
		logV2PartitionKeyFixture,
		logV2SortKeyFixture,
		models.LogsV2DDBEntry{},
		nil,
	).Return(&collection.QueryItemsResult{Items: entries, LastEvaluatedKey: nil}, nil)

	s.logsStorage.EXPECT().
		DownloadFromBlobStorage(gomock.Any(), entry).
		DoAndReturn(func(ctx context.Context, entry collection.Item) error {
			err := entry.SetData(compressed)
			require.NoError(err)
			return nil
		})

	logs, err := s.storage.GetLogsV2(ctx, logTagFixture, logHeightFixture, logSequenceFixture)
	require.NoError(err)
	require.Equal(expectedLogs, logs)
}

func (s *LogStorageV2TestSuite) TestGetLogs_LargeFileDownloadFailure() {
	require := testutil.Require(s.T())
	ctx := context.Background()
	objectKey := "s3-location"

	entry := &models.LogsV2DDBEntry{
		BaseItem: collection.NewBaseItem(
			logV2PartitionKeyFixture,
			logV2SortKeyFixture,
			logTagFixture,
		).WithObjectKey(objectKey),
		IndexPartitionKey: logsV2IndexPartitionKeyFixture,
		IndexSortKey:      logsV2IndexSortKeyFixture,
		Height:            logHeightFixture,
		Hash:              logHashFixture,
		LogsBloom:         logsBloomFixture,
		BlockTime:         logsBlockTimeFixture,
	}

	entries := make([]interface{}, 1)
	entries[0] = entry

	s.logsStorage.EXPECT().QueryItemByMaxSortKey(
		gomock.Any(),
		logV2PartitionKeyFixture,
		logV2SortKeyFixture,
		models.LogsV2DDBEntry{},
		nil,
	).Return(&collection.QueryItemsResult{Items: entries, LastEvaluatedKey: nil}, nil)

	s.logsStorage.EXPECT().DownloadFromBlobStorage(gomock.Any(), entry).
		Return(xerrors.Errorf("failed to download"))

	logs, err := s.storage.GetLogsV2(ctx, logTagFixture, logHeightFixture, logSequenceFixture)
	require.Error(err)
	require.Nil(logs)
}
