package collection_test

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	awsrequest "github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/fx"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainnode/internal/api"
	"github.com/coinbase/chainnode/internal/config"
	"github.com/coinbase/chainnode/internal/storage/blob"
	blobmocks "github.com/coinbase/chainnode/internal/storage/blob/mocks"
	"github.com/coinbase/chainnode/internal/storage/collection"
	collectionmocks "github.com/coinbase/chainnode/internal/storage/collection/mocks"
	"github.com/coinbase/chainnode/internal/storage/internal"
	"github.com/coinbase/chainnode/internal/utils/pointer"
	"github.com/coinbase/chainnode/internal/utils/testapp"
	"github.com/coinbase/chainnode/internal/utils/testutil"
)

type (
	CollectionStorageTestSuite struct {
		suite.Suite
		ctrl        *gomock.Controller
		blobStorage *blobmocks.MockBlobStorage
		dynamoAPI   *collectionmocks.MockDynamoAPI
		storage     collection.CollectionStorage
		cfg         *config.Config
		app         testapp.TestApp
	}

	testDDBEntry struct {
		*collection.BaseItem
		Height uint64 `dynamodbav:"height"`

		ExpectedObjectKey string // the object key returned by MakeObjectKey
	}
)

var _ collection.Item = (*testDDBEntry)(nil)

func TestCollectionStorageTestSuite(t *testing.T) {
	suite.Run(t, new(CollectionStorageTestSuite))
}

func (s *CollectionStorageTestSuite) SetupTest() {
	var deps struct {
		fx.In
		Storage collection.CollectionStorage `name:"collection"`
	}
	require := testutil.Require(s.T())

	s.ctrl = gomock.NewController(s.T())
	s.blobStorage = blobmocks.NewMockBlobStorage(s.ctrl)
	s.dynamoAPI = collectionmocks.NewMockDynamoAPI(s.ctrl)

	cfg, err := config.New()
	require.NoError(err)
	s.cfg = cfg
	s.app = testapp.New(
		s.T(),
		collection.Module,
		testapp.WithConfig(cfg),
		fx.Provide(func() blob.BlobStorage { return s.blobStorage }),
		fx.Provide(func() collection.DynamoAPI { return s.dynamoAPI }),
		fx.Populate(&deps),
	)
	s.storage = deps.Storage.WithCollection(api.CollectionCheckpoints)
}

func (s *CollectionStorageTestSuite) TearDownTest() {
	s.app.Close()
	s.ctrl.Finish()
}

func (s *CollectionStorageTestSuite) TestUploadToBlobStorage_LargeFile() {
	require := testutil.Require(s.T())

	// set a small MaxDataSize so that the data exceeds the limit
	s.cfg.AWS.DynamoDB.MaxDataSize = 1

	expectedData := []byte("test data")
	expectedKey := "expected key"
	entry := makeTestDDBEntry("", "", expectedData, "", expectedKey)
	s.blobStorage.EXPECT().Upload(gomock.Any(), expectedKey, expectedData).Return(nil)

	err := s.storage.UploadToBlobStorage(context.Background(), entry, false)
	require.NoError(err)
	key, err := entry.GetObjectKey()
	require.NoError(err)
	require.Equal(expectedKey, key)
	data, err := entry.GetData()
	require.NoError(err)
	require.Nil(data)
}

func (s *CollectionStorageTestSuite) TestUploadToBlobStorage_EnforceUpload() {
	require := testutil.Require(s.T())

	expectedData := []byte("test data")
	expectedKey := "expected key"
	entry := makeTestDDBEntry("", "", expectedData, "", expectedKey)
	s.blobStorage.EXPECT().Upload(gomock.Any(), expectedKey, expectedData).Return(nil)

	err := s.storage.UploadToBlobStorage(context.Background(), entry, true)
	require.NoError(err)
	key, err := entry.GetObjectKey()
	require.NoError(err)
	require.Equal(expectedKey, key)
	data, err := entry.GetData()
	require.NoError(err)
	require.Nil(data)
}

func (s *CollectionStorageTestSuite) TestUploadToBlobStorage_UploadFailure() {
	require := testutil.Require(s.T())

	// set a small MaxDataSize so that the data exceeds the limit
	s.cfg.AWS.DynamoDB.MaxDataSize = 1

	expectedData := []byte("test data")
	expectedKey := "expected key"
	entry := makeTestDDBEntry("", "", expectedData, "", expectedKey)
	s.blobStorage.EXPECT().Upload(gomock.Any(), expectedKey, expectedData).
		Return(xerrors.Errorf("failed to upload"))

	err := s.storage.UploadToBlobStorage(context.Background(), entry, false)
	require.Error(err)
	key, err := entry.GetObjectKey()
	require.NoError(err)
	require.Empty(key)
	data, err := entry.GetData()
	require.NoError(err)
	require.Equal(expectedData, data)
}

func (s *CollectionStorageTestSuite) TestUploadToBlobStorage_SmallFile() {
	require := testutil.Require(s.T())

	// MaxDataSize > len(data)
	s.cfg.AWS.DynamoDB.MaxDataSize = 100

	expectedData := []byte("test data")
	expectedKey := "dummy key"
	entry := makeTestDDBEntry("", "", expectedData, "", expectedKey)

	err := s.storage.UploadToBlobStorage(context.Background(), entry, false)
	require.NoError(err)
	key, err := entry.GetObjectKey()
	require.NoError(err)
	require.Equal("", key)
	data, err := entry.GetData()
	require.NoError(err)
	require.Equal(expectedData, data)
}

func (s *CollectionStorageTestSuite) TestDownloadFromBlobStorage() {
	require := testutil.Require(s.T())

	expectedData := []byte("test data")
	expectedKey := "expected key"
	entry := makeTestDDBEntry("", "", nil, expectedKey, "dummy key")
	s.blobStorage.EXPECT().Download(gomock.Any(), expectedKey).Return(expectedData, nil)

	err := s.storage.DownloadFromBlobStorage(context.Background(), entry)
	require.NoError(err)
	data, err := entry.GetData()
	require.NoError(err)
	require.Equal(expectedData, data)
}

func (s *CollectionStorageTestSuite) TestDownloadFromBlobStorage_EmptyObjectKey() {
	require := testutil.Require(s.T())

	entry := makeTestDDBEntry("", "", nil, "", "dummy key")
	err := s.storage.DownloadFromBlobStorage(context.Background(), entry)
	require.Error(err)
	data, err := entry.GetData()
	require.NoError(err)
	require.Empty(data)
}

func (s *CollectionStorageTestSuite) TestDownloadFromBlobStorage_DownloadFailure() {
	require := testutil.Require(s.T())

	expectedKey := "expected key"
	entry := makeTestDDBEntry("", "", nil, expectedKey, "dummy key")
	s.blobStorage.EXPECT().Download(gomock.Any(), expectedKey).
		Return(nil, xerrors.Errorf("download failure"))

	err := s.storage.DownloadFromBlobStorage(context.Background(), entry)
	require.Error(err)
	data, err := entry.GetData()
	require.NoError(err)
	require.Empty(data)
}

func (s *CollectionStorageTestSuite) TestWriteItems_1() {
	s.testWriteItems(1)
}

func (s *CollectionStorageTestSuite) TestWriteItems_25() {
	s.testWriteItems(25)
}

func (s *CollectionStorageTestSuite) TestWriteItems_26() {
	s.testWriteItems(26)
}

func (s *CollectionStorageTestSuite) TestWriteItems_67() {
	s.testWriteItems(67)
}

func (s *CollectionStorageTestSuite) TestWriteItems_293() {
	s.testWriteItems(293)
}

func (s *CollectionStorageTestSuite) testWriteItems(numItems int) {
	require := testutil.Require(s.T())

	tableName := s.app.Config().AWS.DynamoDB.CollectionTable
	ctx := context.Background()
	entries := makeTestDDBEntries(numItems)

	seen := sync.Map{}
	s.dynamoAPI.EXPECT().BatchWriteItemWithContext(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx aws.Context, input *dynamodb.BatchWriteItemInput) (*dynamodb.BatchWriteItemOutput, error) {
			for _, writeRequest := range input.RequestItems[tableName] {
				item := writeRequest.PutRequest.Item
				pk := item["pk"]
				require.NotNil(pk.S)
				require.NotEmpty(*pk.S)
				_, ok := seen.LoadOrStore(*pk.S, struct{}{})
				require.False(ok)
			}
			return &dynamodb.BatchWriteItemOutput{}, nil
		}).
		AnyTimes()
	err := s.storage.WriteItems(ctx, entries)
	require.NoError(err)

	for i := 0; i < numItems; i++ {
		pk := fmt.Sprintf("pk-%v", i)
		_, ok := seen.LoadOrStore(pk, struct{}{})
		require.True(ok, fmt.Sprintf("pk %v not seen", pk))
	}
}

func (s *CollectionStorageTestSuite) TestWriteItems_RetrySuccess() {
	require := testutil.Require(s.T())
	tableName := s.app.Config().AWS.DynamoDB.CollectionTable
	ctx := context.Background()
	const numItems = 23
	entries := makeTestDDBEntries(numItems)

	attempts := 0
	s.dynamoAPI.EXPECT().BatchWriteItemWithContext(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx aws.Context, input *dynamodb.BatchWriteItemInput) (*dynamodb.BatchWriteItemOutput, error) {
			attempts += 1
			if attempts == 1 {
				// For the first attempt, expects to see all items.
				require.Equal(numItems, len(input.RequestItems[tableName]))

				item, err := dynamodbattribute.MarshalMap(entries[0])
				require.NoError(err)
				return &dynamodb.BatchWriteItemOutput{
					UnprocessedItems: map[string][]*dynamodb.WriteRequest{
						tableName: {
							{
								PutRequest: &dynamodb.PutRequest{
									Item: item,
								},
							},
						},
					},
				}, nil
			}

			// For the subsequent attempts, expects to see only one item.
			require.Equal(1, len(input.RequestItems[tableName]))
			item := input.RequestItems[tableName][0].PutRequest.Item
			pk := pointer.StringDeref(item["pk"].S)
			require.Equal("pk-0", pk)
			if attempts == 2 {
				item, err := dynamodbattribute.MarshalMap(entries[0])
				require.NoError(err)
				return &dynamodb.BatchWriteItemOutput{
					UnprocessedItems: map[string][]*dynamodb.WriteRequest{
						tableName: {
							{
								PutRequest: &dynamodb.PutRequest{
									Item: item,
								},
							},
						},
					},
				}, nil
			}

			return &dynamodb.BatchWriteItemOutput{}, nil
		}).
		AnyTimes()
	err := s.storage.WriteItems(ctx, entries)
	require.NoError(err)
}

func (s *CollectionStorageTestSuite) TestWriteItems_RetryFailure() {
	require := testutil.Require(s.T())
	tableName := s.app.Config().AWS.DynamoDB.CollectionTable
	ctx := context.Background()
	const numItems = 23
	entries := makeTestDDBEntries(numItems)

	s.dynamoAPI.EXPECT().BatchWriteItemWithContext(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx aws.Context, input *dynamodb.BatchWriteItemInput) (*dynamodb.BatchWriteItemOutput, error) {
			item, err := dynamodbattribute.MarshalMap(entries[0])
			require.NoError(err)
			return &dynamodb.BatchWriteItemOutput{
				UnprocessedItems: map[string][]*dynamodb.WriteRequest{
					tableName: {
						{
							PutRequest: &dynamodb.PutRequest{
								Item: item,
							},
						},
					},
				},
			}, nil

		}).
		AnyTimes()
	err := s.storage.WriteItems(ctx, entries)
	require.Error(err)
	require.Contains(err.Error(), "failed to process 1 items during batch write items")
}

func (s *CollectionStorageTestSuite) TestTransactWriteItems_26() {
	require := testutil.Require(s.T())

	numItems := 26
	transactItems := make([]*collection.TransactItem, numItems)
	err := s.storage.TransactWriteItems(context.Background(), &collection.TransactWriteItemsRequest{TransactItems: transactItems})
	require.Error(err)
	require.Contains(err.Error(), "too many items")
}

func (s *CollectionStorageTestSuite) TestTransactWriteItemsWithPut_1() {
	s.testTransactWriteItemsWithPut(1)
}

func (s *CollectionStorageTestSuite) TestTransactWriteItemsWithPut_10() {
	s.testTransactWriteItemsWithPut(10)
}

func (s *CollectionStorageTestSuite) TestTransactWriteItemsWithPut_25() {
	s.testTransactWriteItemsWithPut(25)
}

func (s *CollectionStorageTestSuite) testTransactWriteItemsWithPut(numItems int) {
	require := testutil.Require(s.T())

	tableName := s.app.Config().AWS.DynamoDB.CollectionTable
	ctx := context.Background()
	entries := makeTestDDBEntries(numItems)
	transactItems := make([]*collection.TransactItem, numItems)
	for i := range transactItems {
		transactItems[i] = &collection.TransactItem{
			Put: &collection.TransactPutItem{Item: entries[i]},
		}
	}

	seen := sync.Map{}
	s.dynamoAPI.EXPECT().TransactWriteItemsWithContext(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx aws.Context, input *dynamodb.TransactWriteItemsInput) (*dynamodb.TransactWriteItemsOutput, error) {
			for _, transactItem := range input.TransactItems {
				require.Equal(tableName, *transactItem.Put.TableName)
				item := transactItem.Put.Item
				pk := item["pk"]
				require.NotNil(pk.S)
				require.NotEmpty(*pk.S)
				_, ok := seen.LoadOrStore(*pk.S, struct{}{})
				require.False(ok)
			}
			return &dynamodb.TransactWriteItemsOutput{}, nil
		}).
		AnyTimes()

	err := s.storage.TransactWriteItems(ctx, &collection.TransactWriteItemsRequest{TransactItems: transactItems})
	require.NoError(err)

	for i := 0; i < numItems; i++ {
		pk := fmt.Sprintf("pk-%v", i)
		_, ok := seen.LoadOrStore(pk, struct{}{})
		require.True(ok, fmt.Sprintf("pk %v not seen", pk))
	}
}

func (s *CollectionStorageTestSuite) TestTransactWriteItemsWithUpdate_1() {
	s.testTransactWriteItemsWithUpdate(1)
}

func (s *CollectionStorageTestSuite) TestTransactWriteItemsWithUpdate_10() {
	s.testTransactWriteItemsWithUpdate(10)
}

func (s *CollectionStorageTestSuite) TestTransactWriteItemsWithUpdate_25() {
	s.testTransactWriteItemsWithUpdate(25)
}

func (s *CollectionStorageTestSuite) testTransactWriteItemsWithUpdate(numItems int) {
	require := testutil.Require(s.T())

	tableName := s.app.Config().AWS.DynamoDB.CollectionTable
	ctx := context.Background()

	entries := makeTestDDBEntries(numItems)
	updateExpressions, err := testDDBEntriesToUpdateExpression(entries)
	require.NoError(err)
	require.Equal(numItems, len(updateExpressions))
	attributeMaps, err := testDDBEntriesToAttributeMaps(entries)
	require.NoError(err)

	transactItems := make([]*collection.TransactItem, numItems)
	for i := range transactItems {
		transactItems[i] = &collection.TransactItem{
			Update: &collection.TransactUpdateItem{
				ExpressionAttributesNames: updateExpressions[i].Names(),
				ExpressionAttributeValues: updateExpressions[i].Values(),
				Key:                       attributeMaps[i],
				UpdateExpression:          updateExpressions[i].Update(),
			},
		}
	}

	seen := sync.Map{}
	s.dynamoAPI.EXPECT().TransactWriteItemsWithContext(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx aws.Context, input *dynamodb.TransactWriteItemsInput) (*dynamodb.TransactWriteItemsOutput, error) {
			for _, transactItem := range input.TransactItems {
				require.Equal(tableName, *transactItem.Update.TableName)
				pk := transactItem.Update.Key["pk"]
				require.NotNil(pk.S)
				require.NotEmpty(*pk.S)
				_, ok := seen.LoadOrStore(*pk.S, struct{}{})
				require.False(ok)
			}
			return &dynamodb.TransactWriteItemsOutput{}, nil
		}).
		AnyTimes()

	err = s.storage.TransactWriteItems(ctx, &collection.TransactWriteItemsRequest{TransactItems: transactItems})
	require.NoError(err)

	for i := 0; i < numItems; i++ {
		pk := fmt.Sprintf("pk-%v", i)
		_, ok := seen.LoadOrStore(pk, struct{}{})
		require.True(ok, fmt.Sprintf("pk %v not seen", pk))
	}
}

func (s *CollectionStorageTestSuite) TestQueryItemByMaxSortKey() {
	require := testutil.Require(s.T())

	pk := "pk"
	sk := "sk"
	data := []byte("test-data")

	items := make([]map[string]*dynamodb.AttributeValue, 1)
	items[0] = map[string]*dynamodb.AttributeValue{
		"data": {
			B: data,
		},
		"pk": {
			S: aws.String(pk),
		},
		"sk": {
			S: aws.String(sk),
		},
	}

	s.dynamoAPI.EXPECT().QueryWithContext(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, input *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
			require.NotNil(input)
			require.NotEmpty(input.ExpressionAttributeValues)
			require.NotEmpty(input.ExpressionAttributeNames)
			require.False(*input.ScanIndexForward)
			require.Equal(int64(1), *input.Limit)
			require.Equal(s.cfg.AWS.DynamoDB.CollectionTable, *input.TableName)
			require.Equal(aws.String(dynamodb.SelectAllAttributes), input.Select)
			require.Empty(input.ProjectionExpression)
			require.Nil(input.ExclusiveStartKey)
			require.NotNil(input.KeyConditionExpression)
			return &dynamodb.QueryOutput{
				Items:            items,
				LastEvaluatedKey: nil,
			}, nil
		})
	queryResult, err := s.storage.QueryItemByMaxSortKey(
		context.Background(), pk, sk, testDDBEntry{}, nil,
	)
	require.NoError(err)
	require.NotNil(queryResult)
	require.Len(queryResult.Items, 1)
	entry, ok := queryResult.Items[0].(*testDDBEntry)
	require.True(ok)
	require.Equal(data, entry.Data)
	require.Equal(pk, entry.PartitionKey)
	require.Equal(sk, entry.SortKey)
}

func (s *CollectionStorageTestSuite) TestQueryItemByMaxSortKey_WithSelect() {
	require := testutil.Require(s.T())

	pk := "pk"
	sk := "sk"
	data := []byte("test-data")

	items := make([]map[string]*dynamodb.AttributeValue, 1)
	items[0] = map[string]*dynamodb.AttributeValue{
		"data": {
			B: data,
		},
	}

	s.dynamoAPI.EXPECT().QueryWithContext(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, input *dynamodb.QueryInput) (*dynamodb.QueryOutput, error) {
			require.NotNil(input)
			require.NotEmpty(input.ExpressionAttributeValues)
			require.NotEmpty(input.ExpressionAttributeNames)
			require.False(*input.ScanIndexForward)
			require.Equal(int64(1), *input.Limit)
			require.Equal(s.cfg.AWS.DynamoDB.CollectionTable, *input.TableName)
			require.NotEmpty(input.Select)
			require.NotEmpty(input.ProjectionExpression)
			require.Nil(input.ExclusiveStartKey)
			require.NotNil(input.KeyConditionExpression)
			return &dynamodb.QueryOutput{
				Items:            items,
				LastEvaluatedKey: nil,
			}, nil
		})
	queryResult, err := s.storage.QueryItemByMaxSortKey(
		context.Background(), pk, sk, testDDBEntry{}, []string{"data"},
	)
	require.NoError(err)
	require.NotNil(queryResult)
	require.Len(queryResult.Items, 1)
	entry, ok := queryResult.Items[0].(*testDDBEntry)
	require.True(ok)
	require.Equal(data, entry.Data)
}

func (s *CollectionStorageTestSuite) TestQueryItemByMaxSortKey_Canceled() {
	require := testutil.Require(s.T())

	pk := "pk"
	sk := "sk"

	s.dynamoAPI.EXPECT().QueryWithContext(gomock.Any(), gomock.Any()).
		Return(nil, awserr.New(awsrequest.CanceledErrorCode,
			"canceled",
			nil))
	queryResult, err := s.storage.QueryItemByMaxSortKey(
		context.Background(), pk, sk, testDDBEntry{}, nil,
	)
	require.Error(err)
	require.True(xerrors.Is(err, internal.ErrRequestCanceled))
	require.Nil(queryResult)
}

func (s *CollectionStorageTestSuite) TestQueryItemByMaxSortKey_OtherError() {
	require := testutil.Require(s.T())

	pk := "pk"
	sk := "sk"

	s.dynamoAPI.EXPECT().QueryWithContext(gomock.Any(), gomock.Any()).
		Return(nil, awserr.New(dynamodb.ErrCodeIndexNotFoundException,
			"other",
			nil))
	queryResult, err := s.storage.QueryItemByMaxSortKey(
		context.Background(), pk, sk, testDDBEntry{}, nil,
	)
	require.Error(err)
	require.Nil(queryResult)
}

func (s *CollectionStorageTestSuite) TestGetItems_1() {
	s.testGetItems(1)
}

func (s *CollectionStorageTestSuite) TestGetItems_50() {
	s.testGetItems(50)
}

func (s *CollectionStorageTestSuite) TestGetItems_51() {
	s.testGetItems(51)
}

func (s *CollectionStorageTestSuite) TestGetItems_99() {
	s.testGetItems(99)
}

func (s *CollectionStorageTestSuite) TestGetItems_501() {
	s.testGetItems(501)
}

func (s *CollectionStorageTestSuite) testGetItems(numItems int) {
	require := testutil.Require(s.T())

	tableName := s.app.Config().AWS.DynamoDB.CollectionTable
	ctx := context.Background()
	entries := makeTestDDBEntries(numItems)
	attributesMap, err := testDDBEntriesToAttributeMaps(entries)
	require.NoError(err)
	keyMaps := makeKeyMapsForTestDDBEntries(numItems)

	seen := sync.Map{}
	s.dynamoAPI.EXPECT().TransactGetItemsWithContext(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, input *dynamodb.TransactGetItemsInput) (
			*dynamodb.TransactGetItemsOutput, error) {
			transactItems := input.TransactItems
			begin := numItems
			end := -1
			for _, item := range transactItems {
				var out collection.StringMap
				require.Equal(tableName, *item.Get.TableName)
				err := dynamodbattribute.UnmarshalMap(item.Get.Key, &out)
				require.NoError(err)
				require.NotEmpty(out["pk"])
				require.NotEmpty(out["sk"])

				indexString := strings.Split(out["pk"].(string), "-")[1]
				index, err := strconv.ParseInt(indexString, 10, 64)
				require.NoError(err)
				_, ok := seen.LoadOrStore(index, struct{}{})
				require.False(ok)

				if int(index) > end {
					end = int(index)
				}
				if int(index) < begin {
					begin = int(index)
				}
			}

			responses := make([]*dynamodb.ItemResponse, end+1-begin)
			for i := begin; i <= end; i++ {
				responses[i-begin] = &dynamodb.ItemResponse{Item: attributesMap[i]}
			}
			return &dynamodb.TransactGetItemsOutput{
				Responses: responses,
			}, nil
		}).AnyTimes()
	items, err := s.storage.GetItems(ctx, &collection.GetItemsRequest{
		KeyMaps:   keyMaps,
		EntryType: reflect.TypeOf(testDDBEntry{}),
	})
	require.NoError(err)
	require.Equal(numItems, len(items))

	for i := 0; i < numItems; i++ {
		_, ok := seen.LoadOrStore(int64(i), struct{}{})
		require.True(ok, fmt.Sprintf("items[%v] not seen", i))
	}
}

func (s *CollectionStorageTestSuite) TestGetItems_NilItem() {
	require := testutil.Require(s.T())

	numItems := 23
	tableName := s.app.Config().AWS.DynamoDB.CollectionTable
	ctx := context.Background()
	entries := makeTestDDBEntries(numItems)
	attributesMap, err := testDDBEntriesToAttributeMaps(entries)
	require.NoError(err)
	keyMaps := makeKeyMapsForTestDDBEntries(numItems)

	seen := sync.Map{}
	s.dynamoAPI.EXPECT().TransactGetItemsWithContext(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, input *dynamodb.TransactGetItemsInput) (
			*dynamodb.TransactGetItemsOutput, error) {
			transactItems := input.TransactItems
			begin := numItems
			end := -1
			for _, item := range transactItems {
				var out collection.StringMap
				require.Equal(tableName, *item.Get.TableName)
				err := dynamodbattribute.UnmarshalMap(item.Get.Key, &out)
				require.NoError(err)
				require.NotEmpty(out["pk"])
				require.NotEmpty(out["sk"])

				indexString := strings.Split(out["pk"].(string), "-")[1]
				index, err := strconv.ParseInt(indexString, 10, 64)
				require.NoError(err)
				_, ok := seen.LoadOrStore(index, struct{}{})
				require.False(ok)

				if int(index) > end {
					end = int(index)
				}
				if int(index) < begin {
					begin = int(index)
				}
			}

			responses := make([]*dynamodb.ItemResponse, end+1-begin)
			// the first item is missing
			for i := begin + 1; i <= end; i++ {
				responses[i-begin] = &dynamodb.ItemResponse{Item: attributesMap[i]}
			}
			return &dynamodb.TransactGetItemsOutput{
				Responses: responses,
			}, nil
		}).AnyTimes()
	_, err = s.storage.GetItems(ctx, &collection.GetItemsRequest{
		KeyMaps:   keyMaps,
		EntryType: reflect.TypeOf(testDDBEntry{}),
	})
	require.Error(err)
	require.True(xerrors.Is(err, internal.ErrItemNotFound))
}

func (s *CollectionStorageTestSuite) TestGetItems_EmptyItem() {
	require := testutil.Require(s.T())

	numItems := 23
	tableName := s.app.Config().AWS.DynamoDB.CollectionTable
	ctx := context.Background()
	entries := makeTestDDBEntries(numItems)
	attributesMap, err := testDDBEntriesToAttributeMaps(entries)
	require.NoError(err)
	keyMaps := makeKeyMapsForTestDDBEntries(numItems)

	seen := sync.Map{}
	s.dynamoAPI.EXPECT().TransactGetItemsWithContext(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, input *dynamodb.TransactGetItemsInput) (
			*dynamodb.TransactGetItemsOutput, error) {
			transactItems := input.TransactItems
			begin := numItems
			end := -1
			for _, item := range transactItems {
				var out collection.StringMap
				require.Equal(tableName, *item.Get.TableName)
				err := dynamodbattribute.UnmarshalMap(item.Get.Key, &out)
				require.NoError(err)
				require.NotEmpty(out["pk"])
				require.NotEmpty(out["sk"])

				indexString := strings.Split(out["pk"].(string), "-")[1]
				index, err := strconv.ParseInt(indexString, 10, 64)
				require.NoError(err)
				_, ok := seen.LoadOrStore(index, struct{}{})
				require.False(ok)

				if int(index) > end {
					end = int(index)
				}
				if int(index) < begin {
					begin = int(index)
				}
			}

			responses := make([]*dynamodb.ItemResponse, end+1-begin)
			// the first item is empty
			responses[0] = &dynamodb.ItemResponse{Item: nil}
			for i := begin + 1; i <= end; i++ {
				responses[i-begin] = &dynamodb.ItemResponse{Item: attributesMap[i]}
			}
			return &dynamodb.TransactGetItemsOutput{
				Responses: responses,
			}, nil
		}).AnyTimes()
	_, err = s.storage.GetItems(ctx, &collection.GetItemsRequest{
		KeyMaps:   keyMaps,
		EntryType: reflect.TypeOf(testDDBEntry{}),
	})
	require.Error(err)
	require.True(xerrors.Is(err, internal.ErrItemNotFound))
}

func (s *CollectionStorageTestSuite) TestGetItems_TransactionConflict_RetrySuccess() {
	require := testutil.Require(s.T())
	numItems := 20

	tableName := s.app.Config().AWS.DynamoDB.CollectionTable
	ctx := context.Background()
	entries := makeTestDDBEntries(numItems)
	attributesMap, err := testDDBEntriesToAttributeMaps(entries)
	require.NoError(err)
	keyMaps := makeKeyMapsForTestDDBEntries(numItems)

	seen := sync.Map{}
	attempts := 0
	s.dynamoAPI.EXPECT().TransactGetItemsWithContext(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, input *dynamodb.TransactGetItemsInput) (
			*dynamodb.TransactGetItemsOutput, error) {
			attempts += 1
			if attempts == 1 {
				require.Equal(numItems, len(input.TransactItems))
				mockCancelReasons := make([]*dynamodb.CancellationReason, numItems)
				for i := range mockCancelReasons {
					mockCancelReasons[i] = &dynamodb.CancellationReason{
						Code: aws.String("None"),
					}
				}

				mockCancelReasons[1] = &dynamodb.CancellationReason{
					Code:    aws.String("TransactionConflict"),
					Message: aws.String("Transaction is ongoing for the item"),
				}
				return &dynamodb.TransactGetItemsOutput{}, &dynamodb.TransactionCanceledException{
					CancellationReasons: mockCancelReasons,
				}
			}

			transactItems := input.TransactItems
			begin := numItems
			end := -1
			for _, item := range transactItems {
				var out collection.StringMap
				require.Equal(tableName, *item.Get.TableName)
				err := dynamodbattribute.UnmarshalMap(item.Get.Key, &out)
				require.NoError(err)
				require.NotEmpty(out["pk"])
				require.NotEmpty(out["sk"])

				indexString := strings.Split(out["pk"].(string), "-")[1]
				index, err := strconv.ParseInt(indexString, 10, 64)
				require.NoError(err)
				_, ok := seen.LoadOrStore(index, struct{}{})
				require.False(ok)

				if int(index) > end {
					end = int(index)
				}
				if int(index) < begin {
					begin = int(index)
				}
			}

			responses := make([]*dynamodb.ItemResponse, end+1-begin)
			for i := begin; i <= end; i++ {
				responses[i-begin] = &dynamodb.ItemResponse{Item: attributesMap[i]}
			}
			return &dynamodb.TransactGetItemsOutput{
				Responses: responses,
			}, nil
		}).Times(2)
	items, err := s.storage.GetItems(ctx, &collection.GetItemsRequest{
		KeyMaps:   keyMaps,
		EntryType: reflect.TypeOf(testDDBEntry{}),
	})
	require.NoError(err)
	require.Equal(numItems, len(items))

	for i := 0; i < numItems; i++ {
		_, ok := seen.LoadOrStore(int64(i), struct{}{})
		require.True(ok, fmt.Sprintf("items[%v] not seen", i))
	}
}

func (s *CollectionStorageTestSuite) TestGetItems_TransactionConflict_RetryFailure() {
	require := testutil.Require(s.T())
	numItems := 20

	ctx := context.Background()
	keyMaps := makeKeyMapsForTestDDBEntries(numItems)

	s.dynamoAPI.EXPECT().TransactGetItemsWithContext(gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, input *dynamodb.TransactGetItemsInput) (
			*dynamodb.TransactGetItemsOutput, error) {
			require.Equal(numItems, len(input.TransactItems))
			mockCancelReasons := make([]*dynamodb.CancellationReason, numItems)
			for i := range mockCancelReasons {
				mockCancelReasons[i] = &dynamodb.CancellationReason{
					Code: aws.String("None"),
				}
			}

			mockCancelReasons[1] = &dynamodb.CancellationReason{
				Code:    aws.String("TransactionConflict"),
				Message: aws.String("Transaction is ongoing for the item"),
			}
			return &dynamodb.TransactGetItemsOutput{}, &dynamodb.TransactionCanceledException{
				CancellationReasons: mockCancelReasons,
			}
		}).AnyTimes()
	_, err := s.storage.GetItems(ctx, &collection.GetItemsRequest{
		KeyMaps:   keyMaps,
		EntryType: reflect.TypeOf(testDDBEntry{}),
	})
	require.Error(err)
}

func makeTestDDBEntry(pk string, sk string, data []byte, objectKey string, expectedObjectKey string) *testDDBEntry {
	if pk == "" {
		pk = "pk"
	}

	if sk == "" {
		sk = "sk"
	}

	return &testDDBEntry{
		BaseItem: collection.NewBaseItem(
			pk,
			sk,
			uint32(2),
		).WithObjectKey(objectKey).WithData(data),
		ExpectedObjectKey: expectedObjectKey,
	}
}

func makeTestDDBEntries(numEntries int) []interface{} {
	entries := make([]interface{}, numEntries)
	for i := 0; i < numEntries; i++ {
		pk := fmt.Sprintf("pk-%v", i)
		sk := fmt.Sprintf("sk-%v", i)
		data := testutil.MakeFile(i)
		entries[i] = makeTestDDBEntry(pk, sk, data, "", "")
	}

	return entries
}

func testDDBEntriesToAttributeMaps(
	entries []interface{},
) ([]map[string]*dynamodb.AttributeValue, error) {
	attributes := make([]map[string]*dynamodb.AttributeValue, len(entries))
	for i := range entries {
		attribute, err := dynamodbattribute.MarshalMap(entries[i])
		if err != nil {
			return nil, err
		}
		attributes[i] = attribute
	}
	return attributes, nil
}

func makeKeyMapsForTestDDBEntries(numEntries int) []*collection.StringMap {
	results := make([]*collection.StringMap, numEntries)
	for i := 0; i < numEntries; i++ {
		results[i] = &collection.StringMap{
			"pk": fmt.Sprintf("pk-%v", i),
			"sk": fmt.Sprintf("sk-%v", i),
		}
	}
	return results
}

func testDDBEntriesToUpdateExpression(entries []interface{}) ([]expression.Expression, error) {
	expressions := make([]expression.Expression, len(entries))
	for i, entryInterface := range entries {
		entry, ok := entryInterface.(*testDDBEntry)
		if !ok {
			return nil, xerrors.Errorf("wrong type of the entries=%v", entryInterface)
		}

		var err error
		updateExpr := expression.UpdateBuilder{}.
			Set(expression.Name("height"), expression.Value(entry.Height+1))
		expressions[i], err = expression.NewBuilder().WithUpdate(updateExpr).Build()

		if err != nil {
			return nil, err
		}
	}

	return expressions, nil
}

func (t *testDDBEntry) MakeObjectKey() (string, error) {
	return t.ExpectedObjectKey, nil
}

func (t *testDDBEntry) AsAPI(interface{}) error {
	return nil
}
