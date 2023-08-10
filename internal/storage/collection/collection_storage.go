package collection

import (
	"context"
	"reflect"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	"github.com/uber-go/tally/v4"
	"go.uber.org/fx"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainnode/internal/api"
	"github.com/coinbase/chainnode/internal/config"
	"github.com/coinbase/chainnode/internal/storage/blob"
	"github.com/coinbase/chainnode/internal/utils/fxparams"
	"github.com/coinbase/chainnode/internal/utils/instrument"
)

type (
	CollectionStorageParams struct {
		fx.In
		fxparams.Params
		blob.BlobStorage
		AWSConfig  *aws.Config
		EmptyTable *emptyTableOption `optional:"true"`
		DynamoAPI  DynamoAPI         `optional:"true"`
		Session    *session.Session
	}

	CollectionStorage interface {
		// WithCollection initializes the collection context, which is used for instrumentation.
		// This method must be called before calling other methods.
		WithCollection(collection api.Collection) CollectionStorage

		WriteItem(ctx context.Context, item interface{}) error
		WriteItems(ctx context.Context, items []interface{}) error
		GetItem(ctx context.Context, request *GetItemRequest) (interface{}, error)
		GetItems(ctx context.Context, request *GetItemsRequest) ([]interface{}, error)
		UpdateItem(ctx context.Context, request *UpdateItemRequest) (interface{}, error)
		TransactWriteItems(ctx context.Context, request *TransactWriteItemsRequest) error

		// UploadToBlobStorage uploads data to blob storage if the size exceeds the maxDataSize
		// always upload to blob storage if enforceUpload is true
		UploadToBlobStorage(ctx context.Context, entry Item, enforceUpload bool) error

		// DownloadFromBlobStorage downloads data from blob storage if the object key is not empty
		DownloadFromBlobStorage(ctx context.Context, entry Item) error

		// QueryItemByMaxSortKey queries for item whose partitionKey=`partitionKey` and sortkey<=`sortKey`
		QueryItemByMaxSortKey(ctx context.Context, partitionKey string, sortKey string,
			modelType interface{}, selectedAttributeNames []string) (*QueryItemsResult, error)

		// QueryItems queries items
		QueryItems(ctx context.Context, request *QueryItemsRequest) (*QueryItemsResult, error)
	}

	collectionStorageImpl struct {
		table       ddbTable
		config      *config.Config
		scope       tally.Scope
		blobStorage blob.BlobStorage
		collection  api.Collection
		metrics     *collectionStorageMetrics
	}

	collectionStorageMetrics struct {
		writeItem               instrument.Call
		writeItems              instrument.Call
		transactWriteItems      instrument.Call
		getItem                 instrument.Call
		getItems                instrument.Call
		updateItem              instrument.Call
		uploadToBlobStorage     instrument.Call
		downloadFromBlobStorage instrument.Call
		queryItemByMaxSortKey   instrument.Call
		queryItems              instrument.Call
	}
)

const (
	partitionKeyName = "pk"
	sortKeyName      = "sk"

	// used in logs table
	blockByRangeIndexPartitionKeyName = "bbr_pk"
	blockByRangeIndexSortKeyName      = "bbr_sk"
	logsBloomAttributeName            = "logs_bloom"
)

var (
	commonAttributeDefinitions = []*dynamodb.AttributeDefinition{
		{
			AttributeName: aws.String(partitionKeyName),
			AttributeType: awsStringType,
		},
		{
			AttributeName: aws.String(sortKeyName),
			AttributeType: awsStringType,
		},
	}
	commonKeySchema = []*dynamodb.KeySchemaElement{
		{
			AttributeName: aws.String(partitionKeyName),
			KeyType:       hashKeyType,
		},
		{
			AttributeName: aws.String(sortKeyName),
			KeyType:       rangeKeyType,
		},
	}
)

var _ CollectionStorage = (*collectionStorageImpl)(nil)

func NewCollectionStorage(params CollectionStorageParams) (CollectionStorage, error) {
	var globalSecondaryIndexes []*dynamodb.GlobalSecondaryIndex
	collectionTable, err := newDDBTable(
		params.Config.AWS.DynamoDB.CollectionTable,
		commonKeySchema,
		commonAttributeDefinitions,
		globalSecondaryIndexes,
		params,
	)
	if err != nil {
		return nil, xerrors.Errorf("failed to create collection table: %w", err)
	}

	return &collectionStorageImpl{
		table:       collectionTable,
		config:      params.Config,
		scope:       params.Metrics,
		blobStorage: params.BlobStorage,
	}, nil
}

func NewLogsStorage(params CollectionStorageParams) (CollectionStorage, error) {
	// globalSecondaryIndexes used in local stack
	globalSecondaryIndexes := []*dynamodb.GlobalSecondaryIndex{
		{
			IndexName: aws.String(params.Config.AWS.DynamoDB.LogsByBlockRangeIndexName),
			KeySchema: []*dynamodb.KeySchemaElement{
				{
					AttributeName: aws.String(blockByRangeIndexPartitionKeyName),
					KeyType:       hashKeyType,
				},
				{
					AttributeName: aws.String(blockByRangeIndexSortKeyName),
					KeyType:       rangeKeyType,
				},
			},
			Projection: &dynamodb.Projection{
				ProjectionType: aws.String(dynamodb.ProjectionTypeInclude),
				NonKeyAttributes: []*string{
					aws.String(logsBloomAttributeName),
				},
			},
			ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
				ReadCapacityUnits:  aws.Int64(1),
				WriteCapacityUnits: aws.Int64(1),
			},
		},
	}

	secondaryIndexKeyAttributeDefinitions := []*dynamodb.AttributeDefinition{
		{
			AttributeName: aws.String(blockByRangeIndexPartitionKeyName),
			AttributeType: awsStringType,
		},
		{
			AttributeName: aws.String(blockByRangeIndexSortKeyName),
			AttributeType: awsStringType,
		},
	}
	attributeDefinitions := append(commonAttributeDefinitions, secondaryIndexKeyAttributeDefinitions...)
	collectionTable, err := newDDBTable(
		params.Config.AWS.DynamoDB.LogsTable,
		commonKeySchema,
		attributeDefinitions,
		globalSecondaryIndexes,
		params,
	)
	if err != nil {
		return nil, xerrors.Errorf("failed to create collection table: %w", err)
	}

	return &collectionStorageImpl{
		table:       collectionTable,
		config:      params.Config,
		scope:       params.Metrics,
		blobStorage: params.BlobStorage,
	}, nil
}

func newCollectionStorageMetrics(scope tally.Scope, collection api.Collection) *collectionStorageMetrics {
	tags := map[string]string{"collection": collection.String()}
	scope = scope.SubScope("storage").SubScope("collection").Tagged(tags)
	return &collectionStorageMetrics{
		writeItem:               instrument.NewCall(scope, "write_item"),
		writeItems:              instrument.NewCall(scope, "write_items"),
		transactWriteItems:      instrument.NewCall(scope, "transact_write_items"),
		getItem:                 instrument.NewCall(scope, "get_item"),
		getItems:                instrument.NewCall(scope, "get_items"),
		updateItem:              instrument.NewCall(scope, "update_item"),
		uploadToBlobStorage:     instrument.NewCall(scope, "upload_to_blob_storage"),
		downloadFromBlobStorage: instrument.NewCall(scope, "download_from_blob_storage"),
		queryItemByMaxSortKey:   instrument.NewCall(scope, "query_item_by_max_sort_key"),
		queryItems:              instrument.NewCall(scope, "query_items"),
	}
}

func (s *collectionStorageImpl) WithCollection(collection api.Collection) CollectionStorage {
	clone := *s
	clone.collection = collection
	clone.metrics = newCollectionStorageMetrics(s.scope, collection)
	return &clone
}

func (s *collectionStorageImpl) WriteItem(ctx context.Context, item interface{}) error {
	return s.metrics.writeItem.Instrument(ctx, func(ctx context.Context) error {
		return s.table.WriteItem(ctx, item)
	})
}

func (s *collectionStorageImpl) WriteItems(ctx context.Context, items []interface{}) error {
	return s.metrics.writeItems.Instrument(ctx, func(ctx context.Context) error {
		return s.table.WriteItems(ctx, items)
	})
}

func (s *collectionStorageImpl) TransactWriteItems(ctx context.Context, request *TransactWriteItemsRequest) error {
	return s.metrics.transactWriteItems.Instrument(ctx, func(ctx context.Context) error {
		return s.table.TransactWriteItems(ctx, request)
	})
}

func (s *collectionStorageImpl) GetItem(ctx context.Context, request *GetItemRequest) (interface{}, error) {
	var res interface{}
	err := s.metrics.getItem.Instrument(ctx, func(ctx context.Context) error {
		v, err := s.table.GetItem(ctx, request)
		if err != nil {
			return err
		}

		res = v
		return nil
	})

	return res, err
}

func (s *collectionStorageImpl) GetItems(ctx context.Context, request *GetItemsRequest) ([]interface{}, error) {
	var res []interface{}
	err := s.metrics.getItems.Instrument(ctx, func(ctx context.Context) error {
		v, err := s.table.GetItems(ctx, request)
		if err != nil {
			return err
		}

		res = v
		return nil
	})

	return res, err
}

func (s *collectionStorageImpl) UpdateItem(ctx context.Context, request *UpdateItemRequest) (interface{}, error) {
	var res interface{}
	err := s.metrics.updateItem.Instrument(ctx, func(ctx context.Context) error {
		v, err := s.table.UpdateItem(ctx, request)
		if err != nil {
			return err
		}

		res = v
		return nil
	})

	return res, err
}

func (s *collectionStorageImpl) UploadToBlobStorage(ctx context.Context, entry Item, enforceUpload bool) error {
	data, err := entry.GetData()
	if err != nil {
		return xerrors.Errorf("failed to get data from entry: %w", err)
	}

	if !enforceUpload && len(data) < s.config.AWS.DynamoDB.MaxDataSize {
		err := entry.SetObjectKey("")
		if err != nil {
			return xerrors.Errorf("failed to set object key for entry: %w", err)
		}
		return nil
	}

	return s.metrics.uploadToBlobStorage.Instrument(ctx, func(ctx context.Context) error {
		return s.uploadToBlobStorage(ctx, entry, data)
	})
}

func (s *collectionStorageImpl) uploadToBlobStorage(ctx context.Context, entry Item, data []byte) error {
	objectKey, err := entry.MakeObjectKey()
	if err != nil {
		return xerrors.Errorf("failed to make object key for entry: %w", err)
	}

	err = s.blobStorage.Upload(ctx, objectKey, data)
	if err != nil {
		return xerrors.Errorf("failed to upload block data to s3: %w", err)
	}

	err = entry.SetObjectKey(objectKey)
	if err != nil {
		return xerrors.Errorf("failed to set object key for entry: %w", err)
	}

	err = entry.SetData(nil)
	if err != nil {
		return xerrors.Errorf("failed to set data for entry: %w", err)
	}

	return nil
}

func (s *collectionStorageImpl) DownloadFromBlobStorage(ctx context.Context, entry Item) error {
	return s.metrics.downloadFromBlobStorage.Instrument(ctx, func(ctx context.Context) error {
		return s.downloadFromBlobStorage(ctx, entry)
	})
}

func (s *collectionStorageImpl) downloadFromBlobStorage(ctx context.Context, entry Item) error {
	objectKey, err := entry.GetObjectKey()
	if err != nil {
		return xerrors.Errorf("failed to get object key from entry: %w", err)
	}

	if objectKey == "" {
		return xerrors.Errorf("object key cannot be empty")
	}

	data, err := s.blobStorage.Download(ctx, objectKey)
	if err != nil {
		return xerrors.Errorf("failed to download from blobStorage: %w", err)
	}

	err = entry.SetData(data)
	if err != nil {
		return xerrors.Errorf("failed to set data for entry: %w", err)
	}

	return nil
}

func (s *collectionStorageImpl) QueryItemByMaxSortKey(
	ctx context.Context,
	partitionKey string,
	sortKey string,
	modelType interface{},
	selectedAttributeNames []string,
) (*QueryItemsResult, error) {
	var res *QueryItemsResult
	err := s.metrics.queryItemByMaxSortKey.Instrument(ctx, func(ctx context.Context) error {
		v, err := s.queryItemByMaxSortKey(ctx, partitionKey, sortKey, modelType, selectedAttributeNames)
		if err != nil {
			return err
		}

		res = v
		return nil
	})

	return res, err
}

func (s *collectionStorageImpl) queryItemByMaxSortKey(
	ctx context.Context,
	partitionKey string,
	sortKey string,
	modelType interface{},
	selectedAttributeNames []string,
) (*QueryItemsResult, error) {
	pkCondition := expression.Key(PartitionKeyName).
		Equal(expression.Value(partitionKey))
	skCondition := expression.Key(SortKeyName).LessThanEqual(expression.Value(sortKey))
	keyCondition := pkCondition.And(skCondition)

	builder := expression.NewBuilder().WithKeyCondition(keyCondition)

	var projection expression.ProjectionBuilder
	if len(selectedAttributeNames) != 0 {
		for _, attr := range selectedAttributeNames {
			projection = expression.AddNames(projection, expression.Name(attr))
		}
		builder = builder.WithProjection(projection)
	}

	expr, err := builder.Build()
	if err != nil {
		return nil, xerrors.Errorf("failed to build expression for QueryItems: %w", err)
	}

	return s.table.QueryItems(ctx, &QueryItemsRequest{
		ProjectionExpression:      expr.Projection(),
		ExclusiveStartKey:         nil,
		KeyConditionExpression:    expr.KeyCondition(),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		ScanIndexForward:          false,
		Limit:                     1,
		EntryType:                 reflect.TypeOf(modelType),
		ConsistentRead:            true,
	})
}

func (s *collectionStorageImpl) QueryItems(ctx context.Context, request *QueryItemsRequest) (*QueryItemsResult, error) {
	var res *QueryItemsResult
	err := s.metrics.queryItems.Instrument(ctx, func(ctx context.Context) error {
		v, err := s.table.QueryItems(ctx, request)
		if err != nil {
			return err
		}

		res = v
		return nil
	})

	return res, err
}
