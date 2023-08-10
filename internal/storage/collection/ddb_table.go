package collection

import (
	"context"
	"reflect"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	awsrequest "github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbiface"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainnode/internal/storage/internal"
	"github.com/coinbase/chainnode/internal/utils/log"
	"github.com/coinbase/chainnode/internal/utils/retry"
	"github.com/coinbase/chainnode/internal/utils/syncgroup"
)

type (
	ddbTable interface {
		WriteItem(ctx context.Context, item interface{}) error
		// WriteItems will parallelize writing items under the hood, but no guarantee on order, may also result in partial write
		WriteItems(ctx context.Context, items []interface{}) error
		// BatchWriteItems write up to maxWriteItemsSize items in batch
		BatchWriteItems(ctx context.Context, items []interface{}) error
		// TransactWriteItems guarantees all or nothing write for input items but does have size limit (maxWriteItemsSize)
		TransactWriteItems(ctx context.Context, request *TransactWriteItemsRequest) error
		GetItem(ctx context.Context, request *GetItemRequest) (interface{}, error)
		GetItems(ctx context.Context, request *GetItemsRequest) ([]interface{}, error)
		TransactGetItems(ctx context.Context, entryType reflect.Type, keys []*StringMap, outputItems []interface{}) error
		UpdateItem(ctx context.Context, request *UpdateItemRequest) (interface{}, error)
		QueryItems(ctx context.Context, request *QueryItemsRequest) (*QueryItemsResult, error)
	}

	// DynamoAPI For mock generation for testing purpose
	DynamoAPI = dynamodbiface.DynamoDBAPI

	ddbTableImpl struct {
		table  *tableDBAPI
		logger *zap.Logger
		retry  retry.Retry
	}

	GetItemRequest struct {
		KeyMap    StringMap
		EntryType reflect.Type
	}

	GetItemsRequest struct {
		KeyMaps   []*StringMap
		EntryType reflect.Type
	}

	BatchGetItemsRequest struct {
		KeyMaps   []*StringMap
		EntryType reflect.Type
	}

	UpdateItemRequest struct {
		ExpressionAttributesNames map[string]*string
		ExpressionAttributeValues map[string]*dynamodb.AttributeValue
		Key                       map[string]*dynamodb.AttributeValue
		UpdateExpression          *string
		EntryType                 reflect.Type
	}

	QueryItemsRequest struct {
		ExclusiveStartKey         map[string]*dynamodb.AttributeValue
		KeyConditionExpression    *string
		ExpressionAttributeNames  map[string]*string
		ExpressionAttributeValues map[string]*dynamodb.AttributeValue
		ProjectionExpression      *string
		FilterExpression          *string
		ScanIndexForward          bool
		Limit                     int64
		IndexName                 string
		EntryType                 reflect.Type
		ConsistentRead            bool
	}

	QueryItemsResult struct {
		Items            []interface{}
		LastEvaluatedKey map[string]*dynamodb.AttributeValue
	}

	TransactWriteItemsRequest struct {
		TransactItems []*TransactItem
	}

	// TransactItem for either Put or Update
	TransactItem struct {
		Put    *TransactPutItem
		Update *TransactUpdateItem
	}

	TransactPutItem struct {
		Item interface{}
	}

	TransactUpdateItem struct {
		ExpressionAttributesNames map[string]*string
		ExpressionAttributeValues map[string]*dynamodb.AttributeValue
		Key                       map[string]*dynamodb.AttributeValue
		UpdateExpression          *string
	}

	transactWriteOperation int

	tableDBAPI struct {
		TableName string
		DBAPI     DynamoAPI
	}

	StringMap map[string]interface{}
)

var (
	awsStringType = aws.String("S")
	hashKeyType   = aws.String("HASH")
	rangeKeyType  = aws.String("RANGE")
)

const (
	maxWriteWorkers         = 10
	maxGetWorkers           = 10
	maxWriteItemsSize       = 25
	maxTransactGetItemsSize = 25

	ddbCodeTransactionConflict = "TransactionConflict"
)

// transactWriteOperation enum
const (
	transactWriteOperationUnknown transactWriteOperation = iota
	transactWriteOperationUpdate
	transactWriteOperationPut
)

func newDDBTable(
	tableName string,
	keySchema []*dynamodb.KeySchemaElement,
	attrDefs []*dynamodb.AttributeDefinition,
	globalSecondaryIndexes []*dynamodb.GlobalSecondaryIndex,
	params CollectionStorageParams,
) (ddbTable, error) {
	if params.EmptyTable != nil {
		return emptyDDBTable{}, nil
	}

	logger := log.WithPackage(params.Logger)
	retry := retry.New(retry.WithLogger(logger))
	if params.DynamoAPI != nil {
		// Injected by tests.
		return &ddbTableImpl{
			table: &tableDBAPI{
				TableName: tableName,
				DBAPI:     params.DynamoAPI,
			},
			logger: logger,
			retry:  retry,
		}, nil
	}

	awsTable := newTableAPI(tableName, params.Session)
	table := ddbTableImpl{
		table:  awsTable,
		logger: logger,
		retry:  retry,
	}
	if params.Config.AWS.IsLocalStack {
		err := initLocalDb(
			awsTable.DBAPI,
			params.Logger,
			awsTable.TableName,
			keySchema, attrDefs, globalSecondaryIndexes,
			params.Config.AWS.IsResetLocal,
		)
		if err != nil {
			return nil, xerrors.Errorf("failed to prepare local resources for event storage: %w", err)
		}
	}
	return &table, nil
}

func newTableAPI(tableName string, session *session.Session) *tableDBAPI {
	return &tableDBAPI{
		TableName: tableName,
		DBAPI:     dynamodb.New(session),
	}
}

func (d *ddbTableImpl) WriteItem(ctx context.Context, item interface{}) error {
	mItem, err := dynamodbattribute.MarshalMap(item)
	if err != nil {
		return xerrors.Errorf("failed to get marshal ddb entry (%v): %w", item, err)
	}
	_, err = d.table.DBAPI.PutItemWithContext(
		ctx,
		&dynamodb.PutItemInput{
			Item:      mItem,
			TableName: aws.String(d.table.TableName),
		},
	)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == awsrequest.CanceledErrorCode {
			return internal.ErrRequestCanceled
		}
		return xerrors.Errorf("failed to write item: %w", err)
	}
	return nil
}

func (d *ddbTableImpl) WriteItems(ctx context.Context, items []interface{}) error {
	// Limit parallel writes to reduce the chance of getting throttled.
	g, ctx := syncgroup.New(ctx, syncgroup.WithThrottling(maxWriteWorkers))
	for i := 0; i < len(items); i += maxWriteItemsSize {
		begin, end := i, i+maxWriteItemsSize
		if end > len(items) {
			end = len(items)
		}

		g.Go(func() error {
			if err := d.BatchWriteItems(ctx, items[begin:end]); err != nil {
				return xerrors.Errorf("failed to write items: %w", err)
			}
			return nil
		})
	}
	return g.Wait()
}

func (d *ddbTableImpl) BatchWriteItems(ctx context.Context, items []interface{}) error {
	numItems := len(items)
	if numItems == 0 {
		return nil
	}

	if numItems > maxWriteItemsSize {
		return xerrors.Errorf("too many items: %v", numItems)
	}

	writeRequests := make([]*dynamodb.WriteRequest, numItems)
	for i, item := range items {
		writeRequest, err := d.getWriteRequest(item)
		if err != nil {
			return xerrors.Errorf("failed to prepare write items: %w", err)
		}

		writeRequests[i] = writeRequest
	}

	tableName := d.table.TableName
	numProcessed := 0
	if err := d.retry.Retry(ctx, func(ctx context.Context) error {
		input := &dynamodb.BatchWriteItemInput{
			RequestItems: map[string][]*dynamodb.WriteRequest{
				tableName: writeRequests,
			},
		}
		output, err := d.table.DBAPI.BatchWriteItemWithContext(ctx, input)
		if err != nil {
			return xerrors.Errorf("failed to batch write items: %w", err)
		}

		unprocessed := output.UnprocessedItems[tableName]
		numProcessed += len(writeRequests) - len(unprocessed)
		if len(unprocessed) > 0 {
			// If DynamoDB returns any unprocessed items, back off and then retry the batch operation on those items.
			// Ref: https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchWriteItem.html
			writeRequests = unprocessed
			return retry.Retryable(xerrors.Errorf("failed to process %v items during batch write items", len(unprocessed)))
		}

		return nil
	}); err != nil {
		return err
	}

	if numItems != numProcessed {
		return xerrors.Errorf("failed to write all items: expected=%v, actual=%v", numItems, numProcessed)
	}

	return nil
}

func (d *ddbTableImpl) TransactWriteItems(ctx context.Context, request *TransactWriteItemsRequest) error {
	numItems := len(request.TransactItems)
	if numItems == 0 {
		return nil
	}

	if numItems > maxWriteItemsSize {
		return xerrors.Errorf("too many items: %v", numItems)
	}

	batchWriteItems := make([]*dynamodb.TransactWriteItem, numItems)
	var err error
	for i, item := range request.TransactItems {
		batchWriteItems[i], err = d.getTransactWriteItem(item)
		if err != nil {
			return xerrors.Errorf("failed to transact write items: %w", err)
		}
	}

	_, err = d.table.DBAPI.TransactWriteItemsWithContext(
		ctx,
		&dynamodb.TransactWriteItemsInput{
			TransactItems: batchWriteItems,
		},
	)

	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == awsrequest.CanceledErrorCode {
			return internal.ErrRequestCanceled
		}
		return xerrors.Errorf("failed to transact write items: %w", err)
	}

	return nil
}

func (d *ddbTableImpl) getWriteRequest(
	ddbEntry interface{}) (*dynamodb.WriteRequest, error) {
	item, err := dynamodbattribute.MarshalMap(ddbEntry)
	if err != nil {
		return nil, xerrors.Errorf("failed to get marshal ddb entry (%v): %w", ddbEntry, err)
	}
	writeRequest := &dynamodb.WriteRequest{
		PutRequest: &dynamodb.PutRequest{
			Item: item,
		},
	}
	return writeRequest, nil
}

func (d *ddbTableImpl) getTransactWriteItem(transactItem *TransactItem) (*dynamodb.TransactWriteItem, error) {
	var operationType transactWriteOperation
	if transactItem.Put != nil && transactItem.Update == nil {
		operationType = transactWriteOperationPut
	} else if transactItem.Update != nil && transactItem.Put == nil {
		operationType = transactWriteOperationUpdate
	} else {
		operationType = transactWriteOperationUnknown
	}

	tableName := aws.String(d.table.TableName)
	switch operationType {
	case transactWriteOperationPut:
		ddbEntry := transactItem.Put.Item
		item, err := dynamodbattribute.MarshalMap(ddbEntry)
		if err != nil {
			return nil, xerrors.Errorf("failed to get marshal ddb entry (%v): %w", ddbEntry, err)
		}

		return &dynamodb.TransactWriteItem{
			Put: &dynamodb.Put{
				TableName: tableName,
				Item:      item,
			},
		}, nil
	case transactWriteOperationUpdate:
		request := transactItem.Update
		return &dynamodb.TransactWriteItem{
			Update: &dynamodb.Update{
				TableName:                 tableName,
				ExpressionAttributeNames:  request.ExpressionAttributesNames,
				ExpressionAttributeValues: request.ExpressionAttributeValues,
				Key:                       request.Key,
				UpdateExpression:          request.UpdateExpression,
			},
		}, nil
	default:
		return nil, xerrors.Errorf("invalid operation=%v", transactItem)
	}
}

func (d *ddbTableImpl) GetItem(ctx context.Context, request *GetItemRequest) (interface{}, error) {
	dynamodbKey, err := dynamodbattribute.MarshalMap(request.KeyMap)
	if err != nil {
		return nil, xerrors.Errorf("could not marshal given key (%v): %w", request.KeyMap, err)
	}
	input := &dynamodb.GetItemInput{
		Key:            dynamodbKey,
		TableName:      aws.String(d.table.TableName),
		ConsistentRead: aws.Bool(true),
	}
	output, err := d.table.DBAPI.GetItemWithContext(ctx, input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == awsrequest.CanceledErrorCode {
			return nil, internal.ErrRequestCanceled
		}
		return nil, xerrors.Errorf("failed to get item for key (%v): %w", request.KeyMap, err)
	}
	if output.Item == nil {
		return nil, internal.ErrItemNotFound
	}
	outputItem := reflect.New(request.EntryType).Interface()
	err = dynamodbattribute.UnmarshalMap(output.Item, outputItem)
	if err != nil {
		return nil, xerrors.Errorf("failed to unmarshal item (%v): %w", output.Item, err)
	}
	return outputItem, nil
}

func (d *ddbTableImpl) GetItems(ctx context.Context, request *GetItemsRequest) ([]interface{}, error) {
	// Limit parallel writes to reduce the chance of getting throttled.
	g, ctx := syncgroup.New(ctx, syncgroup.WithThrottling(maxGetWorkers))
	keyMaps := request.KeyMaps
	outputItems := make([]interface{}, len(keyMaps))

	for i := 0; i < len(keyMaps); i += maxTransactGetItemsSize {
		begin, end := i, i+maxTransactGetItemsSize
		if end > len(keyMaps) {
			end = len(keyMaps)
		}

		g.Go(func() error {
			err := d.TransactGetItems(
				ctx,
				request.EntryType,
				keyMaps[begin:end],
				outputItems[begin:end],
			)
			if err != nil {
				return xerrors.Errorf("failed to transact get items: %w", err)
			}
			return nil
		})
	}
	return outputItems, g.Wait()
}

func (d *ddbTableImpl) TransactGetItems(ctx context.Context, entryType reflect.Type, keys []*StringMap, outputItems []interface{}) error {
	if len(keys) != len(outputItems) {
		return xerrors.New("inputKeys does not have the same size as outputItems")
	}
	if len(keys) == 0 {
		return nil
	}
	inputItems := make([]*dynamodb.TransactGetItem, len(keys))
	for i, keyMap := range keys {
		dynamodbKey, err := dynamodbattribute.MarshalMap(keyMap)
		if err != nil {
			return xerrors.Errorf("could not marshal given key(%v):%w", keyMap, err)
		}
		inputItems[i] = &dynamodb.TransactGetItem{
			Get: &dynamodb.Get{
				Key:       dynamodbKey,
				TableName: aws.String(d.table.TableName),
			},
		}
	}

	if err := d.retry.Retry(ctx, func(ctx context.Context) error {
		output, err := d.table.DBAPI.TransactGetItemsWithContext(ctx, &dynamodb.TransactGetItemsInput{
			TransactItems: inputItems,
		})

		if err != nil {
			if transactionCanceledException, ok := err.(*dynamodb.TransactionCanceledException); ok {
				reasons := transactionCanceledException.CancellationReasons
				for _, reason := range reasons {
					if reason.Code != nil && *reason.Code == ddbCodeTransactionConflict {
						return retry.Retryable(
							xerrors.Errorf("failed to TransactGetItems because of transaction conflict, reason=(%v)", reason))
					}
				}
			}

			if aerr, ok := err.(awserr.Error); ok && aerr.Code() == awsrequest.CanceledErrorCode {
				return internal.ErrRequestCanceled
			}
			return err
		}

		// verify requested items are retrieved
		// if missing then corresponding ItemResponse at same index will be empty
		for index, inputItem := range inputItems {
			if output.Responses[index] == nil || len(output.Responses[index].Item) == 0 {
				return xerrors.Errorf("missing item key=%v: %w", inputItem.Get.Key, internal.ErrItemNotFound)
			}
			item := reflect.New(entryType).Interface()
			err = dynamodbattribute.UnmarshalMap(output.Responses[index].Item, item)
			if err != nil {
				return xerrors.Errorf("failed to unmarshal item (%v, %v): %w", output.Responses[index].Item, outputItems[index], err)
			}
			outputItems[index] = item
		}

		return nil
	}); err != nil {
		return err
	}

	return nil
}

func (d *ddbTableImpl) UpdateItem(
	ctx context.Context,
	request *UpdateItemRequest,
) (interface{}, error) {
	input := &dynamodb.UpdateItemInput{
		TableName:                 aws.String(d.table.TableName),
		ExpressionAttributeValues: request.ExpressionAttributeValues,
		ExpressionAttributeNames:  request.ExpressionAttributesNames,
		Key:                       request.Key,
		UpdateExpression:          request.UpdateExpression,
		ReturnValues:              aws.String(dynamodb.ReturnValueAllNew),
	}

	output, err := d.table.DBAPI.UpdateItemWithContext(ctx, input)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == awsrequest.CanceledErrorCode {
			return nil, internal.ErrRequestCanceled
		}
		return nil, xerrors.Errorf("failed to update item for key (%v): %w", input.Key, err)
	}

	if len(output.Attributes) == 0 {
		return nil, xerrors.Errorf("unexpected empty output returned from UpdateItem")
	}

	outputItem := reflect.New(request.EntryType).Interface()
	err = dynamodbattribute.UnmarshalMap(output.Attributes, outputItem)
	if err != nil {
		return nil, xerrors.Errorf("failed to unmarshal item (%v): %w", output.Attributes, err)
	}

	return outputItem, nil
}

func (d *ddbTableImpl) QueryItems(
	ctx context.Context,
	request *QueryItemsRequest,
) (*QueryItemsResult, error) {
	queryInput := &dynamodb.QueryInput{
		ExclusiveStartKey:         request.ExclusiveStartKey,
		KeyConditionExpression:    request.KeyConditionExpression,
		FilterExpression:          request.FilterExpression,
		ExpressionAttributeNames:  request.ExpressionAttributeNames,
		ExpressionAttributeValues: request.ExpressionAttributeValues,
		Select:                    aws.String(dynamodb.SelectAllAttributes),
		ScanIndexForward:          aws.Bool(request.ScanIndexForward),
		TableName:                 aws.String(d.table.TableName),
		ConsistentRead:            aws.Bool(request.ConsistentRead),
	}

	if request.ProjectionExpression != nil {
		// if ProjectionExpression is present, select specific attributes only.
		// otherwise, an error will be returned by QueryItems
		queryInput.ProjectionExpression = request.ProjectionExpression
		queryInput.Select = aws.String(dynamodb.SelectSpecificAttributes)
	}

	if request.Limit != 0 {
		queryInput.Limit = aws.Int64(request.Limit)
	}

	if request.IndexName != "" {
		queryInput.IndexName = aws.String(request.IndexName)
	}

	output, err := d.table.DBAPI.QueryWithContext(ctx, queryInput)
	if err != nil {
		if aerr, ok := err.(awserr.Error); ok && aerr.Code() == awsrequest.CanceledErrorCode {
			return nil, internal.ErrRequestCanceled
		}
		return nil, xerrors.Errorf("failed to get query items (keyConditionExpression=%v): %w", request.KeyConditionExpression, err)
	}

	outputItems := make([]interface{}, len(output.Items))
	for i, item := range output.Items {
		outputItem := reflect.New(request.EntryType).Interface()
		err = dynamodbattribute.UnmarshalMap(item, outputItem)
		if err != nil {
			return nil, xerrors.Errorf("failed to unmarshal item (%v): %w", output.Items[0], err)
		}

		outputItems[i] = outputItem
	}

	if len(outputItems) == 0 {
		return nil, internal.ErrItemNotFound
	}

	return &QueryItemsResult{
		Items:            outputItems,
		LastEvaluatedKey: output.LastEvaluatedKey,
	}, nil
}

func (d *ddbTableImpl) getGetRequestKeys(
	keyMaps []*StringMap,
) ([]map[string]*dynamodb.AttributeValue, error) {
	keys := make([]map[string]*dynamodb.AttributeValue, len(keyMaps))
	for i, keyMap := range keyMaps {
		key, err := dynamodbattribute.MarshalMap(keyMap)
		if err != nil {
			return nil, xerrors.Errorf("failed to marshal keyMap: %w", err)
		}
		keys[i] = key
	}
	return keys, nil
}
