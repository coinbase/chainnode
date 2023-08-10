package ethereum

import (
	"context"
	"reflect"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainnode/internal/api"
	"github.com/coinbase/chainnode/internal/api/ethereum"
	"github.com/coinbase/chainnode/internal/storage/collection"
	"github.com/coinbase/chainnode/internal/storage/ethereum/models"
	"github.com/coinbase/chainnode/internal/storage/internal"
	"github.com/coinbase/chainnode/internal/utils/syncgroup"
)

type (
	LogStorageV2 interface {
		// PersistLogsV2 persists logs-v2 from one block to logs storage
		PersistLogsV2(ctx context.Context, logs *ethereum.Logs) error

		// GetLogsLiteByBlockRange fetches LogsLite for a range [beginHeight, endHeight)
		// LogsLite only contains partial logs data
		GetLogsLiteByBlockRange(ctx context.Context, tag uint32,
			beginHeight uint64, endHeight uint64, maxSequence api.Sequence) ([]*ethereum.LogsLite, error)

		// GetLogsV2 gets logs from one block.
		// maxSequence is used to filter the most recent block that has block sequence <= maxSequence
		GetLogsV2(ctx context.Context, tag uint32, height uint64, maxSequence api.Sequence) (*ethereum.Logs, error)
	}

	logStorageV2Impl struct {
		storage                   collection.CollectionStorage `name:"logs"`
		logsByBlockRangeIndexName string
		partitionSize             uint64
	}
)

const (
	getLogsParallelism = 10
)

var (
	_ LogStorageV2 = (*logStorageV2Impl)(nil)
)

func newLogStorageV2(params Params) (LogStorageV2, error) {
	return &logStorageV2Impl{
		storage:                   params.LogsStorage.WithCollection(ethereum.CollectionLogsV2),
		logsByBlockRangeIndexName: params.Config.AWS.DynamoDB.LogsByBlockRangeIndexName,
		partitionSize:             params.Config.AWS.DynamoDB.LogsTablePartitionSize,
	}, nil
}

func (s *logStorageV2Impl) PersistLogsV2(
	ctx context.Context,
	logs *ethereum.Logs,
) error {
	return s.persistLogsV2(ctx, logs)
}

func (s *logStorageV2Impl) persistLogsV2(
	ctx context.Context,
	logs *ethereum.Logs,
) error {
	if logs == nil {
		return xerrors.Errorf("logs cannot be empty")
	}

	entry, err := models.MakeLogsV2DDBEntry(logs, s.partitionSize)
	if err != nil {
		return xerrors.Errorf("failed to make log ddb entry: %w", err)
	}

	if entry.Tag != logs.Tag || entry.LogsBloom != logs.LogsBloom ||
		entry.Height != logs.Height || entry.Hash != logs.Hash {
		return xerrors.Errorf("inconsistent entry values (%+v)", entry)
	}

	err = s.storage.UploadToBlobStorage(ctx, entry, false)
	if err != nil {
		return xerrors.Errorf("failed to upload data: %w", err)
	}

	if err := s.storage.WriteItem(ctx, entry); err != nil {
		return xerrors.Errorf("failed to write logs %w", err)
	}
	return nil
}

func (s *logStorageV2Impl) GetLogsV2(
	ctx context.Context,
	tag uint32,
	height uint64,
	maxSequence api.Sequence,
) (*ethereum.Logs, error) {
	return s.getLogsV2(ctx, tag, height, maxSequence)
}

func (s *logStorageV2Impl) getLogsV2(
	ctx context.Context,
	tag uint32,
	height uint64,
	maxSequence api.Sequence,
) (*ethereum.Logs, error) {
	// :pk = pk AND :sk = maxSequence
	queryResult, err := s.storage.QueryItemByMaxSortKey(
		ctx,
		models.MakeLogsV2PartitionKey(tag, height),
		models.MakeLogsV2SortKey(maxSequence),
		models.LogsV2DDBEntry{},
		nil,
	)
	if err != nil {
		return nil, xerrors.Errorf("failed to query logs: %w", err)
	}

	if len(queryResult.Items) == 0 {
		return nil, xerrors.Errorf("got empty result: %w", internal.ErrItemNotFound)
	}

	outputItem := queryResult.Items[0]
	entry, ok := queryResult.Items[0].(*models.LogsV2DDBEntry)
	if !ok {
		return nil, xerrors.Errorf("failed to convert output=%v to LogsV2DDBEntry", outputItem)
	}

	if entry.ObjectKey != "" {
		err = s.storage.DownloadFromBlobStorage(ctx, entry)
		if err != nil {
			return nil, xerrors.Errorf("failed to download data based on objectKey: %w", err)
		}
	}

	var logs ethereum.Logs
	err = entry.AsAPI(&logs)
	if err != nil {
		return nil, xerrors.Errorf("failed to convert LogsV2DDBEntry to Logs: %w", err)
	}

	return &logs, nil
}

func (s *logStorageV2Impl) GetLogsLiteByBlockRange(
	ctx context.Context,
	tag uint32,
	beginHeight uint64,
	endHeight uint64,
	maxSequence api.Sequence,
) ([]*ethereum.LogsLite, error) {
	return s.getLogsLiteByBlockRange(ctx, tag, beginHeight, endHeight, maxSequence)
}

func (s *logStorageV2Impl) getLogsLiteByBlockRange(
	ctx context.Context,
	tag uint32,
	beginHeight uint64,
	endHeight uint64,
	maxSequence api.Sequence,
) ([]*ethereum.LogsLite, error) {
	if beginHeight >= endHeight {
		return nil, xerrors.Errorf("invalid range [beginHeight=%d, endHeight=%d): %w", beginHeight, endHeight, api.ErrNotAllowed)
	}

	// TODO: follow up: send to channel later to parallelize the processing
	partitionSize := s.partitionSize
	beginPartitionIndex := beginHeight / partitionSize
	endPartitionIndex := (endHeight - 1) / partitionSize

	group, ctx := syncgroup.New(ctx, syncgroup.WithThrottling(getLogsParallelism))

	result := make([]*ethereum.LogsLite, endHeight-beginHeight)
	for i := beginPartitionIndex; i <= endPartitionIndex; i++ {
		curIndex := i
		group.Go(func() error {
			// query block heights within [Math.max(curIndex*partitionSize, beginHeight), Math.min((curIndex+1)*partitionSize-1, endHeight-1)]
			pk := models.MakeLogsV2IndexPartitionKey(tag, curIndex*partitionSize, partitionSize)

			var skBegin, skEnd string
			if beginHeight > curIndex*partitionSize {
				skBegin = models.MakeLogsV2IndexSortKey(beginHeight)
			} else {
				skBegin = models.MakeLogsV2IndexSortKey(curIndex * partitionSize)
			}

			if endHeight < (curIndex+1)*partitionSize {
				skEnd = models.MakeLogsV2IndexSortKey(endHeight - 1)
			} else {
				skEnd = models.MakeLogsV2IndexSortKey((curIndex+1)*partitionSize - 1)
			}

			keyCondition := expression.Key(models.LogsByBlockRangePartitionKeyName).
				Equal(expression.Value(pk))
			keyCondition = keyCondition.And(expression.Key(models.LogsByBlockRangeSortKeyName).
				Between(expression.Value(skBegin), expression.Value(skEnd)))

			// include logsBloom, pk, sk in query result
			selectedAttributeNames := []string{
				collection.PartitionKeyName,
				collection.SortKeyName,
				models.LogsBloomName,
			}
			var projection expression.ProjectionBuilder
			for _, attr := range selectedAttributeNames {
				projection = expression.AddNames(projection, expression.Name(attr))
			}

			// sequence needs to be <= maxSequence
			filterCondition := expression.LessThanEqual(expression.Name(collection.SortKeyName), expression.Value(models.MakeLogsV2SortKey(maxSequence)))

			builder := expression.NewBuilder().WithKeyCondition(keyCondition).WithFilter(filterCondition).WithProjection(projection)

			expr, err := builder.Build()
			if err != nil {
				return xerrors.Errorf("failed to build expression for QueryItems: %w", err)
			}

			var items []interface{}
			var exclusiveStartKey map[string]*dynamodb.AttributeValue
			for {
				queryResult, err := s.storage.QueryItems(
					ctx, &collection.QueryItemsRequest{
						ExclusiveStartKey:         exclusiveStartKey,
						KeyConditionExpression:    expr.KeyCondition(),
						ExpressionAttributeNames:  expr.Names(),
						ExpressionAttributeValues: expr.Values(),
						ProjectionExpression:      expr.Projection(),
						FilterExpression:          expr.Filter(),
						ScanIndexForward:          false,
						EntryType:                 reflect.TypeOf(models.LogsLiteDDBEntry{}),
						IndexName:                 s.logsByBlockRangeIndexName,
						ConsistentRead:            false, // Only eventual consistency is available for querying GSI
					},
				)

				if err != nil {
					return xerrors.Errorf("failed to query logs: %w", err)
				}

				if len(queryResult.Items) == 0 {
					// TODO: follow up to see if we should allow this case. For some oldest blocks, logs is empty
					return xerrors.Errorf("got empty result: %w", internal.ErrItemNotFound)
				}

				items = append(items, queryResult.Items...)

				exclusiveStartKey = queryResult.LastEvaluatedKey
				if len(exclusiveStartKey) == 0 {
					break
				}
			}

			for _, item := range items {
				entry, ok := item.(*models.LogsLiteDDBEntry)
				if !ok {
					return xerrors.Errorf("failed to convert output=%v to LogsLiteDDBEntry", item)
				}

				var logs ethereum.LogsLite
				err := entry.AsLogsLite(&logs)
				if err != nil {
					return xerrors.Errorf("failed to convert LogsLiteDDBEntry to LogsLite: %w", err)
				}

				height := logs.Height
				previousLogs := result[height-beginHeight]
				if previousLogs == nil || previousLogs.Sequence < logs.Sequence {
					// update logs for a given height if the current logs is more up-to-date
					result[height-beginHeight] = &logs
				}
			}

			return nil
		})

	}

	if err := group.Wait(); err != nil {
		return nil, xerrors.Errorf("error in goroutine: %w", err)
	}

	return result, nil
}
