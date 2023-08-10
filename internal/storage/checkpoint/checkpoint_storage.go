package checkpoint

import (
	"context"
	"crypto/rand"
	"math/big"
	"reflect"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodb/expression"
	"github.com/uber-go/tally/v4"
	"go.uber.org/fx"
	"go.uber.org/zap"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainnode/internal/api"
	"github.com/coinbase/chainnode/internal/config"
	"github.com/coinbase/chainnode/internal/storage/checkpoint/models"
	cstorage "github.com/coinbase/chainnode/internal/storage/collection"
	"github.com/coinbase/chainnode/internal/utils/fxparams"
	"github.com/coinbase/chainnode/internal/utils/instrument"
	"github.com/coinbase/chainnode/internal/utils/log"
)

type (
	CheckpointStorageParams struct {
		fx.In
		fxparams.Params

		cstorage.CollectionStorage `name:"collection"`
	}

	CheckpointStorage interface {
		PersistCheckpoint(ctx context.Context, checkpoint *api.Checkpoint) error
		GetCheckpoint(ctx context.Context, collection api.Collection, tag uint32) (*api.Checkpoint, error)
		GetCheckpoints(ctx context.Context, collections []api.Collection, tag uint32) ([]*api.Checkpoint, error)
	}

	checkpointStorageImpl struct {
		collectionStorage cstorage.CollectionStorage

		config  *config.Config
		logger  *zap.Logger
		metrics *checkpointStorageMetrics
	}

	checkpointStorageMetrics struct {
		checkpointSequenceFromPrimaryGauge   tally.Gauge
		checkpointSequenceFromShardGauge     tally.Gauge
		checkpointSequenceDeltaGauge         tally.Gauge
		instrumentGetCheckpointFromPrimary   instrument.Call
		instrumentGetCheckpointFromShard     instrument.Call
		instrumentPersistCheckpointToPrimary instrument.Call
		instrumentPersistCheckpointToShard   instrument.Call
	}

	prepareUpdateItemResult struct {
		expression *expression.Expression
		key        map[string]*dynamodb.AttributeValue
	}
)

const (
	checkpointSequenceFromPrimaryMetric = "checkpoint_sequence_from_primary"
	checkpointSequenceFromShardMetric   = "checkpoint_sequence_from_shard"
	checkpointSequenceDeltaMetric       = "checkpoint_sequence_primary_and_shard_delta"
	getCheckpointFromPrimaryName        = "get_checkpoint_from_primary"
	getCheckpointFromShardName          = "get_checkpoint_from_shard"
	persistCheckpointToPrimaryName      = "persist_checkpoint_to_primary"
	persistCheckpointToShardName        = "persist_checkpoint_to_shard"
)

var _ CheckpointStorage = (*checkpointStorageImpl)(nil)

func NewCheckpointStorage(params CheckpointStorageParams) (CheckpointStorage, error) {
	return &checkpointStorageImpl{
		collectionStorage: params.CollectionStorage.WithCollection(api.CollectionCheckpoints),
		config:            params.Config,
		logger:            log.WithPackage(params.Logger),
		metrics:           newCheckpointStorageMetrics(params.Metrics),
	}, nil
}

func newCheckpointStorageMetrics(scope tally.Scope) *checkpointStorageMetrics {
	scope = scope.SubScope("checkpoint_storage")
	return &checkpointStorageMetrics{
		checkpointSequenceFromPrimaryGauge:   scope.Gauge(checkpointSequenceFromPrimaryMetric),
		checkpointSequenceFromShardGauge:     scope.Gauge(checkpointSequenceFromShardMetric),
		checkpointSequenceDeltaGauge:         scope.Gauge(checkpointSequenceDeltaMetric),
		instrumentGetCheckpointFromPrimary:   instrument.NewCall(scope, getCheckpointFromPrimaryName),
		instrumentGetCheckpointFromShard:     instrument.NewCall(scope, getCheckpointFromShardName),
		instrumentPersistCheckpointToPrimary: instrument.NewCall(scope, persistCheckpointToPrimaryName),
		instrumentPersistCheckpointToShard:   instrument.NewCall(scope, persistCheckpointToShardName),
	}
}

func (c *checkpointStorageImpl) PersistCheckpoint(
	ctx context.Context,
	checkpoint *api.Checkpoint,
) error {
	return c.persistCheckpoint(ctx, checkpoint)
}

func (c *checkpointStorageImpl) persistCheckpoint(
	ctx context.Context,
	checkpoint *api.Checkpoint,
) error {
	// process collection checkpoints (not global ones)
	if !checkpoint.IsGlobal() {
		err := c.persistCheckpointToPrimary(ctx, checkpoint)
		if err != nil {
			return xerrors.Errorf("failed to persistCheckpointToPrimary: %w", err)
		}
	} else {
		// persist global checkpoints in shards
		err := c.persistCheckpointToShards(ctx, checkpoint)
		if err != nil {
			return xerrors.Errorf("failed to persistCheckpointToShards: %w", err)
		}
	}

	return nil
}

func (c *checkpointStorageImpl) persistCheckpointToPrimary(
	ctx context.Context,
	checkpoint *api.Checkpoint,
) error {
	return c.metrics.instrumentPersistCheckpointToPrimary.Instrument(ctx, func(ctx context.Context) error {
		result, err := c.preparePrimaryUpdateItem(checkpoint)
		if err != nil {
			return xerrors.Errorf("failed to prepare primary update expression: %w", err)
		}

		expr := result.expression
		_, err = c.collectionStorage.UpdateItem(ctx, &cstorage.UpdateItemRequest{
			ExpressionAttributesNames: expr.Names(),
			ExpressionAttributeValues: expr.Values(),
			Key:                       result.key,
			UpdateExpression:          expr.Update(),
			EntryType:                 reflect.TypeOf(models.CheckpointDDBEntry{}),
		})
		if err != nil {
			return xerrors.Errorf("failed to update item: %w", err)
		}

		return nil
	})
}

func (c *checkpointStorageImpl) persistCheckpointToShards(
	ctx context.Context,
	checkpoint *api.Checkpoint,
) error {
	if !checkpoint.IsGlobal() {
		return xerrors.Errorf("does not allow non-global checkpoint=%v to write to shard", checkpoint)
	}

	return c.metrics.instrumentPersistCheckpointToShard.Instrument(ctx, func(ctx context.Context) error {
		prepareResults, err := c.prepareShardedUpdateItem(checkpoint)
		if err != nil {
			return xerrors.Errorf("failed to prepare sharded update item: %w", err)
		}

		transactItems := make([]*cstorage.TransactItem, len(prepareResults))
		for i, result := range prepareResults {
			expr := result.expression
			transactItems[i] = &cstorage.TransactItem{
				Update: &cstorage.TransactUpdateItem{
					ExpressionAttributesNames: expr.Names(),
					ExpressionAttributeValues: expr.Values(),
					Key:                       result.key,
					UpdateExpression:          expr.Update(),
				},
			}
		}

		err = c.collectionStorage.TransactWriteItems(ctx, &cstorage.TransactWriteItemsRequest{
			TransactItems: transactItems,
		})
		if err != nil {
			return xerrors.Errorf("failed to transact write item: %w", err)
		}

		return nil
	})
}

func (c *checkpointStorageImpl) preparePrimaryUpdateItem(checkpoint *api.Checkpoint) (*prepareUpdateItemResult, error) {
	result, err := c.prepareUpdateItem(checkpoint, models.PartitionKeyIndexWithoutSharding)
	if err != nil {
		return nil, xerrors.Errorf("failed to prepare update expression: %w", err)
	}

	return result, nil
}

func (c *checkpointStorageImpl) prepareShardedUpdateItem(checkpoint *api.Checkpoint) ([]*prepareUpdateItemResult, error) {
	numOfShards := c.config.Storage.GlobalCheckpoint.NumberOfShards
	result := make([]*prepareUpdateItemResult, numOfShards)
	var err error
	for i := 0; i < numOfShards; i++ {
		partitionKeyIndex := i + models.PartitionKeyStartIndexWithSharding
		result[i], err = c.prepareUpdateItem(checkpoint, partitionKeyIndex)
		if err != nil {
			return nil, xerrors.Errorf(
				"failed to prepare update item for checkpoint=%+v, partitionKeyIndex=%v: %w",
				checkpoint,
				partitionKeyIndex,
				err,
			)
		}
	}
	return result, nil
}

func (c *checkpointStorageImpl) prepareUpdateItem(
	checkpoint *api.Checkpoint, partitionKeyIndex int,
) (*prepareUpdateItemResult, error) {
	// TODO: make updatedAt identical for all the shards
	entry, err := models.MakeCheckpointDDBEntry(checkpoint, partitionKeyIndex)
	if err != nil {
		return nil, xerrors.Errorf("failed to make checkpoint ddb entry: %w", err)
	}

	if entry.Tag != checkpoint.Tag || entry.Sequence != checkpoint.Sequence || entry.Height != checkpoint.Height {
		return nil, xerrors.Errorf("inconsistent entry values (%+v)", entry)
	}

	// set :tag = tag, :sequence = sequence, :height = height, :blockTime = blockTime, :updatedAt = updatedAt
	updateExpression := expression.UpdateBuilder{}.
		Set(expression.Name(models.CheckpointTagName), expression.Value(entry.Tag)).
		Set(expression.Name(models.CheckpointSequenceName), expression.Value(entry.Sequence)).
		Set(expression.Name(models.CheckpointHeightName), expression.Value(entry.Height)).
		Set(expression.Name(cstorage.UpdatedAtName), expression.Value(entry.UpdatedAt)).
		Set(expression.Name(models.CheckpointLastBlockTimeName), expression.Value(entry.LastBlockTime))
	expr, err := expression.NewBuilder().WithUpdate(updateExpression).Build()
	if err != nil {
		return nil, xerrors.Errorf("failed to build expression for UpdateItem: %w", err)
	}

	keyMap, err := models.GetCheckpointKeyMap(checkpoint.Tag, checkpoint.Collection, partitionKeyIndex)
	if err != nil {
		return nil, xerrors.Errorf("failed to get checkpoint key map: %w", err)
	}

	key, err := dynamodbattribute.MarshalMap(keyMap)
	if err != nil {
		return nil, xerrors.Errorf("could not marshal given key (%v): %w", key, err)
	}

	return &prepareUpdateItemResult{
		expression: &expr,
		key:        key,
	}, nil
}

func (c *checkpointStorageImpl) GetCheckpoint(
	ctx context.Context,
	collection api.Collection,
	tag uint32,
) (*api.Checkpoint, error) {
	return c.getCheckpoint(ctx, collection, tag)
}

func (c *checkpointStorageImpl) getCheckpoint(
	ctx context.Context,
	collection api.Collection,
	tag uint32,
) (*api.Checkpoint, error) {
	var checkpoint *api.Checkpoint
	var err error
	// read collection checkpoint (not global ones) from primary
	if !collection.IsGlobal() {
		checkpoint, err = c.getCheckpointFromPrimary(ctx, collection, tag)
		if err != nil {
			return nil, xerrors.Errorf("failed to get checkpoint from primary: %w", err)
		}
	} else {
		// process global checkpoint. Read from shard
		checkpoint, err = c.getCheckpointFromShard(ctx, collection, tag)
		if err != nil {
			return nil, xerrors.Errorf("failed to get checkpoint from shard: %w", err)
		}
	}
	return checkpoint, nil
}

func (c *checkpointStorageImpl) getCheckpointFromPrimary(
	ctx context.Context,
	collection api.Collection,
	tag uint32,
) (*api.Checkpoint, error) {
	var checkpoint *api.Checkpoint
	if err := c.metrics.instrumentGetCheckpointFromPrimary.Instrument(ctx, func(ctx context.Context) error {
		keyMap, err := models.GetCheckpointKeyMap(tag, collection, models.PartitionKeyIndexWithoutSharding)
		if err != nil {
			return xerrors.Errorf("failed to get checkpoint key map: %w", err)
		}

		checkpoint, err = c.getCheckpointFromStorage(ctx, keyMap)
		if err != nil {
			return xerrors.Errorf("failed to get checkpoint from collection storage: %w", err)
		}

		return nil
	}); err != nil {
		return nil, err
	}

	if checkpoint == nil {
		return nil, xerrors.New("got unexpected nil checkpoint")
	}

	c.metrics.checkpointSequenceFromPrimaryGauge.Update(float64(checkpoint.Sequence))
	return checkpoint, nil
}

func (c *checkpointStorageImpl) getCheckpointFromShard(
	ctx context.Context,
	collection api.Collection,
	tag uint32,
) (*api.Checkpoint, error) {
	if !collection.IsGlobal() {
		return nil, xerrors.Errorf("does not allow non-global checkpoint=%s to read from shard", collection)
	}

	var checkpoint *api.Checkpoint
	if err := c.metrics.instrumentGetCheckpointFromShard.Instrument(ctx, func(ctx context.Context) error {
		numOfShards := c.config.Storage.GlobalCheckpoint.NumberOfShards
		partitionIndex, err := rand.Int(rand.Reader, big.NewInt(int64(numOfShards)))
		if err != nil {
			return xerrors.Errorf("failed to generate random partitionIndex: %w", err)
		}

		keyMap, err := models.GetCheckpointKeyMap(
			tag,
			collection,
			models.PartitionKeyStartIndexWithSharding+int(partitionIndex.Uint64()),
		)
		if err != nil {
			return xerrors.Errorf("failed to get checkpoint key map (partitionIndex=%v): %w", partitionIndex, err)
		}

		checkpoint, err = c.getCheckpointFromStorage(ctx, keyMap)
		if err != nil {
			return xerrors.Errorf("failed to get checkpoint from collection storage (partitionIndex=%v): %w", partitionIndex, err)
		}

		return nil
	}); err != nil {
		return nil, err
	}

	if checkpoint == nil {
		return nil, xerrors.New("got unexpected nil checkpoint")
	}

	c.metrics.checkpointSequenceFromShardGauge.Update(float64(checkpoint.Sequence))
	return checkpoint, nil
}

func (c *checkpointStorageImpl) getCheckpointFromStorage(ctx context.Context, keyMap cstorage.StringMap) (*api.Checkpoint, error) {
	outputItem, err := c.collectionStorage.GetItem(ctx, &cstorage.GetItemRequest{
		KeyMap:    keyMap,
		EntryType: reflect.TypeOf(models.CheckpointDDBEntry{}),
	})
	if err != nil {
		return nil, xerrors.Errorf("failed to get checkpoint: %w", err)
	}

	checkpointDDBEntry, ok := outputItem.(*models.CheckpointDDBEntry)
	if !ok {
		return nil, xerrors.Errorf("failed to convert output (%+v) to CheckpointDDBEntry", outputItem)
	}

	var checkpoint api.Checkpoint
	err = checkpointDDBEntry.AsAPI(&checkpoint)
	if err != nil {
		return nil, xerrors.Errorf("failed to convert from CheckpointDDBEntry: %w", err)
	}

	return &checkpoint, nil
}

// GetCheckpoints TODO: prohibit global checkpoints, rename to GetCollectionCheckpoints
func (c *checkpointStorageImpl) GetCheckpoints(
	ctx context.Context,
	collections []api.Collection,
	tag uint32,
) ([]*api.Checkpoint, error) {
	return c.getCheckpoints(ctx, collections, tag)
}

func (c *checkpointStorageImpl) getCheckpoints(
	ctx context.Context,
	collections []api.Collection,
	tag uint32,
) ([]*api.Checkpoint, error) {
	keyMaps := make([]*cstorage.StringMap, len(collections))
	for i, collection := range collections {
		keyMap, err := models.GetCheckpointKeyMap(tag, collection, models.PartitionKeyIndexWithoutSharding)
		if err != nil {
			return nil, xerrors.Errorf("failed to get checkpoint(%s) key map: %w", collection.String(), err)
		}
		keyMaps[i] = &keyMap
	}

	outputItems, err := c.collectionStorage.GetItems(ctx, &cstorage.GetItemsRequest{
		KeyMaps:   keyMaps,
		EntryType: reflect.TypeOf(models.CheckpointDDBEntry{}),
	})
	if err != nil {
		return nil, xerrors.Errorf("failed to get checkpoints: %w", err)
	}

	if len(outputItems) != len(collections) {
		return nil, xerrors.Errorf(
			"unexpected number of checkpoints returned by storage (actual=%v, expected=%v)",
			len(outputItems),
			len(collections),
		)
	}

	checkpoints := make([]*api.Checkpoint, len(collections))
	for i, outputItem := range outputItems {
		checkpointDDBEntry, ok := outputItem.(*models.CheckpointDDBEntry)
		if !ok {
			return nil, xerrors.Errorf("failed to convert output (%+v) to CheckpointDDBEntry", outputItem)
		}

		var checkpoint api.Checkpoint
		err = checkpointDDBEntry.AsAPI(&checkpoint)
		if err != nil {
			return nil, xerrors.Errorf("failed to convert from CheckpointDDBEntry: %w", err)
		}

		checkpoints[i] = &checkpoint
	}

	return checkpoints, nil
}
