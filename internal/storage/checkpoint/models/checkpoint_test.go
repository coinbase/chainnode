package models

import (
	"fmt"
	"testing"

	"golang.org/x/xerrors"

	"github.com/coinbase/chainnode/internal/api"
	"github.com/coinbase/chainnode/internal/utils/testutil"
)

func TestCheckpoint(t *testing.T) {
	require := testutil.Require(t)
	tag := uint32(1)
	height := uint64(123123)
	sequence := api.Sequence(555)
	collection := api.Collection("test-collection")
	require.Less(PartitionKeyIndexWithoutSharding, PartitionKeyStartIndexWithSharding)

	partitionKeyIndexes := []int{PartitionKeyIndexWithoutSharding, PartitionKeyStartIndexWithSharding, PartitionKeyStartIndexWithSharding + 2}
	for _, partitionKeyIndex := range partitionKeyIndexes {
		if partitionKeyIndex == 0 {
			require.Equal("1#checkpoints", MakeCheckpointPartitionKey(tag, partitionKeyIndex))
		} else {
			require.Equal(fmt.Sprintf("1#checkpoints#%v", partitionKeyIndex), MakeCheckpointPartitionKey(tag, partitionKeyIndex))
		}

		require.Equal("test-collection", MakeCheckpointSortKey(collection))

		checkpoint := &api.Checkpoint{
			Collection: collection,
			Tag:        tag,
			Sequence:   sequence,
			Height:     height,
		}
		entry, err := MakeCheckpointDDBEntry(checkpoint, partitionKeyIndex)
		require.NoError(err)
		require.NotNil(entry)
		testutil.MustTime(entry.UpdatedAt)

		objectKey, err := entry.MakeObjectKey()
		require.Error(err)
		require.True(xerrors.Is(err, api.ErrNotImplemented))
		require.Empty(objectKey)

		pk, err := entry.GetPartitionKey()
		require.NoError(err)
		require.Equal(MakeCheckpointPartitionKey(tag, partitionKeyIndex), pk)

		sk, err := entry.GetSortKey()
		require.NoError(err)
		require.Equal("test-collection", sk)

		actualData, err := entry.GetData()
		require.Error(err)
		require.True(xerrors.Is(err, api.ErrNotImplemented))
		require.Empty(actualData)

		var actualCheckpoint api.Checkpoint
		err = entry.AsAPI(&actualCheckpoint)
		require.NoError(err)
		require.False(actualCheckpoint.UpdatedAt.IsZero())
		checkpoint.UpdatedAt = actualCheckpoint.UpdatedAt
		require.Equal(checkpoint, &actualCheckpoint)

	}
}
