package ethereum

import (
	"context"

	"golang.org/x/xerrors"

	"github.com/coinbase/chainnode/internal/api"
	"github.com/coinbase/chainnode/internal/api/ethereum"
	"github.com/coinbase/chainnode/internal/storage/collection"
	"github.com/coinbase/chainnode/internal/storage/ethereum/models"
	"github.com/coinbase/chainnode/internal/storage/internal"
)

type (
	BlockStorage interface {
		PersistBlock(ctx context.Context, block *ethereum.Block) error
		// GetBlock gets the most recent block that has a block sequence that is no later than the specified maxSequence
		GetBlock(ctx context.Context, tag uint32, height uint64, maxSequence api.Sequence) (*ethereum.Block, error)
		// GetBlockMetadata gets the metadata of the most recent block that has a block sequence that is no later than the specified maxSequence
		GetBlockMetadata(ctx context.Context, tag uint32, height uint64, maxSequence api.Sequence) (*ethereum.BlockMetadata, error)
	}

	blockStorageImpl struct {
		collectionStorage collection.CollectionStorage `name:"collection"`
	}
)

var _ BlockStorage = (*blockStorageImpl)(nil)

func newBlockStorage(params Params) (BlockStorage, error) {
	return &blockStorageImpl{
		collectionStorage: params.CollectionStorage.WithCollection(ethereum.CollectionBlocks),
	}, nil
}

func (s *blockStorageImpl) PersistBlock(
	ctx context.Context,
	block *ethereum.Block,
) error {
	return s.persistBlock(ctx, block)
}

func (s *blockStorageImpl) persistBlock(
	ctx context.Context,
	block *ethereum.Block,
) error {
	if block == nil {
		return xerrors.Errorf("block cannot be nil")
	}

	tag := block.Tag
	hash := block.Hash
	height := block.Height

	entry, err := models.MakeBlockDDBEntry(block)
	if err != nil {
		return xerrors.Errorf("failed to make block ddb entry: %w", err)
	}

	if entry.Tag != tag || entry.Height != height || entry.Hash != hash {
		return xerrors.Errorf("inconsistent entry values (%+v)", entry)
	}

	err = s.collectionStorage.UploadToBlobStorage(ctx, entry, false)
	if err != nil {
		return xerrors.Errorf("failed to upload data: %w", err)
	}

	if err := s.collectionStorage.WriteItem(ctx, entry); err != nil {
		return xerrors.Errorf("failed to write block %w", err)
	}

	return nil
}

func (s *blockStorageImpl) GetBlock(ctx context.Context, tag uint32, height uint64, maxSequence api.Sequence) (*ethereum.Block, error) {
	return s.getBlock(ctx, tag, height, maxSequence)
}

func (s *blockStorageImpl) getBlock(ctx context.Context, tag uint32, height uint64, maxSequence api.Sequence) (*ethereum.Block, error) {
	// get the most recent block entry that has sequence <= specified sequence
	queryResult, err := s.queryBlockByMaxSequence(ctx, tag, height, maxSequence, nil)
	if err != nil {
		return nil, xerrors.Errorf("failed to query block: %w", err)
	}

	outputItem := queryResult.Items[0]
	blockDDBEntry, ok := outputItem.(*models.BlockDDBEntry)
	if !ok {
		return nil, xerrors.Errorf("failed to convert output=%v to BlockDDBEntry", outputItem)
	}

	if blockDDBEntry.ObjectKey != "" {
		err := s.collectionStorage.DownloadFromBlobStorage(ctx, blockDDBEntry)
		if err != nil {
			return nil, xerrors.Errorf("failed to download data based on objectKey: %w", err)
		}
	}

	var block ethereum.Block
	err = blockDDBEntry.AsAPI(&block)
	if err != nil {
		return nil, xerrors.Errorf("failed to convert from BlockDDBEntry: %w", err)
	}

	return &block, nil
}

func (s *blockStorageImpl) GetBlockMetadata(
	ctx context.Context,
	tag uint32,
	height uint64,
	maxSequence api.Sequence,
) (*ethereum.BlockMetadata, error) {
	return s.getBlockMetadata(ctx, tag, height, maxSequence)
}

func (s *blockStorageImpl) getBlockMetadata(
	ctx context.Context,
	tag uint32,
	height uint64,
	maxSequence api.Sequence,
) (*ethereum.BlockMetadata, error) {
	selectedAttributeNames := []string{
		collection.PartitionKeyName,
		collection.SortKeyName, // required to derive sequence
		collection.UpdatedAtName,
		models.BlockTagName,
		models.BlockHashName,
		models.BlockHeightName,
		models.BlockTimeName,
	}
	// get the most recent block entry that has sequence <= specified sequence
	queryResult, err := s.queryBlockByMaxSequence(ctx, tag, height, maxSequence, selectedAttributeNames)
	if err != nil {
		return nil, xerrors.Errorf("failed to query block: %w", err)
	}

	if len(queryResult.Items) == 0 {
		return nil, xerrors.Errorf("got empty result: %w", internal.ErrItemNotFound)
	}

	outputItem := queryResult.Items[0]
	blockDDBEntry, ok := outputItem.(*models.BlockDDBEntry)
	if !ok {
		return nil, xerrors.Errorf("failed to convert output=%v to BlockDDBEntry", outputItem)
	}

	block, err := blockDDBEntry.ToBlockMetadata()
	if err != nil {
		return nil, xerrors.Errorf("failed to convert from BlockDDBEntry: %w", err)
	}
	return block, nil
}

// queryBlockByMaxSequence queries the most recent block entry that has sequence <= maxSequence
// if selectedAttributeNames is empty, it will return all the attributes
func (s *blockStorageImpl) queryBlockByMaxSequence(
	ctx context.Context,
	tag uint32,
	height uint64,
	maxSequence api.Sequence,
	selectedAttributeNames []string,
) (*collection.QueryItemsResult, error) {
	// :pk = pk AND :sk <= maxSequence
	return s.collectionStorage.QueryItemByMaxSortKey(
		ctx,
		models.MakeBlockPartitionKey(tag, height),
		models.MakeBlockSortKey(maxSequence),
		models.BlockDDBEntry{},
		selectedAttributeNames,
	)
}
