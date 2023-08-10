package ethereum

import (
	"context"

	"golang.org/x/xerrors"

	"github.com/coinbase/chainnode/internal/api"
	"github.com/coinbase/chainnode/internal/api/ethereum"
	"github.com/coinbase/chainnode/internal/storage/collection"
	"github.com/coinbase/chainnode/internal/storage/ethereum/models"
)

type (
	BlockByHashStorage interface {
		// PersistBlockByHash persists a block by hash, which includes full tx data
		PersistBlockByHash(ctx context.Context, block *ethereum.Block) error
		// GetBlockByHash gets the most recent block by hash that has a block sequence that is no later than the specified maxSequence
		GetBlockByHash(ctx context.Context, tag uint32, hash string, maxSequence api.Sequence) (*ethereum.Block, error)
	}

	blockByHashStorageImpl struct {
		collectionStorage collection.CollectionStorage `name:"collection"`
	}
)

var _ BlockByHashStorage = (*blockByHashStorageImpl)(nil)

func newBlockByHashStorage(params Params) (BlockByHashStorage, error) {
	return &blockByHashStorageImpl{
		collectionStorage: params.CollectionStorage.WithCollection(ethereum.CollectionBlocksByHash),
	}, nil
}

func (s *blockByHashStorageImpl) PersistBlockByHash(
	ctx context.Context,
	block *ethereum.Block,
) error {
	return s.persistBlockByHash(ctx, block)
}

func (s *blockByHashStorageImpl) persistBlockByHash(
	ctx context.Context,
	block *ethereum.Block,
) error {
	if block == nil {
		return xerrors.Errorf("block cannot be nil")
	}

	tag := block.Tag
	hash := block.Hash
	height := block.Height

	entry, err := models.MakeBlockByHashDDBEntry(block)
	if err != nil {
		return xerrors.Errorf("failed to make block by hash ddb entry: %w", err)
	}

	if entry.Tag != tag || entry.Height != height || entry.Hash != hash {
		return xerrors.Errorf("inconsistent entry values (%+v)", entry)
	}

	err = s.collectionStorage.UploadToBlobStorage(ctx, entry, false)
	if err != nil {
		return xerrors.Errorf("failed to upload data: %w", err)
	}

	if err := s.collectionStorage.WriteItem(ctx, entry); err != nil {
		return xerrors.Errorf("failed to write block by hash %w", err)
	}

	return nil
}

func (s *blockByHashStorageImpl) GetBlockByHash(ctx context.Context, tag uint32, hash string, maxSequence api.Sequence) (*ethereum.Block, error) {
	return s.getBlockByHash(ctx, tag, hash, maxSequence)
}

func (s *blockByHashStorageImpl) getBlockByHash(ctx context.Context, tag uint32, hash string, maxSequence api.Sequence) (*ethereum.Block, error) {
	// get the most recent block by hash entry that has sequence <= specified sequence
	queryResult, err := s.collectionStorage.QueryItemByMaxSortKey(
		ctx,
		models.MakeBlockByHashPartitionKey(tag, hash),
		models.MakeBlockByHashSortKey(maxSequence),
		models.BlockByHashDDBEntry{},
		nil,
	)
	if err != nil {
		return nil, xerrors.Errorf("failed to query block by hash: %w", err)
	}

	outputItem := queryResult.Items[0]
	blockByHashDDBEntry, ok := outputItem.(*models.BlockByHashDDBEntry)
	if !ok {
		return nil, xerrors.Errorf("failed to convert output=%v to BlockByHashDDBEntry", outputItem)
	}

	if blockByHashDDBEntry.ObjectKey != "" {
		err := s.collectionStorage.DownloadFromBlobStorage(ctx, blockByHashDDBEntry)
		if err != nil {
			return nil, xerrors.Errorf("failed to download data based on objectKey: %w", err)
		}
	}

	var block ethereum.Block
	err = blockByHashDDBEntry.AsAPI(&block)
	if err != nil {
		return nil, xerrors.Errorf("failed to convert from BlockByHashDDBEntry: %w", err)
	}

	return &block, nil
}
