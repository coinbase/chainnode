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
	BlockByHashWithoutFullTxStorage interface {
		// PersistBlockByHashWithoutFullTx persists a block by hash, which does NOT include full tx data
		PersistBlockByHashWithoutFullTx(ctx context.Context, block *ethereum.Block) error
		// GetBlockByHashWithoutFullTx gets the most recent block by hash (without tx) that has a block sequence that is no later than the specified maxSequence
		GetBlockByHashWithoutFullTx(ctx context.Context, tag uint32, hash string, maxSequence api.Sequence) (*ethereum.Block, error)
	}

	blockByHashWithoutFullTxStorageImpl struct {
		collectionStorage collection.CollectionStorage `name:"collection"`
	}
)

var _ BlockByHashWithoutFullTxStorage = (*blockByHashWithoutFullTxStorageImpl)(nil)

func newBlockByHashWithoutFullTxStorage(params Params) (BlockByHashWithoutFullTxStorage, error) {
	return &blockByHashWithoutFullTxStorageImpl{
		collectionStorage: params.CollectionStorage.WithCollection(ethereum.CollectionBlocksByHashWithoutFullTx),
	}, nil
}

func (s *blockByHashWithoutFullTxStorageImpl) PersistBlockByHashWithoutFullTx(
	ctx context.Context,
	block *ethereum.Block,
) error {
	return s.persistBlockByHashWithoutFullTx(ctx, block)
}

func (s *blockByHashWithoutFullTxStorageImpl) persistBlockByHashWithoutFullTx(
	ctx context.Context,
	block *ethereum.Block,
) error {
	if block == nil {
		return xerrors.Errorf("block cannot be nil")
	}

	tag := block.Tag
	hash := block.Hash
	height := block.Height

	entry, err := models.MakeBlockByHashWithoutFullTxDDBEntry(block)
	if err != nil {
		return xerrors.Errorf("failed to make BlockByHashWithoutFullTxDDBEntry: %w", err)
	}

	if entry.Tag != tag || entry.Height != height || entry.Hash != hash {
		return xerrors.Errorf("inconsistent entry values (%+v)", entry)
	}

	err = s.collectionStorage.UploadToBlobStorage(ctx, entry, false)
	if err != nil {
		return xerrors.Errorf("failed to upload data: %w", err)
	}

	if err := s.collectionStorage.WriteItem(ctx, entry); err != nil {
		return xerrors.Errorf("failed to write BlockByHashWithoutFullTxDDBEntry %w", err)
	}

	return nil
}

func (s *blockByHashWithoutFullTxStorageImpl) GetBlockByHashWithoutFullTx(ctx context.Context, tag uint32, hash string, maxSequence api.Sequence) (*ethereum.Block, error) {
	return s.getBlockByHashWithoutFullTx(ctx, tag, hash, maxSequence)
}

func (s *blockByHashWithoutFullTxStorageImpl) getBlockByHashWithoutFullTx(ctx context.Context, tag uint32, hash string, maxSequence api.Sequence) (*ethereum.Block, error) {
	// get the most recent entry that has sequence <= specified sequence
	queryResult, err := s.collectionStorage.QueryItemByMaxSortKey(
		ctx,
		models.MakeBlockByHashWithoutFullTxPartitionKey(tag, hash),
		models.MakeBlockByHashWithoutFullTxSortKey(maxSequence),
		models.BlockByHashWithoutFullTxDDBEntry{},
		nil,
	)
	if err != nil {
		return nil, xerrors.Errorf("failed to query BlockByHashWithoutFullTxDDBEntry: %w", err)
	}

	outputItem := queryResult.Items[0]
	entry, ok := outputItem.(*models.BlockByHashWithoutFullTxDDBEntry)
	if !ok {
		return nil, xerrors.Errorf("failed to convert output=%v to BlockByHashWithoutFullTxDDBEntry", outputItem)
	}

	if entry.ObjectKey != "" {
		err := s.collectionStorage.DownloadFromBlobStorage(ctx, entry)
		if err != nil {
			return nil, xerrors.Errorf("failed to download data based on objectKey: %w", err)
		}
	}

	var block ethereum.Block
	err = entry.AsAPI(&block)
	if err != nil {
		return nil, xerrors.Errorf("failed to convert from BlockByHashWithoutFullTxDDBEntry: %w", err)
	}

	return &block, nil
}
