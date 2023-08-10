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
	BlockExtraDataByNumberStorage interface {
		PersistBlockExtraDataByNumber(ctx context.Context, blockExtraData *ethereum.BlockExtraData) error
		GetBlockExtraDataByNumber(ctx context.Context, tag uint32, height uint64, maxSequence api.Sequence) (*ethereum.BlockExtraData, error)
	}

	blockExtraDataByNumberStorageImpl struct {
		collectionStorage collection.CollectionStorage `name:"collection"`
	}
)

var _ BlockExtraDataByNumberStorage = (*blockExtraDataByNumberStorageImpl)(nil)

func newBlockExtraDataByNumberStorage(params Params) (BlockExtraDataByNumberStorage, error) {
	return &blockExtraDataByNumberStorageImpl{
		collectionStorage: params.CollectionStorage.WithCollection(ethereum.CollectionBlocksExtraDataByNumber),
	}, nil
}

func (s *blockExtraDataByNumberStorageImpl) PersistBlockExtraDataByNumber(
	ctx context.Context,
	blockExtraData *ethereum.BlockExtraData,
) error {
	return s.persistBlockExtraDataByNumber(ctx, blockExtraData)
}

func (s *blockExtraDataByNumberStorageImpl) persistBlockExtraDataByNumber(
	ctx context.Context,
	blockExtraData *ethereum.BlockExtraData,
) error {
	if blockExtraData == nil {
		return xerrors.Errorf("blockExtraData cannot be nil")
	}

	tag := blockExtraData.Tag
	hash := blockExtraData.Hash
	height := blockExtraData.Height

	entry, err := models.MakeBlockExtraDataByNumberDDBEntry(blockExtraData)
	if err != nil {
		return xerrors.Errorf("failed to make blockExtraDataByNumber ddb entry: %w", err)
	}

	if entry.Tag != tag || entry.Height != height || entry.Hash != hash {
		return xerrors.Errorf("inconsistent entry values (%+v)", entry)
	}

	err = s.collectionStorage.UploadToBlobStorage(ctx, entry, false)
	if err != nil {
		return xerrors.Errorf("failed to upload data: %w", err)
	}

	if err := s.collectionStorage.WriteItem(ctx, entry); err != nil {
		return xerrors.Errorf("failed to write blockExtraData %w", err)
	}

	return nil
}

func (s *blockExtraDataByNumberStorageImpl) GetBlockExtraDataByNumber(ctx context.Context, tag uint32, height uint64, maxSequence api.Sequence) (*ethereum.BlockExtraData, error) {
	return s.getBlockExtraDataByNumber(ctx, tag, height, maxSequence)
}

func (s *blockExtraDataByNumberStorageImpl) getBlockExtraDataByNumber(ctx context.Context, tag uint32, height uint64, maxSequence api.Sequence) (*ethereum.BlockExtraData, error) {
	// get the most recent blockExtraData entry that has sequence <= specified sequence
	queryResult, err := s.collectionStorage.QueryItemByMaxSortKey(
		ctx,
		models.MakeBlockExtraDataByNumberPartitionKey(tag, height),
		models.MakeBlockExtraDataByNumberSortKey(maxSequence),
		models.BlockExtraDataByNumberDDBEntry{},
		nil,
	)
	if err != nil {
		return nil, xerrors.Errorf("failed to query blockExtraDataByNumber: %w", err)
	}

	outputItem := queryResult.Items[0]
	entry, ok := outputItem.(*models.BlockExtraDataByNumberDDBEntry)
	if !ok {
		return nil, xerrors.Errorf("failed to convert output=%v to BlockExtraDataByNumberDDBEntry", outputItem)
	}

	if entry.ObjectKey != "" {
		err := s.collectionStorage.DownloadFromBlobStorage(ctx, entry)
		if err != nil {
			return nil, xerrors.Errorf("failed to download data based on objectKey: %w", err)
		}
	}

	var blockExtraData ethereum.BlockExtraData
	err = entry.AsAPI(&blockExtraData)
	if err != nil {
		return nil, xerrors.Errorf("failed to convert from BlockExtraDataByNumberDDBEntry: %w", err)
	}

	return &blockExtraData, nil
}
