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
	TransactionReceiptStorage interface {
		PersistTransactionReceipt(ctx context.Context, receipt *ethereum.TransactionReceipt) error
		PersistTransactionReceipts(ctx context.Context, receipts []*ethereum.TransactionReceipt) error
		GetTransactionReceipt(ctx context.Context, tag uint32, hash string, maxSequence api.Sequence) (*ethereum.TransactionReceipt, error)
	}

	transactionReceiptStorageImpl struct {
		collectionStorage collection.CollectionStorage `name:"collection"`
	}
)

var _ TransactionReceiptStorage = (*transactionReceiptStorageImpl)(nil)

func newTransactionReceiptStorage(params Params) (TransactionReceiptStorage, error) {
	return &transactionReceiptStorageImpl{
		collectionStorage: params.CollectionStorage.WithCollection(ethereum.CollectionTransactionReceipts),
	}, nil
}

func (s *transactionReceiptStorageImpl) PersistTransactionReceipt(
	ctx context.Context,
	receipt *ethereum.TransactionReceipt,
) error {
	return s.persistTransactionReceipt(ctx, receipt)
}

func (s *transactionReceiptStorageImpl) persistTransactionReceipt(
	ctx context.Context,
	receipt *ethereum.TransactionReceipt,
) error {
	entry, err := s.prepareTransactionReceipt(ctx, receipt)
	if err != nil {
		return xerrors.Errorf("failed to prepare receipt entry: %w", err)
	}

	if err := s.collectionStorage.WriteItem(ctx, entry); err != nil {
		return xerrors.Errorf("failed to write receipt: %w", err)
	}

	return nil
}

func (s *transactionReceiptStorageImpl) PersistTransactionReceipts(
	ctx context.Context,
	receipts []*ethereum.TransactionReceipt,
) error {
	return s.persistTransactionReceipts(ctx, receipts)
}

func (s *transactionReceiptStorageImpl) persistTransactionReceipts(
	ctx context.Context,
	receipts []*ethereum.TransactionReceipt,
) error {
	if len(receipts) == 0 {
		return xerrors.Errorf("receipts cannot be empty")
	}

	entries := make([]interface{}, len(receipts))
	for i, receipt := range receipts {
		entry, err := s.prepareTransactionReceipt(ctx, receipt)
		if err != nil {
			return xerrors.Errorf("failed to prepare receipt entry: %w", err)
		}
		entries[i] = entry
	}

	if err := s.collectionStorage.WriteItems(ctx, entries); err != nil {
		return xerrors.Errorf("failed to write items for receipt: %w", err)
	}

	return nil
}

func (s *transactionReceiptStorageImpl) GetTransactionReceipt(
	ctx context.Context,
	tag uint32,
	hash string,
	maxSequence api.Sequence,
) (*ethereum.TransactionReceipt, error) {
	return s.getTransactionReceipt(ctx, tag, hash, maxSequence)
}

func (s *transactionReceiptStorageImpl) getTransactionReceipt(
	ctx context.Context,
	tag uint32,
	hash string,
	maxSequence api.Sequence,
) (*ethereum.TransactionReceipt, error) {
	// :pk = pk AND :sk <= maxSequence
	queryResult, err := s.collectionStorage.QueryItemByMaxSortKey(
		ctx,
		models.MakeTransactionReceiptPartitionKey(tag, hash),
		models.MakeTransactionReceiptSortKey(maxSequence),
		models.TransactionReceiptDDBEntry{},
		nil,
	)
	if err != nil {
		return nil, xerrors.Errorf("failed to query receipt by hash: %w", err)
	}

	if len(queryResult.Items) == 0 {
		return nil, xerrors.Errorf("got empty result: %w", internal.ErrItemNotFound)
	}

	outputItem := queryResult.Items[0]
	entry, ok := outputItem.(*models.TransactionReceiptDDBEntry)
	if !ok {
		return nil, xerrors.Errorf("failed to convert output=%v to TransactionReceiptDDBEntry", outputItem)
	}

	if entry.ObjectKey != "" {
		err := s.collectionStorage.DownloadFromBlobStorage(ctx, entry)
		if err != nil {
			return nil, xerrors.Errorf("failed to download data based on objectKey: %w", err)
		}
	}

	var receipt ethereum.TransactionReceipt
	err = entry.AsAPI(&receipt)
	if err != nil {
		return nil, xerrors.Errorf("failed to convert from TransactionReceiptDDBEntry: %w", err)
	}
	return &receipt, nil
}

func (s *transactionReceiptStorageImpl) prepareTransactionReceipt(
	ctx context.Context,
	receipt *ethereum.TransactionReceipt,
) (*models.TransactionReceiptDDBEntry, error) {
	if receipt == nil {
		return nil, xerrors.Errorf("receipt cannot be nil")
	}

	entry, err := models.MakeTransactionReceiptDDBEntry(receipt)
	if err != nil {
		return nil, xerrors.Errorf("failed to make receipt ddb entry: %w", err)
	}

	if entry.Tag != receipt.Tag || entry.Hash != receipt.Hash {
		return nil, xerrors.Errorf("inconsistent entry values (%+v)", entry)
	}

	err = s.collectionStorage.UploadToBlobStorage(ctx, entry, false)
	if err != nil {
		return nil, xerrors.Errorf("failed to upload data: %w", err)
	}
	return entry, nil
}
