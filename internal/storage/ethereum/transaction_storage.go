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
	TransactionStorage interface {
		PersistTransactions(ctx context.Context, transactions []*ethereum.Transaction) error
		GetTransactionByHash(ctx context.Context, tag uint32, hash string, maxSequence api.Sequence) (*ethereum.Transaction, error)
	}

	transactionStorageImpl struct {
		collectionStorage collection.CollectionStorage `name:"collection"`
	}
)

var _ TransactionStorage = (*transactionStorageImpl)(nil)

func newTransactionStorage(params Params) (TransactionStorage, error) {
	return &transactionStorageImpl{
		collectionStorage: params.CollectionStorage.WithCollection(ethereum.CollectionTransactions),
	}, nil
}

func (s *transactionStorageImpl) PersistTransactions(
	ctx context.Context,
	transactions []*ethereum.Transaction,
) error {
	return s.persistTransactions(ctx, transactions)
}

func (s *transactionStorageImpl) persistTransactions(
	ctx context.Context,
	transactions []*ethereum.Transaction,
) error {
	if len(transactions) == 0 {
		return xerrors.Errorf("transactions cannot be empty")
	}

	entries := make([]interface{}, len(transactions))
	for i, transaction := range transactions {
		entry, err := s.prepareTransaction(ctx, transaction)
		if err != nil {
			return xerrors.Errorf("failed to prepare transaction entry: %w", err)
		}
		entries[i] = entry
	}

	if err := s.collectionStorage.WriteItems(ctx, entries); err != nil {
		return xerrors.Errorf("failed to write items for transaction-by-hash: %w", err)
	}

	return nil
}

func (s *transactionStorageImpl) GetTransactionByHash(
	ctx context.Context,
	tag uint32,
	hash string,
	maxSequence api.Sequence,
) (*ethereum.Transaction, error) {
	return s.getTransactionByHash(ctx, tag, hash, maxSequence)
}

func (s *transactionStorageImpl) getTransactionByHash(
	ctx context.Context,
	tag uint32,
	hash string,
	maxSequence api.Sequence,
) (*ethereum.Transaction, error) {
	// :pk = pk AND :sk <= maxSequence
	queryResult, err := s.collectionStorage.QueryItemByMaxSortKey(
		ctx,
		models.MakeTransactionByHashPartitionKey(tag, hash),
		models.MakeTransactionByHashSortKey(maxSequence),
		models.TransactionByHashDDBEntry{},
		nil,
	)
	if err != nil {
		return nil, xerrors.Errorf("failed to query transaction by hash: %w", err)
	}

	if len(queryResult.Items) == 0 {
		return nil, xerrors.Errorf("got empty result: %w", internal.ErrItemNotFound)
	}

	outputItem := queryResult.Items[0]
	entry, ok := outputItem.(*models.TransactionByHashDDBEntry)
	if !ok {
		return nil, xerrors.Errorf("failed to convert output=%v to TransactionByHashDDBEntry", outputItem)
	}

	if entry.ObjectKey != "" {
		err := s.collectionStorage.DownloadFromBlobStorage(ctx, entry)
		if err != nil {
			return nil, xerrors.Errorf("failed to download data based on objectKey: %w", err)
		}
	}

	var transaction ethereum.Transaction
	err = entry.AsAPI(&transaction)
	if err != nil {
		return nil, xerrors.Errorf("failed to convert from TransactionByHashDDBEntry: %w", err)
	}
	return &transaction, nil
}

func (s *transactionStorageImpl) prepareTransaction(
	ctx context.Context,
	transaction *ethereum.Transaction,
) (*models.TransactionByHashDDBEntry, error) {
	if transaction == nil {
		return nil, xerrors.Errorf("transaction cannot be nil")
	}

	entry, err := models.MakeTransactionByHashDDBEntry(transaction)
	if err != nil {
		return nil, xerrors.Errorf("failed to make transaction-by-hash ddb entry: %w", err)
	}

	if entry.Tag != transaction.Tag || entry.Hash != transaction.Hash {
		return nil, xerrors.Errorf("inconsistent entry values (%+v)", entry)
	}

	err = s.collectionStorage.UploadToBlobStorage(ctx, entry, false)
	if err != nil {
		return nil, xerrors.Errorf("failed to upload data: %w", err)
	}
	return entry, nil
}
