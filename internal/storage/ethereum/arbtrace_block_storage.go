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
	ArbtraceBlockStorage interface {
		// PersistArbtraceBlock persists trace by block height
		PersistArbtraceBlock(ctx context.Context, trace *ethereum.Trace) error
		// GetArbtraceBlock gets trace from one block by block height
		GetArbtraceBlock(ctx context.Context, tag uint32, height uint64, maxSequence api.Sequence) (*ethereum.Trace, error)
	}

	arbtraceBlockStorageImpl struct {
		collectionStorage collection.CollectionStorage `name:"collection"`
	}
)

var _ ArbtraceBlockStorage = (*arbtraceBlockStorageImpl)(nil)

func newArbtraceBlockStorage(params Params) (ArbtraceBlockStorage, error) {
	return &arbtraceBlockStorageImpl{
		collectionStorage: params.CollectionStorage.WithCollection(ethereum.CollectionArbtraceBlock),
	}, nil
}

func (s *arbtraceBlockStorageImpl) PersistArbtraceBlock(
	ctx context.Context,
	trace *ethereum.Trace,
) error {
	return s.persistTrace(ctx, trace)
}

func (s *arbtraceBlockStorageImpl) persistTrace(
	ctx context.Context,
	trace *ethereum.Trace,
) error {
	if trace == nil {
		return xerrors.Errorf("arbtrace cannot be empty")
	}

	entry, err := models.MakeArbtraceBlockDDBEntry(trace)
	if err != nil {
		return xerrors.Errorf("failed to make arbtrace ddb entry: %w", err)
	}

	if entry.Tag != trace.Tag || entry.Height != trace.Height ||
		entry.Hash != trace.Hash {
		return xerrors.Errorf("inconsistent entry values (%+v)", entry)
	}

	err = s.collectionStorage.UploadToBlobStorage(ctx, entry, false)
	if err != nil {
		return xerrors.Errorf("failed to upload data: %w", err)
	}

	if err := s.collectionStorage.WriteItem(ctx, entry); err != nil {
		return xerrors.Errorf("failed to write arbtrace %w", err)
	}
	return nil
}

func (s *arbtraceBlockStorageImpl) GetArbtraceBlock(
	ctx context.Context,
	tag uint32,
	height uint64,
	maxSequence api.Sequence,
) (*ethereum.Trace, error) {
	return s.getTrace(ctx, tag, height, maxSequence)
}

func (s *arbtraceBlockStorageImpl) getTrace(
	ctx context.Context,
	tag uint32,
	height uint64,
	maxSequence api.Sequence,
) (*ethereum.Trace, error) {
	// :pk = pk AND :sk = maxSequence
	queryResult, err := s.collectionStorage.QueryItemByMaxSortKey(
		ctx,
		models.MakeArbtraceBlockPartitionKey(tag, height),
		models.MakeArbtraceBlockSortKey(maxSequence),
		models.ArbtraceBlockDDBEntry{},
		nil,
	)
	if err != nil {
		return nil, xerrors.Errorf("failed to query arbtrace by number: %w", err)
	}

	if len(queryResult.Items) == 0 {
		return nil, xerrors.Errorf("got empty result: %w", internal.ErrItemNotFound)
	}

	outputItem := queryResult.Items[0]
	arbtraceBlockDDBEntry, ok := queryResult.Items[0].(*models.ArbtraceBlockDDBEntry)
	if !ok {
		return nil, xerrors.Errorf("failed to convert output=%v to ArbtraceBlockDDBEntry", outputItem)
	}

	if arbtraceBlockDDBEntry.ObjectKey != "" {
		err = s.collectionStorage.DownloadFromBlobStorage(ctx, arbtraceBlockDDBEntry)
		if err != nil {
			return nil, xerrors.Errorf("failed to download data based on objectKey: %w", err)
		}
	}

	var trace ethereum.Trace
	err = arbtraceBlockDDBEntry.AsAPI(&trace)
	if err != nil {
		return nil, xerrors.Errorf("failed to convert ArbtraceBlockDDBEntry to Arbtrace: %w", err)
	}

	return &trace, nil
}
