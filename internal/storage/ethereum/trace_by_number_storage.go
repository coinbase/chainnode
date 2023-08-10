package ethereum

import (
	"context"

	"golang.org/x/xerrors"

	"github.com/coinbase/chainnode/internal/api"
	"github.com/coinbase/chainnode/internal/api/ethereum"
	"github.com/coinbase/chainnode/internal/config"
	"github.com/coinbase/chainnode/internal/storage/collection"
	"github.com/coinbase/chainnode/internal/storage/ethereum/models"
	"github.com/coinbase/chainnode/internal/storage/internal"
)

type (
	TraceByNumberStorage interface {
		// PersistTraceByNumber persists trace by block height
		PersistTraceByNumber(ctx context.Context, trace *ethereum.Trace) error
		// GetTraceByNumber gets trace from one block by block height
		GetTraceByNumber(ctx context.Context, tag uint32, height uint64, maxSequence api.Sequence) (*ethereum.Trace, error)
	}

	traceByNumberStorageImpl struct {
		config            *config.Config
		collectionStorage collection.CollectionStorage `name:"collection"`
	}
)

var _ TraceByNumberStorage = (*traceByNumberStorageImpl)(nil)

func newTraceByNumberStorage(params Params) (TraceByNumberStorage, error) {
	return &traceByNumberStorageImpl{
		collectionStorage: params.CollectionStorage.WithCollection(ethereum.CollectionTracesByNumber),
		config:            params.Config,
	}, nil
}

func (s *traceByNumberStorageImpl) PersistTraceByNumber(
	ctx context.Context,
	trace *ethereum.Trace,
) error {
	return s.persistTrace(ctx, trace)
}

func (s *traceByNumberStorageImpl) persistTrace(
	ctx context.Context,
	trace *ethereum.Trace,
) error {
	if trace == nil {
		return xerrors.Errorf("trace cannot be empty")
	}

	entry, err := models.MakeTraceByNumberDDBEntry(trace)
	if err != nil {
		return xerrors.Errorf("failed to make trace ddb entry: %w", err)
	}

	if entry.Tag != trace.Tag || entry.Height != trace.Height ||
		entry.Hash != trace.Hash {
		return xerrors.Errorf("inconsistent entry values (%+v)", entry)
	}

	err = s.collectionStorage.UploadToBlobStorage(ctx, entry, s.config.Storage.TraceUploadEnforced)
	if err != nil {
		return xerrors.Errorf("failed to upload data: %w", err)
	}

	if err := s.collectionStorage.WriteItem(ctx, entry); err != nil {
		return xerrors.Errorf("failed to write trace %w", err)
	}
	return nil
}

func (s *traceByNumberStorageImpl) GetTraceByNumber(
	ctx context.Context,
	tag uint32,
	height uint64,
	maxSequence api.Sequence,
) (*ethereum.Trace, error) {
	return s.getTrace(ctx, tag, height, maxSequence)
}

func (s *traceByNumberStorageImpl) getTrace(
	ctx context.Context,
	tag uint32,
	height uint64,
	maxSequence api.Sequence,
) (*ethereum.Trace, error) {
	// :pk = pk AND :sk = maxSequence
	queryResult, err := s.collectionStorage.QueryItemByMaxSortKey(
		ctx,
		models.MakeTraceByNumberPartitionKey(tag, height),
		models.MakeTraceByNumberSortKey(maxSequence),
		models.TraceByNumberDDBEntry{},
		nil,
	)
	if err != nil {
		return nil, xerrors.Errorf("failed to query trace by number: %w", err)
	}

	if len(queryResult.Items) == 0 {
		return nil, xerrors.Errorf("got empty result: %w", internal.ErrItemNotFound)
	}

	outputItem := queryResult.Items[0]
	traceByNumberDDBEntry, ok := queryResult.Items[0].(*models.TraceByNumberDDBEntry)
	if !ok {
		return nil, xerrors.Errorf("failed to convert output=%v to TraceByNumberDDBEntry", outputItem)
	}

	if traceByNumberDDBEntry.ObjectKey != "" {
		err = s.collectionStorage.DownloadFromBlobStorage(ctx, traceByNumberDDBEntry)
		if err != nil {
			return nil, xerrors.Errorf("failed to download data based on objectKey: %w", err)
		}
	}

	var trace ethereum.Trace
	err = traceByNumberDDBEntry.AsAPI(&trace)
	if err != nil {
		return nil, xerrors.Errorf("failed to convert TraceByNumberDDBEntry to Trace: %w", err)
	}

	return &trace, nil
}
