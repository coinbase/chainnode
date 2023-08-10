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
	TraceByHashStorage interface {
		// PersistTraceByHash persists trace by block hash
		PersistTraceByHash(ctx context.Context, trace *ethereum.Trace) error
		// GetTraceByHash gets trace from one block by block hash
		GetTraceByHash(ctx context.Context, tag uint32, hash string, maxSequence api.Sequence) (*ethereum.Trace, error)
	}

	traceByHashStorageImpl struct {
		config            *config.Config
		collectionStorage collection.CollectionStorage `name:"collection"`
	}
)

var _ TraceByHashStorage = (*traceByHashStorageImpl)(nil)

func newTraceByHashStorage(params Params) (TraceByHashStorage, error) {
	return &traceByHashStorageImpl{
		collectionStorage: params.CollectionStorage.WithCollection(ethereum.CollectionTracesByHash),
		config:            params.Config,
	}, nil
}

func (s *traceByHashStorageImpl) PersistTraceByHash(
	ctx context.Context,
	trace *ethereum.Trace,
) error {
	return s.persistTrace(ctx, trace)
}

func (s *traceByHashStorageImpl) persistTrace(
	ctx context.Context,
	trace *ethereum.Trace,
) error {
	if trace == nil {
		return xerrors.Errorf("trace cannot be empty")
	}

	entry, err := models.MakeTraceByHashDDBEntry(trace)
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

func (s *traceByHashStorageImpl) GetTraceByHash(
	ctx context.Context,
	tag uint32,
	hash string,
	maxSequence api.Sequence,
) (*ethereum.Trace, error) {
	return s.getTrace(ctx, tag, hash, maxSequence)
}

func (s *traceByHashStorageImpl) getTrace(
	ctx context.Context,
	tag uint32,
	hash string,
	maxSequence api.Sequence,
) (*ethereum.Trace, error) {
	// :pk = pk AND :sk = maxSequence
	queryResult, err := s.collectionStorage.QueryItemByMaxSortKey(
		ctx,
		models.MakeTraceByHashPartitionKey(tag, hash),
		models.MakeTraceByHashSortKey(maxSequence),
		models.TraceByHashDDBEntry{},
		nil,
	)
	if err != nil {
		return nil, xerrors.Errorf("failed to query trace by hash: %w", err)
	}

	if len(queryResult.Items) == 0 {
		return nil, xerrors.Errorf("got empty result: %w", internal.ErrItemNotFound)
	}

	outputItem := queryResult.Items[0]
	traceByHashDDBEntry, ok := queryResult.Items[0].(*models.TraceByHashDDBEntry)
	if !ok {
		return nil, xerrors.Errorf("failed to convert output=%v to traceByHashDDBEntry", outputItem)
	}

	if traceByHashDDBEntry.ObjectKey != "" {
		err = s.collectionStorage.DownloadFromBlobStorage(ctx, traceByHashDDBEntry)
		if err != nil {
			return nil, xerrors.Errorf("failed to download data based on objectKey: %w", err)
		}
	}

	var trace ethereum.Trace
	err = traceByHashDDBEntry.AsAPI(&trace)
	if err != nil {
		return nil, xerrors.Errorf("failed to convert TraceByHashDDBEntry to Trace: %w", err)
	}

	return &trace, nil
}
