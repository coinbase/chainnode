package ethereum

import (
	"go.uber.org/fx"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainnode/internal/storage/collection"
	"github.com/coinbase/chainnode/internal/utils/fxparams"
)

type (
	Storage interface {
		BlockStorage
		BlockExtraDataByNumberStorage
		BlockByHashStorage
		BlockByHashWithoutFullTxStorage
		LogStorageV2
		TransactionStorage
		TransactionReceiptStorage
		TraceByHashStorage
		TraceByNumberStorage
		ArbtraceBlockStorage
	}

	storageImpl struct {
		BlockStorage
		BlockExtraDataByNumberStorage
		BlockByHashStorage
		BlockByHashWithoutFullTxStorage
		LogStorageV2
		TransactionStorage
		TransactionReceiptStorage
		TraceByHashStorage
		TraceByNumberStorage
		ArbtraceBlockStorage
	}

	Params struct {
		fx.In
		fxparams.Params

		CollectionStorage collection.CollectionStorage `name:"collection"`
		LogsStorage       collection.CollectionStorage `name:"logs"`
	}

	Result struct {
		fx.Out
		BlockStorage                    BlockStorage
		BlockExtraDataByNumberStorage   BlockExtraDataByNumberStorage
		BlockByHashStorage              BlockByHashStorage
		BlockByHashWithoutFullTxStorage BlockByHashWithoutFullTxStorage
		LogStorageV2                    LogStorageV2
		TransactionStorage              TransactionStorage
		TransactionReceiptStorage       TransactionReceiptStorage
		TraceByHashStorage              TraceByHashStorage
		TraceByNumberStorage            TraceByNumberStorage
		ArbtraceBlockStorage            ArbtraceBlockStorage
		Storage                         Storage
	}
)

func New(params Params) (Result, error) {
	blockStorage, err := newBlockStorage(params)
	if err != nil {
		return Result{}, xerrors.Errorf("failed to create block storage: %w", err)
	}

	blockExtraDataByNumberStorage, err := newBlockExtraDataByNumberStorage(params)
	if err != nil {
		return Result{}, xerrors.Errorf("failed to create block extra data storage: %w", err)
	}

	blockByHashStorage, err := newBlockByHashStorage(params)
	if err != nil {
		return Result{}, xerrors.Errorf("failed to create block by hash storage: %w", err)
	}

	blockByHashWithoutFullTxStorage, err := newBlockByHashWithoutFullTxStorage(params)
	if err != nil {
		return Result{}, xerrors.Errorf("failed to create block by hash without tx storage: %w", err)
	}

	logStorageV2, err := newLogStorageV2(params)
	if err != nil {
		return Result{}, xerrors.Errorf("failed to create log storage v2: %w", err)
	}

	transactionStorage, err := newTransactionStorage(params)
	if err != nil {
		return Result{}, xerrors.Errorf("failed to create transaction storage: %w", err)
	}

	transactionReceiptStorage, err := newTransactionReceiptStorage(params)
	if err != nil {
		return Result{}, xerrors.Errorf("failed to create transaction receipt storage: %w", err)
	}

	traceByHashStorage, err := newTraceByHashStorage(params)
	if err != nil {
		return Result{}, xerrors.Errorf("failed to create traceByHash storage: %w", err)
	}

	traceByNumberStorage, err := newTraceByNumberStorage(params)
	if err != nil {
		return Result{}, xerrors.Errorf("failed to create traceByNumber storage: %w", err)
	}

	arbtraceBlockStorage, err := newArbtraceBlockStorage(params)
	if err != nil {
		return Result{}, xerrors.Errorf("failed to create arbtraceBlock storage: %w", err)
	}

	storage := &storageImpl{
		BlockStorage:                    blockStorage,
		BlockExtraDataByNumberStorage:   blockExtraDataByNumberStorage,
		BlockByHashStorage:              blockByHashStorage,
		BlockByHashWithoutFullTxStorage: blockByHashWithoutFullTxStorage,
		LogStorageV2:                    logStorageV2,
		TransactionStorage:              transactionStorage,
		TransactionReceiptStorage:       transactionReceiptStorage,
		TraceByHashStorage:              traceByHashStorage,
		TraceByNumberStorage:            traceByNumberStorage,
		ArbtraceBlockStorage:            arbtraceBlockStorage,
	}
	return Result{
		BlockStorage:                    storage.BlockStorage,
		BlockExtraDataByNumberStorage:   storage.BlockExtraDataByNumberStorage,
		BlockByHashStorage:              storage.BlockByHashStorage,
		BlockByHashWithoutFullTxStorage: blockByHashWithoutFullTxStorage,
		LogStorageV2:                    storage.LogStorageV2,
		TransactionStorage:              storage.TransactionStorage,
		TransactionReceiptStorage:       storage.TransactionReceiptStorage,
		TraceByHashStorage:              storage.TraceByHashStorage,
		TraceByNumberStorage:            storage.TraceByNumberStorage,
		ArbtraceBlockStorage:            storage.ArbtraceBlockStorage,
		Storage:                         storage,
	}, nil
}
