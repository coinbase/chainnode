package ethereum

import (
	"github.com/coinbase/chainnode/internal/api"
)

const (
	CollectionBlocks                    api.Collection = "blocks"
	CollectionBlocksExtraDataByNumber   api.Collection = "blocks-extra-data-by-number"
	CollectionLogsV2                    api.Collection = "logs-v2"
	CollectionTransactions              api.Collection = "transactions"
	CollectionTransactionReceipts       api.Collection = "transaction-receipts"
	CollectionTracesByHash              api.Collection = "traces-by-hash"
	CollectionTracesByNumber            api.Collection = "traces-by-number"
	CollectionBlocksByHash              api.Collection = "blocks-by-hash"
	CollectionBlocksByHashWithoutFullTx api.Collection = "blocks-by-hash-without-full-tx"
	CollectionArbtraceBlock             api.Collection = "arbtrace-block"
)

// ParentCollections elements in ParentCollections have dedicated indexer.
var ParentCollections = []api.Collection{
	CollectionBlocks,
	CollectionBlocksExtraDataByNumber,
	CollectionLogsV2,
	CollectionTransactions,
	CollectionTransactionReceipts,
	CollectionTracesByHash,
	CollectionTracesByNumber,
	CollectionBlocksByHash,
}

// ChildCollections elements in ChildCollections do not have dedicated indexer.
// These collections will be indexed by its parent collection's indexer.
// It is required that every child collection should have one and only one parent collection.
var ChildCollections = []api.Collection{
	CollectionBlocksByHashWithoutFullTx,
}

// ChildToParentCollectionMap the map from a child collection to its parent collection
var ChildToParentCollectionMap = map[api.Collection]api.Collection{
	CollectionBlocksByHashWithoutFullTx: CollectionBlocksByHash,
}
