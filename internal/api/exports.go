package api

import (
	chainstorageapi "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"
)

type (
	Event       = chainstorageapi.BlockchainEvent
	Block       = chainstorageapi.Block
	NativeBlock = chainstorageapi.NativeBlock
)

const (
	EventAdded   = chainstorageapi.BlockchainEvent_BLOCK_ADDED
	EventRemoved = chainstorageapi.BlockchainEvent_BLOCK_REMOVED
)
