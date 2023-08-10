package testutil

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/ethereum/go-ethereum/common/hexutil"

	"github.com/coinbase/chainstorage/protos/coinbase/c3/common"
	chainstorageapi "github.com/coinbase/chainstorage/protos/coinbase/chainstorage"

	"github.com/coinbase/chainnode/internal/api"
	"github.com/coinbase/chainnode/internal/api/ethereum"
)

type (
	Option func(*builderOptions)

	builderOptions struct {
		blockHashFormat  string
		dataCompression  chainstorageapi.Compression
		blockSkipped     bool
		withoutTimestamp bool
		blockHeight      uint64
	}

	Header struct {
		Hash       string `json:"hash"`
		ParentHash string `json:"parentHash"`
		Number     string `json:"number"`
		Timestamp  string `json:"timestamp"`
		LogsBloom  string `json:"logsBloom"`
	}
)

const (
	blockTimestamp = 0x5fbd2fb9
	SequenceOffset = 11_000_000
)

func MakeBlockEvent(sequence api.Sequence, tag uint32, eventTag uint32, opts ...Option) *chainstorageapi.BlockchainEvent {
	height := uint64(sequence + SequenceOffset)
	block := getBlock(height, tag, opts...)
	return &chainstorageapi.BlockchainEvent{
		SequenceNum: sequence.AsInt64(),
		Type:        chainstorageapi.BlockchainEvent_BLOCK_ADDED,
		Block: &chainstorageapi.BlockIdentifier{
			Hash:      block.Hash,
			Height:    block.Height,
			Tag:       block.Tag,
			Skipped:   block.Skipped,
			Timestamp: block.Timestamp,
		},
	}
}

func MakeBlockEvents(sequence api.Sequence, size int, tag uint32, eventTag uint32, opts ...Option) []*chainstorageapi.BlockchainEvent {
	events := make([]*chainstorageapi.BlockchainEvent, 0, size)
	for i := int64(sequence); i < int64(sequence)+int64(size); i++ {
		events = append(events, MakeBlockEvent(api.Sequence(i+1), tag, eventTag, opts...))
	}
	return events
}

func getBlock(height uint64, tag uint32, opts ...Option) *chainstorageapi.BlockMetadata {
	options := &builderOptions{
		blockHashFormat: "0x%s",
	}
	for _, opt := range opts {
		opt(options)
	}

	if options.blockHeight != 0 {
		height = options.blockHeight
	}

	if options.blockSkipped {
		return &chainstorageapi.BlockMetadata{
			Tag:     tag,
			Height:  height,
			Skipped: true,
		}
	}

	hash := fmt.Sprintf(options.blockHashFormat, strconv.FormatInt(int64(height), 16))
	parentHash := fmt.Sprintf(options.blockHashFormat, strconv.FormatInt(int64(height-1), 16))
	objectKeyMain := fmt.Sprintf("%v/%v/%v", tag, height, hash)
	if options.dataCompression == chainstorageapi.Compression_GZIP {
		objectKeyMain += ".gzip"
	}

	metadata := &chainstorageapi.BlockMetadata{
		Tag:           tag,
		Hash:          hash,
		ParentHash:    parentHash,
		Height:        height,
		ParentHeight:  height - 1,
		ObjectKeyMain: objectKeyMain,
	}

	if !options.withoutTimestamp {
		metadata.Timestamp = ToTimestamp(blockTimestamp)
	}
	return metadata
}

func MakeBlockMetadata(height uint64, tag uint32, opts ...Option) *chainstorageapi.BlockMetadata {
	block := getBlock(height, tag, opts...)
	return block
}

func MakeBlockMetadatas(size int, tag uint32, opts ...Option) []*chainstorageapi.BlockMetadata {
	return MakeBlockMetadatasFromStartHeight(0, size, tag, opts...)
}

func MakeBlockMetadatasFromStartHeight(startHeight uint64, size int, tag uint32, opts ...Option) []*chainstorageapi.BlockMetadata {
	blocks := make([]*chainstorageapi.BlockMetadata, size)
	for i := 0; i < size; i++ {
		height := startHeight + uint64(i)
		blocks[i] = MakeBlockMetadata(height, tag, opts...)
	}
	return blocks
}

func MakeBlock(height uint64, tag uint32, opts ...Option) *chainstorageapi.Block {
	metadata := MakeBlockMetadata(height, tag, opts...)
	header := Header{
		Hash:       metadata.Hash,
		ParentHash: metadata.ParentHash,
		Number:     hexutil.EncodeUint64(metadata.Height),
		Timestamp:  hexutil.EncodeUint64(blockTimestamp),
	}
	headerData, err := json.Marshal(header)
	if err != nil {
		panic(err)
	}

	return &chainstorageapi.Block{
		Blockchain: common.Blockchain_BLOCKCHAIN_ETHEREUM,
		Network:    common.Network_NETWORK_ETHEREUM_MAINNET,
		Metadata:   metadata,
		Blobdata: &chainstorageapi.Block_Ethereum{
			Ethereum: &chainstorageapi.EthereumBlobdata{
				Header: headerData,
			},
		},
	}
}

func MakeTime(value string) time.Time {
	if value == "" {
		return time.Time{}
	}

	t, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		panic(err)
	}
	return t
}

func MakeBlocksFromStartHeight(startHeight uint64, size int, tag uint32, opts ...Option) []*chainstorageapi.Block {
	blocks := make([]*chainstorageapi.Block, size)
	for i := 0; i < size; i++ {
		height := startHeight + uint64(i)
		blocks[i] = MakeBlock(height, tag, opts...)
	}

	return blocks
}

func MakeEthereumLogs(
	tag uint32,
	sequence api.Sequence,
	height uint64,
	hash string,
	logsBloom string,
	data []byte,
	blockTime string,
) (*ethereum.Logs, error) {
	t, err := time.Parse(time.RFC3339Nano, blockTime)
	if err != nil {
		return nil, err
	}
	return ethereum.NewLogs(tag, height, hash, sequence, logsBloom, data, t), nil
}

func MakeEthereumLogsLite(
	tag uint32,
	sequence api.Sequence,
	height uint64,
	logsBloom string,
) (*ethereum.LogsLite, error) {
	return ethereum.NewLogsLite(tag, height, sequence, logsBloom), nil
}

func MakeEthereumTrace(
	tag uint32,
	sequence api.Sequence,
	height uint64,
	hash string,
	data []byte,
	blockTime string,
) (*ethereum.Trace, error) {
	t, err := time.Parse(time.RFC3339Nano, blockTime)
	if err != nil {
		return nil, err
	}
	return ethereum.NewTrace(tag, height, hash, sequence, data, t), nil
}

func MakeFile(size int) []byte {
	data := make([]byte, size)
	for i := 0; i < len(data); i++ {
		data[i] = 0x1
	}
	return data
}

func WithBlockHashFormat(format string) Option {
	return func(opts *builderOptions) {
		opts.blockHashFormat = format
	}
}

func WithDataCompression(compression chainstorageapi.Compression) Option {
	return func(opts *builderOptions) {
		opts.dataCompression = compression
	}
}

func WithBlockSkipped() Option {
	return func(opts *builderOptions) {
		opts.blockSkipped = true
	}
}

func WithoutTimestamp() Option {
	return func(opts *builderOptions) {
		opts.withoutTimestamp = true
	}
}

func WithBlockHeight(height uint64) Option {
	return func(opts *builderOptions) {
		opts.blockHeight = height
	}
}
