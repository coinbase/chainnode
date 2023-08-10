package ethereum

import (
	"time"

	"github.com/coinbase/chainnode/internal/api"
)

type Block struct {
	Tag       uint32
	Height    uint64
	Hash      string
	Sequence  api.Sequence
	Data      []byte
	BlockTime time.Time

	UpdatedAt time.Time // set by storage
}

// BlockMetadata stores a lightweight version of Block
type BlockMetadata struct {
	Tag       uint32
	Height    uint64
	Hash      string
	Sequence  api.Sequence
	BlockTime time.Time

	UpdatedAt time.Time // set by storage
}

func NewBlock(
	tag uint32,
	height uint64,
	hash string,
	sequence api.Sequence,
	blockTime time.Time,
	data []byte,
) *Block {

	return &Block{
		Tag:       tag,
		Height:    height,
		Hash:      hash,
		Sequence:  sequence,
		BlockTime: blockTime,
		Data:      data,
	}
}
