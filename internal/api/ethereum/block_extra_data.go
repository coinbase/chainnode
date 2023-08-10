package ethereum

import (
	"time"

	"github.com/coinbase/chainnode/internal/api"
)

type BlockExtraData struct {
	Tag       uint32
	Height    uint64
	Hash      string
	Sequence  api.Sequence
	Data      []byte
	BlockTime time.Time

	UpdatedAt time.Time // set by storage
}

func NewBlockExtraData(
	tag uint32,
	height uint64,
	hash string,
	sequence api.Sequence,
	blockTime time.Time,
	data []byte,
) *BlockExtraData {
	return &BlockExtraData{
		Tag:       tag,
		Height:    height,
		Hash:      hash,
		Sequence:  sequence,
		BlockTime: blockTime,
		Data:      data,
	}
}
