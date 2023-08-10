package ethereum

import (
	"time"

	"github.com/coinbase/chainnode/internal/api"
)

type Trace struct {
	Tag       uint32
	Height    uint64
	Hash      string
	Sequence  api.Sequence
	Data      []byte
	BlockTime time.Time

	UpdatedAt time.Time // set by storage
}

func NewTrace(
	tag uint32,
	height uint64,
	hash string,
	sequence api.Sequence,
	data []byte,
	blockTime time.Time,
) *Trace {
	return &Trace{
		Tag:       tag,
		Height:    height,
		Hash:      hash,
		Sequence:  sequence,
		Data:      data,
		BlockTime: blockTime,
	}
}
