package ethereum

import (
	"time"

	"github.com/coinbase/chainnode/internal/api"
)

type Transaction struct {
	Tag       uint32
	Hash      string
	Sequence  api.Sequence
	Data      []byte
	BlockTime time.Time

	UpdatedAt time.Time // set by storage
}

func NewTransaction(
	tag uint32,
	hash string,
	sequence api.Sequence,
	data []byte,
	blockTime time.Time,
) *Transaction {

	return &Transaction{
		Tag:       tag,
		Hash:      hash,
		Sequence:  sequence,
		Data:      data,
		BlockTime: blockTime,
	}
}
