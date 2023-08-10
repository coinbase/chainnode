package ethereum

import (
	"time"

	"github.com/coinbase/chainnode/internal/api"
)

type Logs struct {
	Tag       uint32
	Height    uint64
	Hash      string
	Sequence  api.Sequence
	LogsBloom string
	Data      []byte
	BlockTime time.Time

	UpdatedAt time.Time // set by storage
}

// LogsLite only contains partial data
type LogsLite struct {
	Tag       uint32
	Height    uint64
	Sequence  api.Sequence
	LogsBloom string
}

func NewLogs(
	tag uint32,
	height uint64,
	hash string,
	sequence api.Sequence,
	logsBloom string,
	data []byte,
	blockTime time.Time,
) *Logs {
	return &Logs{
		Tag:       tag,
		Height:    height,
		Hash:      hash,
		Sequence:  sequence,
		LogsBloom: logsBloom,
		Data:      data,
		BlockTime: blockTime,
	}
}

func NewLogsLite(
	tag uint32,
	height uint64,
	sequence api.Sequence,
	logsBloom string,
) *LogsLite {
	return &LogsLite{
		Tag:       tag,
		Height:    height,
		Sequence:  sequence,
		LogsBloom: logsBloom,
	}
}
