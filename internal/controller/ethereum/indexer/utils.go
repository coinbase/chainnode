package indexer

import (
	"encoding/json"

	"golang.org/x/xerrors"

	"github.com/coinbase/chainnode/internal/api"
	"github.com/coinbase/chainnode/internal/api/ethereum"
	"github.com/coinbase/chainnode/internal/utils/arbitrumutil"
)

type (
	traceByHashResult struct {
		Result json.RawMessage `json:"result"`
	}
)

// parseTraceFromBlock used by trace indexers to get trace from event and block
func parseTraceFromBlock(
	tag uint32,
	event *api.Event,
	block *api.Block,
) (*ethereum.Trace, error) {
	sequence := api.ParseSequenceNum(event.GetSequenceNum())

	blockID := event.GetBlock()
	transactionTraces := block.GetEthereum().GetTransactionTraces()
	blockHeader := block.GetEthereum().GetHeader()
	if len(blockHeader) == 0 {
		return nil, xerrors.Errorf("failed to find block header")
	}

	// parse raw header
	var headerLite headerLiteForTrace
	if err := json.Unmarshal(blockHeader, &headerLite); err != nil {
		return nil, xerrors.Errorf("failed to parse header into headerLiteForTrace: %w", err)
	}

	var data []byte
	var err error
	if arbitrumutil.IsArbitrumAndBeforeNITROUpgrade(block.GetBlockchain(), block.GetNetwork(), event.GetBlock().GetHeight()) {
		rawArbtraces := make([]json.RawMessage, len(transactionTraces))
		for index, trace := range transactionTraces {
			rawArbtraces[index] = trace
		}
		data, err = json.Marshal(rawArbtraces)
		if err != nil {
			return nil, xerrors.Errorf("failed to marshal arbtraces: %w", err)
		}
	} else {
		rawTraces := make([]traceByHashResult, len(transactionTraces))
		for index, trace := range transactionTraces {
			rawTraces[index].Result = trace
		}

		data, err = json.Marshal(rawTraces)
		if err != nil {
			return nil, xerrors.Errorf("failed to marshal traces: %w", err)
		}
	}

	blockTime := headerLite.BlockTime.AsTime()
	return ethereum.NewTrace(
		tag,
		blockID.GetHeight(),
		blockID.GetHash(),
		sequence,
		data,
		blockTime,
	), nil
}
