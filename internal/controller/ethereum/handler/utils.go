package handler

import (
	"context"
	"encoding/json"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/eth/filters"
	"golang.org/x/xerrors"

	"github.com/coinbase/chainnode/internal/api"
	"github.com/coinbase/chainnode/internal/api/ethereum"
	"github.com/coinbase/chainnode/internal/clients/blockchain/jsonrpc"
	"github.com/coinbase/chainnode/internal/config"
	"github.com/coinbase/chainnode/internal/server/rpc"
	"github.com/coinbase/chainnode/internal/storage"
	"github.com/coinbase/chainnode/internal/utils/jsonutil"

	c3common "github.com/coinbase/chainstorage/protos/coinbase/c3/common"
)

type (
	// FilterCriteria represents a request to create a new filter.
	// Same as filters.FilterCriteria but with MarshalJSON/UnmarshalJSON methods.
	FilterCriteria filters.FilterCriteria

	filterCriteriaMarshaler struct {
		BlockHash *common.Hash     `json:"blockHash,omitempty"`
		FromBlock string           `json:"fromBlock,omitempty"`
		ToBlock   string           `json:"toBlock,omitempty"`
		Addresses []common.Address `json:"address,omitempty"`
		Topics    [][]common.Hash  `json:"topics,omitempty"`
	}
)

var (
	_ json.Marshaler   = (*FilterCriteria)(nil)
	_ json.Unmarshaler = (*FilterCriteria)(nil)

	// See https://chainlist.org/ for the complete list.
	evmChainIDs = map[c3common.Network]int{
		c3common.Network_NETWORK_ETHEREUM_MAINNET: 1,
		c3common.Network_NETWORK_ETHEREUM_GOERLI:  5,
		c3common.Network_NETWORK_POLYGON_MAINNET:  137,
	}

	getTransactionByHashIgnoredFields = []string{
		"accessList",
		"chainId",
		"maxFeePerGas",
		"maxPriorityFeePerGas",
	}

	getTransactionReceiptIgnoredFields = []string{
		"effectiveGasPrice",
	}
)

const (
	traceExecutionTimeoutError = "execution timeout"
)

func (c FilterCriteria) MarshalJSON() ([]byte, error) {
	v := &filterCriteriaMarshaler{
		BlockHash: c.BlockHash,
		FromBlock: encodeBigInt(c.FromBlock),
		ToBlock:   encodeBigInt(c.ToBlock),
		Addresses: c.Addresses,
		Topics:    c.Topics,
	}
	return json.Marshal(&v)
}

func (c *FilterCriteria) UnmarshalJSON(data []byte) error {
	return (*filters.FilterCriteria)(c).UnmarshalJSON(data)
}

func encodeBigInt(v *big.Int) string {
	if v == nil {
		return ""
	}

	return hexutil.EncodeUint64(v.Uint64())
}

func formatJSON(input interface{}) (string, error) {
	return jsonutil.FormatJSON(input)
}

func filterFields(input interface{}, ignoredFields ...string) (interface{}, error) {
	data, err := json.Marshal(input)
	if err != nil {
		return nil, err
	}

	var output interface{}
	if err := json.Unmarshal(data, &output); err != nil {
		return nil, err
	}

	jsonutil.FilterNulls(output)

	switch v := output.(type) {
	case map[string]interface{}:
		for _, field := range ignoredFields {
			delete(v, field)
		}
	}

	return output, nil
}

// shortenString shortens the input so that the log is less likely getting discarded by Datadog.
func shortenString(s string, maxLength int) string {
	if len(s) <= maxLength {
		return s
	}

	return s[:maxLength] + "..."
}

func noopFilter(_ *config.Config, _ interface{}, _ interface{}) bool {
	return false
}

func nullFilter(cfg *config.Config, primaryOutput interface{}, shadowOutput interface{}) bool {
	return primaryOutput == nil || shadowOutput == nil
}

func blockNumberFilter(cfg *config.Config, primaryOutput interface{}, shadowOutput interface{}) bool {
	primaryValue, ok := primaryOutput.(string)
	if !ok {
		return false
	}

	primaryBlockNumber, err := hexutil.DecodeUint64(primaryValue)
	if err != nil {
		return false
	}

	shadowValue, ok := shadowOutput.(string)
	if !ok {
		return false
	}

	shadowBlockNumber, err := hexutil.DecodeUint64(shadowValue)
	if err != nil {
		return false
	}

	if primaryBlockNumber == shadowBlockNumber {
		return false
	}

	if primaryBlockNumber > shadowBlockNumber {
		// If ChainNode is ahead of shadow endpoint, do not report as unmatched.
		return true
	}

	diff := shadowBlockNumber - primaryBlockNumber
	return diff <= uint64(cfg.Controller.Handler.MaxBlockNumberDiff)
}

func defaultPreprocessor(cfg *config.Config, input interface{}) (interface{}, error) {
	return filterFields(input)
}

func traceBlockByHashPreprocessor(cfg *config.Config, input interface{}) (interface{}, error) {
	if IsOptimism(cfg.Blockchain()) {
		// Filter `time` field for Optimism only, because this field varies among the node providers.
		// `Time` field holds a node provider specific value (most probably a timing calculation in runtime).
		// When doing the comparison between ChainNode and the validator, we always run into a
		// mismatch because of the `time` field, hence, "parity unmatched" gets published on datadog.
		// This preprocessor alleviates the problem by filtering that field from the traces object.
		inputFiltered, err := filterFields(input)
		if err != nil {
			return nil, xerrors.Errorf("failed to filter fields: %w", err)
		}

		tracesResponse, ok := inputFiltered.([]interface{})
		if !ok {
			return nil, xerrors.Errorf("failed to convert input=%+v to []interface{}", input)
		}

		for i := range tracesResponse {
			tracesResult, ok := tracesResponse[i].(map[string]interface{})
			if !ok {
				return nil, xerrors.Errorf("failed to convert traces result to map[string]interface{}=%+v", tracesResponse[i])
			}

			traces, ok := tracesResult["result"].(map[string]interface{})
			if !ok {
				return nil, xerrors.Errorf("failed to convert traces to map[string]interface{}=%+v", tracesResult)
			}

			filteredTraces, err := filterFields(traces, "time")
			if err != nil {
				return nil, xerrors.Errorf("failed to filter time field: %w", err)
			}

			tracesResult["result"] = filteredTraces
			tracesResponse[i] = tracesResult
		}

		return tracesResponse, nil
	} else if IsEthereum(cfg.Blockchain()) {
		// Filter `txHash` field for Ethereum only, `txHash` has been added to debug_traceBlockByHash endpoint in the
		//  latest geth client(v1.12.0), see this commit: https://github.com/ethereum/go-ethereum/pull/27183
		inputFiltered, err := filterFields(input)
		if err != nil {
			return nil, xerrors.Errorf("failed to filter fields: %w", err)
		}

		tracesResponse, ok := inputFiltered.([]interface{})
		if !ok {
			return nil, xerrors.Errorf("failed to convert input=%+v to []interface{}", input)
		}

		for i := range tracesResponse {
			tracesResult, ok := tracesResponse[i].(map[string]interface{})
			if !ok {
				return nil, xerrors.Errorf("failed to convert traces result to map[string]interface{}=%+v", tracesResponse[i])
			}

			filteredTraces, err := filterFields(tracesResult, "txHash")
			if err != nil {
				return nil, xerrors.Errorf("failed to filter txHash field: %w", err)
			}

			tracesResponse[i] = filteredTraces
		}

		return tracesResponse, nil
	} else if IsPolygon(cfg.Blockchain()) {
		inputFiltered, err := filterFields(input)
		if err != nil {
			return nil, xerrors.Errorf("failed to filter fields: %w", err)
		}

		tracesResponse, ok := inputFiltered.([]interface{})
		if !ok {
			return nil, xerrors.Errorf("failed to convert input=%+v to []interface{}", input)
		}

		for i := range tracesResponse {
			tracesResult, ok := tracesResponse[i].(map[string]interface{})
			if !ok {
				return nil, xerrors.Errorf("failed to convert traces result to map[string]interface{}=%+v", tracesResponse[i])
			}

			traces, ok := tracesResult["result"].(map[string]interface{})
			if !ok {
				return nil, xerrors.Errorf("failed to convert traces to map[string]interface{}=%+v", tracesResult)
			}

			filteredTraces, err := filterFields(traces, "gasUsed")
			if err != nil {
				return nil, xerrors.Errorf("failed to filter gasUsed field: %w", err)
			}

			tracesResult["result"] = filteredTraces
			tracesResponse[i] = tracesResult
		}

		return tracesResponse, nil
	} else {
		return defaultPreprocessor(cfg, input)
	}
}

func blockPreprocessor(cfg *config.Config, input interface{}) (interface{}, error) {
	input, err := filterFields(input)
	if err != nil {
		return nil, xerrors.Errorf("failed to filter fields: %w", err)
	}

	block, ok := input.(map[string]interface{})
	if !ok {
		return nil, xerrors.Errorf("failed to convert input=%+v to map[string]interface{}", input)
	}

	inputTxs, ok := block["transactions"]
	if !ok {
		return nil, xerrors.Errorf("missing transactions in block=(%+v)", block)
	}

	txs, ok := inputTxs.([]interface{})
	if !ok {
		return nil, xerrors.New("failed to convert inputTxs to []interface{}")
	}

	filteredTxs := make([]interface{}, len(txs))
	for i, d := range txs {
		tx, ok := d.(map[string]interface{})
		if !ok {
			// transactions could be a list of strings, skip filtering if cannot convert to map[string]interface{}
			return block, nil
		}

		filteredTxs[i], err = filterFields(tx, "chainId")
		if err != nil {
			return nil, xerrors.Errorf("failed to filter chainId field: %w", err)
		}
	}
	block["transactions"] = filteredTxs

	return block, nil
}

func blockFilter(cfg *config.Config, primaryOutput interface{}, shadowOutput interface{}) bool {
	if nullFilter(cfg, primaryOutput, shadowOutput) {
		return true
	}

	primaryBlock, ok := primaryOutput.(map[string]interface{})
	if !ok {
		return false
	}

	shadowBlock, ok := shadowOutput.(map[string]interface{})
	if !ok {
		return false
	}

	if primaryBlock["number"] != shadowBlock["number"] {
		return false
	}

	// If the block hash is different, the outputs are from different forks,
	// and the comparison should be skipped.
	return primaryBlock["hash"] != shadowBlock["hash"]
}

func logsFilter(cfg *config.Config, primaryOutput interface{}, shadowOutput interface{}) bool {
	if nullFilter(cfg, primaryOutput, shadowOutput) {
		return true
	}

	primaryLogs, ok := primaryOutput.([]interface{})
	if !ok {
		return false
	}

	shadowLogs, ok := shadowOutput.([]interface{})
	if !ok {
		return false
	}

	if len(primaryLogs) == 0 || len(shadowLogs) == 0 {
		return true
	}

	primaryLog, ok := primaryLogs[0].(map[string]interface{})
	if !ok {
		return false
	}

	shadowLog := shadowLogs[0].(map[string]interface{})
	if !ok {
		return false
	}

	if primaryLog["blockNumber"] != shadowLog["blockNumber"] {
		return false
	}

	// If the block hashes are different, the outputs are from different forks,
	// and the comparison should be skipped.
	if primaryLog["blockHash"] != shadowLog["blockHash"] {
		return true
	}

	// Output of eth_getLogs from shadow sometimes miss some logs or contain more logs
	// as the latest block numbers in shadow and primary may be different.
	// Usually transient issues and should be ignored.
	return isProperSubset(shadowLogs, primaryLogs) || isProperSubset(primaryLogs, shadowLogs)
}

// isProperSubset checks if logs1 is a proper subset of logs2
func isProperSubset(logs1, logs2 []interface{}) bool {
	if len(logs2) <= len(logs1) {
		return false
	}

	// blockLogsMap2 stores a blockHash:LogIndex:true mapping of logs2
	blockLogsMap2 := make(map[interface{}]map[interface{}]bool)
	for _, log2 := range logs2 {
		logMap2, ok := log2.(map[string]interface{})
		if !ok {
			return false
		}
		blockHash := logMap2["blockHash"]
		logIndex := logMap2["logIndex"]
		if _, ok := blockLogsMap2[blockHash]; !ok {
			blockLogsMap2[blockHash] = make(map[interface{}]bool)
		}
		blockLogsMap2[blockHash][logIndex] = true
	}

	// check if every log in logs1 has a match in logs2
	for _, log1 := range logs1 {
		logMap1, ok := log1.(map[string]interface{})
		if !ok {
			return false
		}
		blockHash := logMap1["blockHash"]
		logIndex := logMap1["logIndex"]
		logsMap2, ok := blockLogsMap2[blockHash]
		if !ok {
			return false
		}
		if _, ok = logsMap2[logIndex]; !ok {
			return false
		}
	}

	return true
}

func transactionByHashPreprocessor(cfg *config.Config, input interface{}) (interface{}, error) {
	return filterFields(input, getTransactionByHashIgnoredFields...)
}

func transactionFilter(cfg *config.Config, primaryOutput interface{}, shadowOutput interface{}) bool {
	if nullFilter(cfg, primaryOutput, shadowOutput) {
		return true
	}

	primaryTransaction, ok := primaryOutput.(map[string]interface{})
	if !ok {
		return false
	}

	shadowTransaction, ok := shadowOutput.(map[string]interface{})
	if !ok {
		return false
	}

	// If the block number or hash is different, the outputs are from different forks,
	// and the comparison should be skipped.
	return primaryTransaction["blockNumber"] != shadowTransaction["blockNumber"] ||
		primaryTransaction["blockHash"] != shadowTransaction["blockHash"]
}

func transactionReceiptPreprocessor(cfg *config.Config, input interface{}) (interface{}, error) {
	return filterFields(input, getTransactionReceiptIgnoredFields...)
}

func transactionReceiptFilter(cfg *config.Config, primaryOutput interface{}, shadowOutput interface{}) bool {
	return transactionFilter(cfg, primaryOutput, shadowOutput)
}

func traceBlockFilter(cfg *config.Config, primaryOutput interface{}, shadowOutput interface{}) bool {
	if nullFilter(cfg, primaryOutput, shadowOutput) {
		return true
	}

	primaryTrace, ok := primaryOutput.([]interface{})
	if !ok {
		return false
	}

	shadowTrace, ok := shadowOutput.([]interface{})
	if !ok {
		return false
	}

	if len(primaryTrace) == 0 || len(shadowTrace) == 0 {
		return true
	}

	if len(primaryTrace) != len(shadowTrace) {
		return false
	}

	for i := range shadowTrace {
		shadowTraceMap, ok := shadowTrace[i].(map[string]interface{})
		if !ok {
			return false
		}

		errorMsg, ok := shadowTraceMap["error"]
		if ok && errorMsg == traceExecutionTimeoutError {
			return true
		}
	}

	return false
}

// isBatch returns true when the first non-whitespace characters is '['
// Ref: https://github.com/ethereum/go-ethereum/blob/5bcbb2980be0c565088e8cdabddb0fb2d08e1fad/rpc/json.go#L272
func isBatch(request json.RawMessage) bool {
	for _, c := range request {
		// skip insignificant whitespace (http://www.ietf.org/rfc/rfc4627.txt)
		if c == 0x20 || c == 0x09 || c == 0x0a || c == 0x0d {
			continue
		}
		return c == '['
	}
	return false
}

func isCanceledError(err error) bool {
	return xerrors.Is(err, context.Canceled) || xerrors.Is(err, storage.ErrRequestCanceled)
}

func isNotImplementedError(err error) bool {
	return xerrors.Is(err, api.ErrNotImplemented)
}

func isNotAllowedError(err error) bool {
	return xerrors.Is(err, api.ErrNotAllowed)
}

func filterProxyError(err error) bool {
	var errRPC *jsonrpc.RPCError
	return xerrors.As(err, &errRPC) && errRPC.Code != errorCodeInternal
}

func filterClientError(err error) bool {
	var errRPC *jsonrpc.RPCError
	if xerrors.As(err, &errRPC) {
		switch errRPC.Code {
		case errorCodeBadRequest, errorCodeCanceled, errorCodeNotSupported:
			return true
		}
	}

	return false
}

func getChainID(network c3common.Network) (int, error) {
	v, ok := evmChainIDs[network]
	if !ok {
		return 0, xerrors.Errorf("unknown network %v: %w", network.GetName(), api.ErrNotImplemented)
	}

	return v, nil
}

// Ref: https://github.com/ethereum/go-ethereum/blob/d9566e39bd1b05325cfa08c832488f0e04feacd3/eth/filters/filter.go#L351
func bloomFilter(bloom types.Bloom, addresses []common.Address, topics [][]common.Hash) bool {
	if len(addresses) > 0 {
		var included bool
		for _, addr := range addresses {
			if types.BloomLookup(bloom, addr) {
				included = true
				break
			}
		}
		if !included {
			return false
		}
	}

	for _, sub := range topics {
		included := len(sub) == 0 // empty rule set == wildcard
		for _, topic := range sub {
			if types.BloomLookup(bloom, topic) {
				included = true
				break
			}
		}
		if !included {
			return false
		}
	}
	return true
}

func shouldProxyMethod(blockNumber rpc.BlockNumber) (bool, error) {
	if blockNumber < 0 && blockNumber != rpc.PendingBlockNumber && blockNumber != rpc.LatestBlockNumber {
		return false, xerrors.Errorf("tagged blockNumber=%+v is not implemented: %w", blockNumber, api.ErrNotImplemented)
	} else if blockNumber == rpc.PendingBlockNumber {
		return true, nil
	}

	return false, nil
}

func getBlockTransactionByIndex(block *ethereum.Block, index rpc.DecimalOrHex) (json.RawMessage, error) {
	var blockData blockLiteTransactions
	err := json.Unmarshal(block.Data, &blockData)
	if err != nil {
		return nil, xerrors.Errorf("failed to unmarshal block.data: %w", err)
	}

	if uint64(index) >= uint64(len(blockData.Transactions)) {
		return nil, nil
	}

	return blockData.Transactions[index], nil
}

func getBlockTransactionCount(block *ethereum.Block) (json.RawMessage, error) {
	var blockData blockLiteTransactions
	err := json.Unmarshal(block.Data, &blockData)
	if err != nil {
		return nil, xerrors.Errorf("failed to unmarshal block.data: %w", err)
	}

	result, err := json.Marshal(hexutil.EncodeUint64(uint64(len(blockData.Transactions))))
	if err != nil {
		return nil, xerrors.Errorf("failed to marshal numTransactionsInBlock: %w", err)
	}

	return result, nil
}

func getBlockUncleCount(block *ethereum.Block) (json.RawMessage, error) {
	var blockData blockLiteUncles
	err := json.Unmarshal(block.Data, &blockData)
	if err != nil {
		return nil, xerrors.Errorf("failed to unmarshal block.data: %w", err)
	}

	result, err := json.Marshal(hexutil.EncodeUint64(uint64(len(blockData.Uncles))))
	if err != nil {
		return nil, xerrors.Errorf("failed to marshal numUnclesInBlock: %w", err)
	}

	return result, nil
}

func IsOptimism(blockchain c3common.Blockchain) bool {
	return blockchain == c3common.Blockchain_BLOCKCHAIN_OPTIMISM
}

func IsEthereum(blockchain c3common.Blockchain) bool {
	return blockchain == c3common.Blockchain_BLOCKCHAIN_ETHEREUM
}

func IsPolygon(blockchain c3common.Blockchain) bool {
	return blockchain == c3common.Blockchain_BLOCKCHAIN_POLYGON
}
