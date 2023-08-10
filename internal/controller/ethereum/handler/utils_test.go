package handler

import (
	"encoding/json"
	"fmt"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"

	"github.com/coinbase/chainnode/internal/clients/blockchain/jsonrpc"
	"github.com/coinbase/chainnode/internal/config"
	"github.com/coinbase/chainnode/internal/utils/fixtures"
	"github.com/coinbase/chainnode/internal/utils/instrument"
	"github.com/coinbase/chainnode/internal/utils/pointer"
	"github.com/coinbase/chainnode/internal/utils/testapp"
	"github.com/coinbase/chainnode/internal/utils/testutil"
)

func TestFilterCriteria(t *testing.T) {
	tests := []struct {
		name     string
		expected string
		input    FilterCriteria
	}{
		{
			name:     "withBlockRange",
			expected: fixtures.MustReadString("controller/ethereum/eth_filter_criteria_with_block_range.json"),
			input: FilterCriteria{
				FromBlock: big.NewInt(100),
				ToBlock:   big.NewInt(101),
			},
		},
		{
			name:     "withBlockHash",
			expected: fixtures.MustReadString("controller/ethereum/eth_filter_criteria_with_block_hash.json"),
			input: FilterCriteria{
				BlockHash: pointer.Hash(common.HexToHash("0x123")),
			},
		},
		{
			name:     "withAll",
			expected: fixtures.MustReadString("controller/ethereum/eth_filter_criteria_with_all.json"),
			input: FilterCriteria{
				FromBlock: big.NewInt(100),
				ToBlock:   big.NewInt(101),
				Addresses: []common.Address{common.HexToAddress("0xabc")},
				Topics:    [][]common.Hash{{common.HexToHash("0x888"), common.HexToHash("0x999")}},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := testutil.Require(t)

			data, err := json.MarshalIndent(test.input, "", "  ")
			require.NoError(err)
			actual := string(data)
			require.Equal(test.expected, actual)

			var output FilterCriteria
			err = json.Unmarshal(data, &output)
			require.NoError(err)
			if len(output.Addresses) == 0 {
				output.Addresses = nil
			}
			if len(output.Topics) == 0 {
				output.Topics = nil
			}
			require.Equal(test.input, output)
		})
	}
}

func TestFormatJSON(t *testing.T) {
	tests := []struct {
		name     string
		expected string
		input    interface{}
		filters  []string
	}{
		{
			name:     "string",
			expected: `"foo"`,
			input:    "foo",
		},
		{
			name:     "stringWithFilters",
			expected: `"foo"`,
			input:    "foo",
			filters:  []string{"foo"},
		},
		{
			name: "array",
			expected: `[
  {
    "foo": "bar"
  },
  {
    "bar": "foo"
  }
]`,
			input: []map[string]string{{"foo": "bar"}, {"bar": "foo"}},
		},
		{
			name: "arrayWithFilters",
			expected: `[
  {
    "foo": "bar"
  },
  {
    "bar": "foo"
  }
]`,
			input:   []map[string]string{{"foo": "bar"}, {"bar": "foo"}},
			filters: []string{"foo"},
		},
		{
			name: "map",
			expected: `{
  "bar": "foo",
  "foo": "bar"
}`,
			input: map[string]string{"foo": "bar", "bar": "foo"},
		},
		{
			name: "mapWithFilters",
			expected: `{
  "bar": "foo"
}`,
			input:   map[string]string{"foo": "bar", "bar": "foo"},
			filters: []string{"foo"},
		},
		{
			name:     "transaction",
			expected: fixtures.MustReadString("controller/ethereum/eth_transaction_3.json"),
			input:    mustReadJSON("controller/ethereum/eth_transaction_3.json"),
		},
		{
			name:     "transactionWithFilters",
			expected: fixtures.MustReadString("controller/ethereum/eth_transaction_3_filtered.json"),
			input:    mustReadJSON("controller/ethereum/eth_transaction_3.json"),
			filters:  getTransactionByHashIgnoredFields,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := testutil.Require(t)

			filteredInput, err := filterFields(test.input, test.filters...)
			require.NoError(err)

			actual, err := formatJSON(filteredInput)
			require.NoError(err)
			require.Equal(test.expected, actual)
		})
	}
}

func TestNullFilter(t *testing.T) {
	require := testutil.Require(t)
	cfg, err := config.New()
	require.NoError(err)
	require.True(nullFilter(cfg, "0x123", nil))
	require.True(nullFilter(cfg, nil, "0x123"))
	require.False(nullFilter(cfg, "0x123", "0xabc"))
}

func TestBlockNumberFilter(t *testing.T) {
	tests := []struct {
		expected   bool
		primary    interface{}
		shadow     interface{}
		asymmetric bool
	}{
		{
			expected: false,
			primary:  nil,
			shadow:   "0xdbbab9",
		},
		{
			expected: false,
			primary:  "",
			shadow:   "0xdbbab9",
		},
		{
			expected: false,
			primary:  "null",
			shadow:   "0xdbbab9",
		},
		{
			expected: false,
			primary:  "0xdbbab9",
			shadow:   "dbbab9",
		},
		{
			expected: false,
			primary:  "0xdbbab9",
			shadow:   "0xdbbab9",
		},
		{
			expected: false,
			primary:  "0xdbbab9",
			shadow:   "0xDBBAB9",
		},
		{
			expected:   false,
			primary:    "0xdbbab3",
			shadow:     "0xdbbab9",
			asymmetric: true,
		},
		{
			expected:   true,
			primary:    "0xdbbab9",
			shadow:     "0xdbbab3",
			asymmetric: true,
		},
		{
			expected: true,
			primary:  "0xdbbab4",
			shadow:   "0xdbbab9",
		},
	}
	for _, test := range tests {
		name := fmt.Sprintf("%v_vs_%v", test.primary, test.shadow)
		t.Run(name, func(t *testing.T) {
			require := testutil.Require(t)

			cfg, err := config.New()
			require.NoError(err)

			// Hardcode the config so that future changes to the config doesn't break the test.
			cfg.Controller.Handler.MaxBlockNumberDiff = 5

			actual := blockNumberFilter(cfg, test.primary, test.shadow)
			require.Equal(test.expected, actual)

			if !test.asymmetric {
				actual = blockNumberFilter(cfg, test.shadow, test.primary)
				require.Equal(test.expected, actual)
			}
		})
	}
}

func TestBlockFilter(t *testing.T) {
	require := testutil.Require(t)

	cfg, err := config.New()
	require.NoError(err)

	block := mustReadJSON("controller/ethereum/eth_header.json")
	require.False(blockFilter(cfg, block, block))

	forkedBlock := mustReadJSON("controller/ethereum/eth_header.json")
	forkedBlock.(map[string]interface{})["hash"] = "0xabc"

	require.True(blockFilter(cfg, block, forkedBlock))
	require.True(blockFilter(cfg, block, nil))
	require.False(noopFilter(cfg, block, forkedBlock))
}

func TestLogsFilter(t *testing.T) {
	require := testutil.Require(t)

	cfg, err := config.New()
	require.NoError(err)

	logs := mustReadJSON("controller/ethereum/eth_logs_1.json")
	require.False(logsFilter(cfg, logs, logs))

	forkedLogs := mustReadJSON("controller/ethereum/eth_logs_1.json")
	forkedLog := forkedLogs.([]interface{})[0]
	forkedLog.(map[string]interface{})["blockHash"] = "0xabc"

	require.True(logsFilter(cfg, logs, forkedLogs))
	require.True(logsFilter(cfg, logs, nil))

	logsPartial := mustReadJSON("controller/ethereum/eth_logs_1_partial.json")
	require.True(logsFilter(cfg, logs, logsPartial))
	require.True(logsFilter(cfg, logsPartial, logs))
}

func TestTraceBlockFilter(t *testing.T) {
	require := testutil.Require(t)

	cfg, err := config.New()
	require.NoError(err)

	trace := mustReadJSON("controller/ethereum/eth_trace.json")
	require.False(traceBlockFilter(cfg, trace, trace))

	emptyArrayTrace := mustReadJSON("controller/ethereum/eth_trace_empty_array.json")
	require.True(traceBlockFilter(cfg, trace, emptyArrayTrace))
	require.True(traceBlockFilter(cfg, emptyArrayTrace, trace))

	require.True(traceBlockFilter(cfg, trace, nil))
	require.True(traceBlockFilter(cfg, nil, trace))

	executionErrorTrace := mustReadJSON("controller/ethereum/eth_trace_execution_timeout.json")
	require.True(traceBlockFilter(cfg, trace, executionErrorTrace))
}

func TestTransactionFilter(t *testing.T) {
	require := testutil.Require(t)

	cfg, err := config.New()
	require.NoError(err)

	transaction := mustReadJSON("controller/ethereum/eth_transaction_1.json")
	require.False(transactionFilter(cfg, transaction, transaction))

	forkedTransaction := mustReadJSON("controller/ethereum/eth_transaction_1.json")
	forkedTransaction.(map[string]interface{})["blockHash"] = "0xabc"

	forkedTransaction2 := mustReadJSON("controller/ethereum/eth_transaction_1.json")
	forkedTransaction2.(map[string]interface{})["blockNumber"] = "0x123"

	require.True(transactionFilter(cfg, transaction, forkedTransaction))
	require.True(transactionFilter(cfg, transaction, forkedTransaction2))
	require.True(transactionFilter(cfg, transaction, nil))
}

func TestTransactionReceiptFilter(t *testing.T) {
	require := testutil.Require(t)

	cfg, err := config.New()
	require.NoError(err)

	receipt := mustReadJSON("controller/ethereum/eth_transactionreceipt_1.json")
	require.False(transactionReceiptFilter(cfg, receipt, receipt))

	forkedReceipt := mustReadJSON("controller/ethereum/eth_transactionreceipt_1.json")
	forkedReceipt.(map[string]interface{})["blockHash"] = "0xabc"

	require.True(transactionReceiptFilter(cfg, receipt, forkedReceipt))
	require.True(transactionReceiptFilter(cfg, receipt, nil))
}

func TestIsBatch(t *testing.T) {

	tests := []struct {
		name     string
		expected bool
		input    string
	}{
		{
			name:     "null",
			expected: false,
			input:    "null",
		},
		{
			name:     "string",
			expected: false,
			input:    `"0xc0de"`,
		},
		{
			name:     "object",
			expected: false,
			input:    `{"foo": bar}`,
		},
		{
			name:     "array",
			expected: true,
			input:    `[{"foo": bar}]`,
		},
		{
			name:     "arrayWithWhitespaces",
			expected: true,
			input:    `  [ { "foo" : bar } ]  `,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := testutil.Require(t)
			actual := isBatch(json.RawMessage(test.input))
			require.Equal(test.expected, actual)
		})
	}
}

func TestFilterError(t *testing.T) {
	tests := []struct {
		name     string
		expected bool
		filterFn instrument.FilterFn
		input    error
	}{
		{
			name:     "filterProxyError_Generic",
			expected: true,
			filterFn: filterProxyError,
			input:    jsonrpc.NewRPCError(errorCodeGeneric, ""),
		},
		{
			name:     "filterProxyError_BadRequest",
			expected: true,
			filterFn: filterProxyError,
			input:    jsonrpc.NewRPCError(errorCodeBadRequest, ""),
		},
		{
			name:     "filterProxyError_Canceled",
			expected: true,
			filterFn: filterProxyError,
			input:    jsonrpc.NewRPCError(errorCodeCanceled, ""),
		},
		{
			name:     "filterProxyError_Unknown",
			expected: true,
			filterFn: filterProxyError,
			input:    jsonrpc.NewRPCError(3, ""), // Sometimes eth_call returns an undefined error code.
		},
		{
			name:     "filterProxyError_Internal",
			expected: false,
			filterFn: filterProxyError,
			input:    jsonrpc.NewRPCError(errorCodeInternal, ""),
		},
		{
			name:     "filterClientError_Generic",
			expected: false,
			filterFn: filterClientError,
			input:    jsonrpc.NewRPCError(errorCodeGeneric, ""),
		},
		{
			name:     "filterClientError_BadRequest",
			expected: true,
			filterFn: filterClientError,
			input:    jsonrpc.NewRPCError(errorCodeBadRequest, ""),
		},
		{
			name:     "filterClientError_Canceled",
			expected: true,
			filterFn: filterClientError,
			input:    jsonrpc.NewRPCError(errorCodeCanceled, ""),
		},
		{
			name:     "filterClientError_Internal",
			expected: false,
			filterFn: filterClientError,
			input:    jsonrpc.NewRPCError(errorCodeInternal, ""),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := testutil.Require(t)
			actual := test.filterFn(test.input)
			require.Equal(test.expected, actual)
		})
	}
}

func TestGetChainID(t *testing.T) {
	testapp.TestAllConfigs(t, func(t *testing.T, cfg *config.Config) {
		require := testutil.Require(t)
		chainID, err := getChainID(cfg.Network())
		require.NoError(err)
		require.Greater(chainID, 0)
	})
}

func mustReadJSON(fixture string) interface{} {
	data := fixtures.MustReadFile(fixture)

	var output interface{}
	err := json.Unmarshal(data, &output)
	if err != nil {
		panic(err)
	}

	return output
}
