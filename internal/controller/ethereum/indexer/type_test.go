package indexer

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/coinbase/chainnode/internal/utils/testutil"
)

func TestMarshalEthereumQuantity(t *testing.T) {
	require := testutil.Require(t)
	type Envelope struct {
		Gas EthereumQuantity
	}

	envelope := Envelope{
		Gas: 0x123,
	}
	data, err := json.Marshal(envelope)
	require.NoError(err)
	require.Equal(`{"Gas":"0x123"}`, string(data))
}

func TestParseEthereumQuantity(t *testing.T) {
	type Envelope struct {
		Gas EthereumQuantity
	}

	tests := []struct {
		name     string
		expected uint64
		input    string
	}{
		{
			name:     "happy",
			expected: 0x1ed3,
			input:    "0x1ed3",
		},
		{
			name:     "upperCase",
			expected: 0x1ed3,
			input:    "0x1ED3",
		},
		{
			name:     "mixedCase",
			expected: 0x1ed3,
			input:    "0x1Ed3",
		},
		{
			name:     "empty",
			expected: 0,
			input:    "",
		},
		{
			name:     "zero",
			expected: 0,
			input:    "0x0",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := testutil.Require(t)

			data := fmt.Sprintf(`{"gas": "%v"}`, test.input)
			var envelope Envelope
			err := json.Unmarshal([]byte(data), &envelope)
			require.NoError(err)
			require.Equal(test.expected, envelope.Gas.Value())
		})
	}
}

func TestParseEthereumQuantity_InvalidInput(t *testing.T) {
	type Envelope struct {
		Gas EthereumQuantity
	}

	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "without0x",
			input: `"1234"`,
		},
		{
			name:  "nan",
			input: `"0x12GF"`,
		},
		{
			name:  "negative",
			input: `"-0x1234"`,
		},
		{
			name:  "overflow",
			input: `"0x12345678123456789"`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := testutil.Require(t)

			data := fmt.Sprintf(`{"gas": %v}`, test.input)
			var envelope Envelope
			err := json.Unmarshal([]byte(data), &envelope)
			require.Error(err, envelope.Gas.Value())
		})
	}
}

func TestMarshalEthereumHexString(t *testing.T) {
	require := testutil.Require(t)
	type Envelope struct {
		Hash EthereumHexString
	}

	envelope := Envelope{
		Hash: "0x123",
	}
	data, err := json.Marshal(envelope)
	require.NoError(err)
	require.Equal(`{"Hash":"0x123"}`, string(data))
}

func TestParseEthereumHexString(t *testing.T) {
	type Envelope struct {
		Hash EthereumHexString
	}

	tests := []struct {
		name     string
		expected string
		input    string
		err      bool
	}{
		{
			name:     "happy",
			expected: "0xf5365847bff6e48d0c6bc23eee276343d2987efd9876c3c1bf597225e3d69991",
			input:    "0xf5365847bff6e48d0c6bc23eee276343d2987efd9876c3c1bf597225e3d69991",
		},
		{
			name:     "upperCase",
			expected: "0xf5365847bff6e48d0c6bc23eee276343d2987efd9876c3c1bf597225e3d69991",
			input:    "0XF5365847BFF6E48D0C6BC23EEE276343D2987EFD9876C3C1BF597225E3D69991",
		},
		{
			name:     "mixedCase",
			expected: "0xf5365847bff6e48d0c6bc23eee276343d2987efd9876c3c1bf597225e3d69991",
			input:    "0xF5365847BFF6e48d0c6bc23eee276343d2987efd9876c3c1bf597225e3d69991",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := testutil.Require(t)

			data := fmt.Sprintf(`{"hash": "%v"}`, test.input)
			var envelope Envelope
			err := json.Unmarshal([]byte(data), &envelope)
			require.NoError(err)
			require.Equal(test.expected, envelope.Hash.Value())
		})
	}
}

func TestParseEthereumHexString_InvalidInput(t *testing.T) {
	require := testutil.Require(t)

	type Envelope struct {
		Hash EthereumHexString
	}

	data := `{"hash": true}`
	var envelope Envelope
	err := json.Unmarshal([]byte(data), &envelope)
	require.Error(err)
}

func TestEthereumQuantityAsTime(t *testing.T) {
	type Envelope struct {
		Timestamp EthereumQuantity
	}

	tests := []struct {
		name     string
		expected time.Time
		data     string
		err      bool
	}{
		{
			name:     "happy",
			expected: testutil.MustTime("2020-11-24T16:07:21Z"),
			data:     `{"timestamp": "0x5fbd2fb9"}`,
			err:      false,
		},
		{
			name:     "empty",
			expected: time.Time{}.UTC(),
			data:     "{}",
			err:      false,
		},
		{
			name:     "zero",
			expected: time.Time{}.UTC(),
			data:     `{"timestamp": "0x0"}`,
			err:      false,
		},
		{
			name:     "invalid",
			expected: time.Time{},
			data:     `{"timestamp": "invalid"}`,
			err:      true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := testutil.Require(t)

			var envelope Envelope
			err := json.Unmarshal([]byte(test.data), &envelope)
			if test.err {
				require.Error(err)
			} else {
				require.NoError(err)
				require.Equal(test.expected, envelope.Timestamp.AsTime())
			}
		})
	}
}
