package jsonutil

import (
	"testing"

	"github.com/coinbase/chainnode/internal/utils/testutil"
)

func TestFormatJSON(t *testing.T) {
	require := testutil.Require(t)
	expected := `{
  "field1": "value1",
  "field2": {
    "field3": "value3",
    "field4": "value4"
  }
}`
	input := map[string]interface{}{
		"field1": "value1",
		"field2": map[string]interface{}{
			"field3": "value3",
			"field4": "value4",
		},
	}

	actual, err := FormatJSON(input)
	require.NoError(err)
	require.Equal(expected, actual)
}

func TestFilterNulls(t *testing.T) {
	tests := []struct {
		name     string
		expected interface{}
		input    interface{}
	}{
		{
			name:     "empty",
			expected: map[string]interface{}{},
			input:    map[string]interface{}{},
		},
		{
			name:     "string",
			expected: "0xa",
			input:    "0xa",
		},
		{
			name: "object",
			expected: map[string]interface{}{
				"field1": "value1",
			},
			input: map[string]interface{}{
				"field1": "value1",
				"field2": nil,
			},
		},
		{
			name: "nestedObject",
			expected: map[string]interface{}{
				"field1": "value1",
				"field2": map[string]interface{}{
					"field4": "value4",
				},
			},
			input: map[string]interface{}{
				"field1": "value1",
				"field2": map[string]interface{}{
					"field3": nil,
					"field4": "value4",
				},
			},
		},
		{
			name: "nestedList",
			expected: map[string]interface{}{
				"field1": "value1",
				"field2": []map[string]interface{}{
					{
						"field4": "value4",
					},
					{
						"field5": "value5",
					},
				},
			},
			input: map[string]interface{}{
				"field1": "value1",
				"field2": []map[string]interface{}{
					{
						"field3": nil,
						"field4": "value4",
					},
					{
						"field5": "value5",
						"field6": nil,
					},
				},
			},
		},
		{
			name: "list",
			expected: []map[string]interface{}{
				{
					"field1": "value1",
					"field2": map[string]interface{}{
						"field4": "value4",
					},
				},
				{
					"field5": "value5",
				},
			},
			input: []map[string]interface{}{
				{
					"field1": "value1",
					"field2": map[string]interface{}{
						"field3": nil,
						"field4": "value4",
					},
				},
				{
					"field5": "value5",
					"field6": nil,
				},
			},
		},
		{
			name: "block",
			expected: map[string]interface{}{
				"id": "1",
				"result": map[string]interface{}{
					"hash": "0x427a2093846d4d9f33fccedd4954281368a436b81df43c17d050e7e2c14ad638",
					"transactions": []map[string]interface{}{
						{
							"to":               "0xa1346592a8b80a600da845638a115fd83e527187",
							"transactionIndex": "0x0",
						},
						{
							"transactionIndex": "0x8e",
						},
					},
				},
			},
			input: map[string]interface{}{
				"id": "1",
				"result": map[string]interface{}{
					"hash": "0x427a2093846d4d9f33fccedd4954281368a436b81df43c17d050e7e2c14ad638",
					"transactions": []map[string]interface{}{
						{
							"to":               "0xa1346592a8b80a600da845638a115fd83e527187",
							"transactionIndex": "0x0",
						},
						{
							"to":               nil,
							"transactionIndex": "0x8e",
						},
					},
				},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			require := testutil.Require(t)
			FilterNulls(test.input)
			require.Equal(test.expected, test.input)
		})
	}
}
