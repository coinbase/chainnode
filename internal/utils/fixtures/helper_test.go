package fixtures

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGetJson(t *testing.T) {
	_, err := ReadFile("controller/ethereum/eth_logs_1.json")
	require.NoError(t, err)
}
