package endpoints

import (
	"testing"

	"github.com/coinbase/chainnode/internal/utils/testutil"
)

func TestHTTPClient_Default(t *testing.T) {
	require := testutil.Require(t)

	client, err := newHTTPClient()
	require.NoError(err)
	require.NotEmpty(client.Timeout)
	require.Nil(client.Jar)
}
