package adapter

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestEndpointForLogs pins that the URL kept for log context never carries the
// provider API key, which Infura and Chainstack put in the path.
func TestEndpointForLogs(t *testing.T) {
	t.Parallel()
	require.Equal(t, "wss://mainnet.infura.io", EndpointForLogs("wss://mainnet.infura.io/ws/v3/0123456789abcdef"))
	require.Equal(t, "https://ethereum-mainnet.core.chainstack.com", EndpointForLogs("https://ethereum-mainnet.core.chainstack.com/abcdef0123456789?x=1"))
	require.Equal(t, "<redacted>", EndpointForLogs("not a url"))
	require.Equal(t, "<redacted>", EndpointForLogs(""))
}
