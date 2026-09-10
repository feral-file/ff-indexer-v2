package types_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/feral-file/ff-indexer-v2/internal/types"
)

func TestIsBrowserIPFSGateway(t *testing.T) {
	for _, source := range []string{
		"https://ipfs.io/ipfs/example", "https://dweb.link/", "https://inbrowser.link/",
		"https://bafyexample.ipfs.inbrowser.link/", "https://IPFS.IO./ipfs/example",
	} {
		require.True(t, types.IsBrowserIPFSGateway(source), source)
	}
	for _, source := range []string{
		"https://ipfs.feralfile.com/ipfs/example", "https://ipfs.filebase.io/ipfs/example",
		"https://notipfs.io/", "https://ipfs.io.example/", "https://ipfs.io@example.com/",
		"https://user@ipfs.io/", "ipfs://example", "file://ipfs.io/", "://",
	} {
		require.False(t, types.IsBrowserIPFSGateway(source), source)
	}
}

func TestIsIPFSGatewayURL_SubdomainPreservesReference(t *testing.T) {
	const cid = "bafybeidhq4d52l3kozhe5upfl2bpu5smjcvrg7g3unf2hpzjvjfbb3wp3y"
	for _, suffix := range []string{"", "/", "/art%20work/index.html?seed=a&seed=b#view", "?seed=1#view"} {
		ok, reference := types.IsIPFSGatewayURL("https://" + cid + ".ipfs.inbrowser.link" + suffix)
		require.True(t, ok)
		require.Equal(t, cid+suffix, reference)
	}
}
