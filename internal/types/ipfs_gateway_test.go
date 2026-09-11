package types_test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/feral-file/ff-indexer-v2/internal/types"
)

func TestIsBrowserIPFSGateway(t *testing.T) {
	for _, source := range []string{
		"https://ipfs.io/ipfs/example", "https://dweb.link/", "https://inbrowser.link/",
		"https://bafyexample.ipfs.inbrowser.link/", "https://IPFS.IO./ipfs/example",
		"https://bafyexample.ipfs.inbrowser.link./", "https://bafyexample.IPFS.InBrowser.Link.:8443/",
		"https://user@ipfs.io/", "https://user:fixture@IPFS.IO.:8443/ipns/example.org",
		"https://user@bafyexample.ipfs.inbrowser.link/",
	} {
		require.True(t, types.IsBrowserIPFSGateway(source), source)
	}
	for _, source := range []string{
		"https://ipfs.feralfile.com/ipfs/example", "https://ipfs.filebase.io/ipfs/example",
		"https://notipfs.io/", "https://ipfs.io.example/", "https://ipfs.io@example.com/",
		"https://user@ipfs.io.example/", "ipfs://example", "file://ipfs.io/", "://",
	} {
		require.False(t, types.IsBrowserIPFSGateway(source), source)
	}
}

func TestIsIPFSGatewayURL_SubdomainPreservesReference(t *testing.T) {
	const cid = "bafybeidhq4d52l3kozhe5upfl2bpu5smjcvrg7g3unf2hpzjvjfbb3wp3y"
	for _, suffix := range []string{"", "/", "/Art%2fwork/index.html?seed=AbC&seed=b#VIEW", "?seed=1#view", "?", "#"} {
		for _, host := range []string{
			cid + ".ipfs.inbrowser.link", cid + ".IPFS.InBrowser.Link",
			strings.ToUpper(cid) + ".IpFs.InBrowser.Link:8443",
			cid + ".ipfs.inbrowser.link.", strings.ToUpper(cid) + ".IpFs.InBrowser.Link.:8443",
		} {
			ok, reference := types.IsIPFSGatewayURL("https://" + host + suffix)
			require.True(t, ok, host+suffix)
			require.Equal(t, cid+suffix, reference, host+suffix)
		}
	}
}
