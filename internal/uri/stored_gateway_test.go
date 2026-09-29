package uri

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestPreferStoredGatewayURL verifies when a stored gateway URL is kept over the
// freshly resolved one.
func TestPreferStoredGatewayURL(t *testing.T) {
	const cid = "bafybeihsfdyjhwuue3unor2n5yywnw74sp2lk5d7xlireo2ejxldx4jsf4"
	const otherCID = "bafybeiabfhzmgp5n7wgud5dcvk7hizj42bht2tgx5437o2jvc2fv3uizwq"
	resolved := "https://gateway-b.example.com/ipfs/" + cid
	ptr := func(s string) *string { return &s }

	tests := []struct {
		name     string
		resolved string
		stored   *string
		want     string
	}{
		{
			name:     "same content on another gateway keeps the stored URL",
			resolved: resolved,
			stored:   ptr("https://gateway-a.example.com/ipfs/" + cid),
			want:     "https://gateway-a.example.com/ipfs/" + cid,
		},
		{
			name:     "same content in subdomain form keeps the stored URL",
			resolved: resolved,
			stored:   ptr("https://" + cid + ".ipfs.gateway-a.example.com"),
			want:     "https://" + cid + ".ipfs.gateway-a.example.com",
		},
		{
			name:     "path and query are part of the reference",
			resolved: resolved + "/index.html?fxhash=abc",
			stored:   ptr("https://gateway-a.example.com/ipfs/" + cid + "/index.html?fxhash=abc"),
			want:     "https://gateway-a.example.com/ipfs/" + cid + "/index.html?fxhash=abc",
		},
		{
			name:     "a bare directory URL heals to the resolved entry point",
			resolved: resolved + "/index.html",
			stored:   ptr("https://gateway-a.example.com/ipfs/" + cid),
			want:     resolved + "/index.html",
		},
		{
			name:     "different content takes the resolved URL",
			resolved: resolved,
			stored:   ptr("https://gateway-a.example.com/ipfs/" + otherCID),
			want:     resolved,
		},
		{
			name:     "a retired browser gateway is never kept",
			resolved: resolved,
			stored:   ptr("https://ipfs.io/ipfs/" + cid),
			want:     resolved,
		},
		{
			name:     "no stored URL takes the resolved URL",
			resolved: resolved,
			stored:   nil,
			want:     resolved,
		},
		{
			name:     "an empty stored URL takes the resolved URL",
			resolved: resolved,
			stored:   ptr(""),
			want:     resolved,
		},
		{
			name:     "a stored URL that is not a gateway URL takes the resolved URL",
			resolved: resolved,
			stored:   ptr("https://cdn.example.com/media/" + cid),
			want:     resolved,
		},
		{
			name:     "a resolved URL that is not a gateway URL is returned unchanged",
			resolved: "https://cdn.example.com/media/image.png",
			stored:   ptr("https://gateway-a.example.com/ipfs/" + cid),
			want:     "https://cdn.example.com/media/image.png",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, PreferStoredGatewayURL(tt.resolved, tt.stored))
		})
	}
}
