package types

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

const testOnChFSRoot = "b12f18fcd51a21cc3ace5090fa808c8d4e4552d9e2239a3886b89f8ea12ff685"

// TestCanonicalOnChFSURI pins the rewrite that db/migrations/034_onchfs_canonical_uri.sql
// reproduces in SQL; both must produce byte-identical URLs for the md5 hashes to agree.
func TestCanonicalOnChFSURI(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"iteration query kept verbatim",
			"https://onchfs.fxhash2.xyz/" + testOnChFSRoot + "/?fxhash=0xb56f&fxiteration=44&fxminter=0x94E3&fxchain=ETHEREUM",
			"onchfs://" + testOnChFSRoot + "/?fxhash=0xb56f&fxiteration=44&fxminter=0x94E3&fxchain=ETHEREUM"},
		{"bare root", "https://onchfs.fxhash2.xyz/" + testOnChFSRoot, "onchfs://" + testOnChFSRoot},
		{"root with trailing slash", "https://onchfs.fxhash2.xyz/" + testOnChFSRoot + "/", "onchfs://" + testOnChFSRoot + "/"},
		{"query without slash", "https://onchfs.fxhash2.xyz/" + testOnChFSRoot + "?fxhash=oo1", "onchfs://" + testOnChFSRoot + "?fxhash=oo1"},
		{"sub path kept", "https://onchfs.fxhash2.xyz/" + testOnChFSRoot + "/index.html", "onchfs://" + testOnChFSRoot + "/index.html"},
		{"upper-case root kept", "https://onchfs.fxhash2.xyz/B12F18FCD51A21CC3ACE5090FA808C8D4E4552D9E2239A3886B89F8EA12FF685", "onchfs://B12F18FCD51A21CC3ACE5090FA808C8D4E4552D9E2239A3886B89F8EA12FF685"},
		{"http and other fxhash gateway", "http://gateway.fxhash.xyz/" + testOnChFSRoot + "/", "onchfs://" + testOnChFSRoot + "/"},
		{"gateway with port", "https://onchfs.example.com:8443/" + testOnChFSRoot, "onchfs://" + testOnChFSRoot},
		{"already canonical", "onchfs://" + testOnChFSRoot + "/?fxhash=oo1", "onchfs://" + testOnChFSRoot + "/?fxhash=oo1"},
		{"short hash untouched", "https://onchfs.fxhash2.xyz/b12f18fc", "https://onchfs.fxhash2.xyz/b12f18fc"},
		{"fragment right after root untouched", "https://onchfs.fxhash2.xyz/" + testOnChFSRoot + "#x", "https://onchfs.fxhash2.xyz/" + testOnChFSRoot + "#x"},
		{"other host untouched", "https://example.com/" + testOnChFSRoot, "https://example.com/" + testOnChFSRoot},
		{"ipfs gateway untouched", "https://ipfs.io/ipfs/QmP1u8KrSShWcF7iCMRhXFnJUNgMKqFrQ7qZMZfD9tjD3c", "https://ipfs.io/ipfs/QmP1u8KrSShWcF7iCMRhXFnJUNgMKqFrQ7qZMZfD9tjD3c"},
		{"empty", "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, CanonicalOnChFSURI(tt.in))
		})
	}
}

// TestIsOnChFSURI pins the well-formedness rule the media health check applies locally.
func TestIsOnChFSURI(t *testing.T) {
	tests := []struct {
		in   string
		want bool
	}{
		{"onchfs://" + testOnChFSRoot, true},
		{"onchfs://" + testOnChFSRoot + "/", true},
		{"onchfs://" + testOnChFSRoot + "/?fxhash=oo1&fxiteration=140", true},
		{"onchfs://" + testOnChFSRoot + "?fxhash=oo1", true},
		{"onchfs://" + testOnChFSRoot + "/index.html", true},
		{"onchfs://" + testOnChFSRoot + "#frag", true},
		{"onchfs://" + "B12F18FCD51A21CC3ACE5090FA808C8D4E4552D9E2239A3886B89F8EA12FF685", true},
		{"onchfs://" + testOnChFSRoot[:63], false},
		{"onchfs://" + testOnChFSRoot + "0", false},
		{"onchfs://" + "z" + testOnChFSRoot[1:], false},
		{"onchfs://", false},
		{"onchfs:" + testOnChFSRoot, false},
		{"https://onchfs.fxhash2.xyz/" + testOnChFSRoot, false},
		{"", false},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			assert.Equal(t, tt.want, IsOnChFSURI(tt.in))
		})
	}
}

// TestHasOnChFSScheme checks that scheme dispatch claims malformed onchfs values too.
func TestHasOnChFSScheme(t *testing.T) {
	assert.True(t, HasOnChFSScheme("onchfs://"+testOnChFSRoot))
	assert.True(t, HasOnChFSScheme("onchfs://not-a-root"))
	assert.False(t, HasOnChFSScheme("https://onchfs.fxhash2.xyz/"+testOnChFSRoot))
	assert.False(t, HasOnChFSScheme(""))
}
