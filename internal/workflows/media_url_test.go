package workflows

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestIsIndexableMediaURL pins which URLs reach media processing. The helper lives in an
// untagged file because the metadata workflow, built in both CGO modes, calls it.
func TestIsIndexableMediaURL(t *testing.T) {
	tests := []struct {
		in   string
		want bool
	}{
		{"https://example.com/media.jpg", true},
		{"data:image/png;base64,iVBORw0KGgo=", true},
		{"onchfs://b12f18fcd51a21cc3ace5090fa808c8d4e4552d9e2239a3886b89f8ea12ff685/?fxhash=oo1", false},
		{"not-a-url", false},
		{"", false},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			assert.Equal(t, tt.want, isIndexableMediaURL(tt.in))
		})
	}
}
