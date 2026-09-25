package types

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// TestMigrateFeralFileCDN pins the rewrite that db/migrations/032_feralfile_cdn_move.sql
// reproduces in SQL; both must produce byte-identical URLs for the md5 hashes to agree.
func TestMigrateFeralFileCDN(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"file path", "https://cdn.feralfileassets.com/previews/a/1/preview.mp4", "https://cdn.artworks.feralfile.io/previews/a/1/preview.mp4"},
		{"extensionless thumbnail", "https://cdn.feralfileassets.com/thumbnails/a/1", "https://cdn.artworks.feralfile.io/thumbnails/a/1"},
		{"query kept", "https://cdn.feralfileassets.com/previews/a/1/index.html?edition_number=2&blockchain=ethereum", "https://cdn.artworks.feralfile.io/previews/a/1/index.html?edition_number=2&blockchain=ethereum"},
		{"directory with query", "https://cdn.feralfileassets.com/previews/a/1/?edition_number=3&blockchain=bitmark", "https://cdn.artworks.feralfile.io/previews/a/1/index.html?edition_number=3&blockchain=bitmark"},
		{"bare directory", "https://cdn.feralfileassets.com/previews/a/1/_unique-previews/0/", "https://cdn.artworks.feralfile.io/previews/a/1/_unique-previews/0/index.html"},
		{"directory with fragment", "https://cdn.feralfileassets.com/previews/a/1/#x", "https://cdn.artworks.feralfile.io/previews/a/1/index.html#x"},
		{"slash inside query only", "https://cdn.feralfileassets.com/previews/a/1/nft.html?next=/", "https://cdn.artworks.feralfile.io/previews/a/1/nft.html?next=/"},
		{"embedded CDN URL", "https://cdn.feralfileassets.com/previews/a/1/index.html?clip_directory_url=https://cdn.feralfileassets.com/misc/clips", "https://cdn.artworks.feralfile.io/previews/a/1/index.html?clip_directory_url=https://cdn.artworks.feralfile.io/misc/clips"},
		{"already migrated directory", "https://cdn.artworks.feralfile.io/previews/a/1/?e=1", "https://cdn.artworks.feralfile.io/previews/a/1/index.html?e=1"},
		{"already migrated file is idempotent", "https://cdn.artworks.feralfile.io/previews/a/1/index.html?e=1", "https://cdn.artworks.feralfile.io/previews/a/1/index.html?e=1"},
		{"origin root untouched", "https://cdn.feralfileassets.com/", "https://cdn.artworks.feralfile.io/"},
		{"other host untouched", "https://example.com/previews/a/", "https://example.com/previews/a/"},
		{"other host embedding old CDN", "https://example.com/?u=https://cdn.feralfileassets.com/x", "https://example.com/?u=https://cdn.artworks.feralfile.io/x"},
		{"empty", "", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, MigrateFeralFileCDN(tt.in))
		})
	}
}
