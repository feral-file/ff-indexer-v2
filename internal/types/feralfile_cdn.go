package types

import "strings"

const (
	// RetiredFeralFileCDNPrefix is the Feral File artwork CDN origin that no longer resolves.
	RetiredFeralFileCDNPrefix = "https://cdn.feralfileassets.com/"
	// FeralFileCDNPrefix is the Feral File artwork CDN origin serving the same paths.
	FeralFileCDNPrefix = "https://cdn.artworks.feralfile.io/"
)

// MigrateFeralFileCDN rewrites a URL on the retired Feral File CDN to the new origin.
//
// Reason: Feral File moved artwork files from cdn.feralfileassets.com to
// cdn.artworks.feralfile.io without changing paths, and the old hostname no longer
// resolves. Both on-chain metadata and the Feral File API still carry the old origin,
// so every rebuild would otherwise restore dead URLs.
//
// Trade-offs: every occurrence is replaced, including CDN URLs embedded in query
// parameters (e.g. clip_directory_url), because a player loading those would hit the
// same dead host. A directory-style path ("…/<ts>/?edition_number=…") gains
// "index.html": the old CDN served a directory index, the new one answers 404.
//
// Constraints: must stay byte-identical to the SQL backfill in
// db/migrations/032_feralfile_cdn_move.sql, since stored URL hashes are md5 of the
// exact string. URLs on any other host are returned unchanged.
func MigrateFeralFileCDN(url string) string {
	url = strings.ReplaceAll(url, RetiredFeralFileCDNPrefix, FeralFileCDNPrefix)
	if !strings.HasPrefix(url, FeralFileCDNPrefix) {
		return url
	}

	pathEnd := strings.IndexAny(url, "?#")
	if pathEnd < 0 {
		pathEnd = len(url)
	}
	if pathEnd <= len(FeralFileCDNPrefix) || url[pathEnd-1] != '/' {
		return url
	}
	return url[:pathEnd] + "index.html" + url[pathEnd:]
}
