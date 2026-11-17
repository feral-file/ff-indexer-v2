package types

import (
	"regexp"
	"strings"
)

// OnChFSScheme is the URI scheme of the on-chain file system that fxhash works are stored on.
const OnChFSScheme = "onchfs://"

var (
	// onchfsURIPattern matches onchfs://<64-hex root> optionally followed by a path, query or
	// fragment.
	onchfsURIPattern = regexp.MustCompile(`^onchfs://[0-9a-fA-F]{64}(?:[/?#].*)?$`)
	// onchfsGatewayRootPattern locates the root of an OnChFS gateway URL: the same hosts
	// IsOnChFSGatewayURL accepts, with the submatch starting at the 64-hex root.
	onchfsGatewayRootPattern = regexp.MustCompile(`^https?://[a-zA-Z0-9._-]*(?:onchfs|fxhash2?\.xyz)[a-zA-Z0-9._-]*(?::[0-9]+)?/([0-9a-fA-F]{64}(?:[/?].*)?)$`)
)

// HasOnChFSScheme reports whether a value claims the onchfs: scheme at all, well-formed or not.
//
// Reason: like HasDataURIScheme, dispatch keys on the scheme so every onchfs value belongs to
// the local OnChFS rule, which classifies a malformed one as invalid rather than letting it
// fall through to HTTP handling.
func HasOnChFSScheme(s string) bool {
	return strings.HasPrefix(s, OnChFSScheme)
}

// IsOnChFSURI reports whether s is a well-formed onchfs URI: the scheme, a 64-hex root
// (Keccak-256 of the root inode), and an optional path, query or fragment.
func IsOnChFSURI(s string) bool {
	return onchfsURIPattern.MatchString(s)
}

// CanonicalOnChFSURI rewrites an OnChFS gateway URL to the onchfs:// URI it was derived from;
// any other value is returned unchanged.
//
// Reason: fxhash's onchfs gateway (onchfs.fxhash2.xyz) is gone and no other host serves
// onchfs, so a gateway URL is a dead link while the URI it came from still names content
// that is intact on chain. Vendors (OpenSea, fxhash's own animation_url) keep supplying the
// gateway form, so every ingest path canonicalizes it.
//
// Trade-offs: everything after the root is kept verbatim, including the fxhash/fxiteration/
// fxminter query that selects the artwork iteration, so the result is byte-identical to the
// artifactUri in the on-chain metadata. The root's case is kept as stored, not lowered.
//
// Constraints: must stay byte-identical to the SQL backfill in
// db/migrations/034_onchfs_canonical_uri.sql, since stored URL hashes are md5 of the exact
// string.
func CanonicalOnChFSURI(s string) string {
	m := onchfsGatewayRootPattern.FindStringSubmatch(s)
	if m == nil {
		return s
	}
	return OnChFSScheme + m[1]
}
