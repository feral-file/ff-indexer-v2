package workflows

import "github.com/feral-file/ff-indexer-v2/internal/types"

// isIndexableMediaURL reports whether media processing can fetch u: an absolute URL or a
// data URI, but not onchfs://.
//
// Reason: onchfs://<root> parses as a URL (the root reads as a host), yet media
// processing only fetches HTTPS and data URIs, so the job would be enqueued just to fail.
// An onchfs work is read from chain by the player instead.
func isIndexableMediaURL(u string) bool {
	if types.HasOnChFSScheme(u) {
		return false
	}
	return types.IsValidURL(u) || types.IsDataURI(u)
}
