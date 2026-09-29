package uri

import "github.com/feral-file/ff-indexer-v2/internal/types"

// PreferStoredGatewayURL returns stored instead of resolved when both are gateway
// URLs for the same IPFS reference, so re-indexing unchanged content keeps its URL.
//
// Reason: gateway selection races every configured gateway and keeps the first to
// validate, so each re-index could store the same content under another host. Media
// assets and health rows are keyed by the exact URL, so that change detached working
// assets from their tokens and re-uploaded identical bytes.
// Trade-offs: the stored URL is kept without a probe of its own. The media health
// check that follows in the same indexing run probes it directly and promotes a
// working alternative only when it fails, so the probe is not repeated here. The
// race behind resolved still runs; its winner also serves MIME detection.
// Constraints: a retired browser gateway is never kept, because its replacement is
// the migration. Only IPFS is covered: the Arweave reference taken from a gateway
// URL drops the path that selects the resource, so two URLs could not be compared
// safely. References must match exactly, so a bare directory URL still heals to the
// index.html entry point that resolution selects.
func PreferStoredGatewayURL(resolved string, stored *string) string {
	if stored == nil || *stored == resolved || types.IsBrowserIPFSGateway(*stored) {
		return resolved
	}
	if !types.SameIPFSReference(resolved, *stored) {
		return resolved
	}
	return *stored
}
