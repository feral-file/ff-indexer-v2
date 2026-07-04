package constants

import "github.com/feral-file/ff-indexer-v2/internal/api/shared/types"

const (
	MAX_TOKEN_CIDS_PER_REQUEST = 50
	MAX_ADDRESSES_PER_REQUEST  = 5
	MAX_PAGE_SIZE              = uint8(255)
	MAX_RETRY_MAX_ATTEMPTS     = 10
	// MAX_RELEASE_MINT_NUMBERS is the maximum number of mint numbers that can be indexed in a
	// single IndexRelease request. Clients must batch larger collections themselves by making
	// multiple calls with non-overlapping mint number lists. Kept intentionally small (50) so
	// that Phase 1 (CID derivation + child-job enqueueing) completes quickly and fxhash/FF API
	// calls stay within a single paginated batch. Matches MAX_TOKEN_CIDS_PER_REQUEST and
	// MAX_TOKEN_MINT_NUMBERS_FILTER for consistency.
	MAX_RELEASE_MINT_NUMBERS = int64(50)
	// MAX_TOKEN_MINT_NUMBERS_FILTER is the maximum number of mint numbers allowed in the
	// mint_numbers filter on GET /api/v1/tokens and the equivalent GraphQL field. Matches
	// MAX_RELEASE_MINT_NUMBERS so a single IndexRelease batch maps 1:1 to a polling query.
	MAX_TOKEN_MINT_NUMBERS_FILTER = int64(50)
	// MAX_API_VENDOR_MINT_SPAN caps max(mint_numbers)-min(mint_numbers) for API-based vendors
	// (fxhash, feralfile) whose CID derivation fetches the entire [min,max] interval from the
	// vendor API, paginated at 100 items per page. A span of 1000 limits worst-case paging to
	// 10 vendor API calls per IndexRelease job regardless of how many mint numbers are requested.
	// artblocks and objkt are deterministic and are not subject to this cap.
	MAX_API_VENDOR_MINT_SPAN = int64(1000)
	DEFAULT_OFFSET                       = uint64(0)
	DEFAULT_TOKENS_LIMIT                 = uint8(20)
	DEFAULT_OWNERS_LIMIT                 = uint8(20)
	DEFAULT_PROVENANCE_EVENTS_LIMIT      = uint8(20)
	DEFAULT_OWNER_PROVENANCES_LIMIT      = uint8(20)
	DEFAULT_SYNC_COLLECTION_LIMIT        = uint8(20)
	DEFAULT_RETRY_MAX_ATTEMPTS           = 5
	DEFAULT_WEBHOOK_CLIENT_SECRET_LENGTH = 32
	DEFAULT_PROVENANCE_EVENTS_ORDER      = types.OrderDesc
)
