package constants

import "github.com/feral-file/ff-indexer-v2/internal/api/shared/types"

const (
	MAX_TOKEN_CIDS_PER_REQUEST = 50
	MAX_ADDRESSES_PER_REQUEST  = 5
	MAX_PAGE_SIZE              = uint8(255)
	MAX_RETRY_MAX_ATTEMPTS     = 10
	// MAX_RELEASE_MINT_RANGE is the maximum number of tokens that can be indexed in a single
	// IndexRelease request. Clients must batch larger collections themselves by making multiple
	// calls with non-overlapping mint ranges. Kept intentionally small (100) so that the Phase 1
	// job (CID derivation + child-job enqueueing) completes quickly and fxhash/FF API calls stay
	// within a single paginated batch.
	MAX_RELEASE_MINT_RANGE               = int64(100)
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
