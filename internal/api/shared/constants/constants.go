package constants

import "github.com/feral-file/ff-indexer-v2/internal/api/shared/types"

const (
	MAX_TOKEN_CIDS_PER_REQUEST      = 20
	MAX_ADDRESSES_PER_REQUEST       = 5
	MAX_PAGE_SIZE                   = 100
	DEFAULT_OFFSET                  = uint64(0)
	DEFAULT_TOKENS_LIMIT            = 20
	DEFAULT_OWNERS_LIMIT            = 10
	DEFAULT_PROVENANCE_EVENTS_LIMIT = 10
	DEFAULT_CHANGES_LIMIT           = 20
	DEFAULT_PROVENANCE_EVENTS_ORDER = types.OrderDesc
)
