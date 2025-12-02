package constants

import "github.com/feral-file/ff-indexer-v2/internal/api/shared/types"

const (
	MAX_TOKEN_CIDS_PER_REQUEST      = 50
	MAX_ADDRESSES_PER_REQUEST       = 5
	MAX_PAGE_SIZE                   = uint8(255)
	DEFAULT_OFFSET                  = uint64(0)
	DEFAULT_TOKENS_LIMIT            = uint8(20)
	DEFAULT_OWNERS_LIMIT            = uint8(20)
	DEFAULT_PROVENANCE_EVENTS_LIMIT = uint8(20)
	DEFAULT_CHANGES_LIMIT           = uint8(20)
	DEFAULT_PROVENANCE_EVENTS_ORDER = types.OrderDesc
)
