package types

import (
	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
)

// TransferEventTypeToProvenanceEventType converts a transfer event type to a provenance event type
func TransferEventTypeToProvenanceEventType(transferEventType domain.EventType) schema.ProvenanceEventType {
	switch transferEventType {
	case domain.EventTypeMint:
		return schema.ProvenanceEventTypeMint
	case domain.EventTypeTransfer:
		return schema.ProvenanceEventTypeTransfer
	case domain.EventTypeBurn:
		return schema.ProvenanceEventTypeBurn
	default:
		return schema.ProvenanceEventTypeTransfer
	}
}
