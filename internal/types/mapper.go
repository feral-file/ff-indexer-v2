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

// ProvenanceEventTypeToSubjectType converts a provenance event type to a subject type
// For transfer events, the subject type depends on the token standard:
// - ERC721 (single token): returns SubjectTypeOwner
// - ERC1155/FA2 (multi-token): returns SubjectTypeBalance
func ProvenanceEventTypeToSubjectType(provenanceEventType schema.ProvenanceEventType, standard domain.ChainStandard) schema.SubjectType {
	switch provenanceEventType {
	case schema.ProvenanceEventTypeMint:
		return schema.SubjectTypeToken
	case schema.ProvenanceEventTypeBurn:
		return schema.SubjectTypeToken
	case schema.ProvenanceEventTypeTransfer:
		if standard == domain.StandardERC721 {
			return schema.SubjectTypeOwner
		}
		return schema.SubjectTypeBalance
	case schema.ProvenanceEventTypeMetadataUpdate:
		return schema.SubjectTypeMetadata
	default:
		return schema.SubjectTypeToken
	}
}
