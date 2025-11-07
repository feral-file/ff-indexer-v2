package dto

import (
	"encoding/json"
	"time"

	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
)

// ChangeResponse represents a change journal entry
type ChangeResponse struct {
	ID          uint64             `json:"id"`
	SubjectType schema.SubjectType `json:"subject_type"`
	SubjectID   string             `json:"subject_id"`
	ChangedAt   time.Time          `json:"changed_at"`
	Meta        json.RawMessage    `json:"meta,omitempty"`
	CreatedAt   time.Time          `json:"created_at"`
	UpdatedAt   time.Time          `json:"updated_at"`

	// Expansion
	Subject interface{} `json:"subject,omitempty"` // Expanded subject based on subject_type
}

// ChangeListResponse represents a paginated list of changes
type ChangeListResponse struct {
	Changes []ChangeResponse `json:"items"`
	Offset  *uint64          `json:"offset,omitempty"` // Offset for the next page
	Total   uint64           `json:"total"`
}

// MapChangeToDTO maps a schema.ChangesJournal to ChangeResponse
func MapChangeToDTO(change *schema.ChangesJournal) *ChangeResponse {
	dto := &ChangeResponse{
		ID:          change.ID,
		SubjectType: change.SubjectType,
		SubjectID:   change.SubjectID,
		ChangedAt:   change.ChangedAt,
		CreatedAt:   change.CreatedAt,
		UpdatedAt:   change.UpdatedAt,
	}

	if change.Meta != nil {
		dto.Meta = json.RawMessage(change.Meta)
	}

	return dto
}
