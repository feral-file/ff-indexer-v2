package dto

import "github.com/feral-file/ff-indexer-v2/internal/store/schema"

// ReleaseResponse represents a cross-vendor release with optional member tokens.
type ReleaseResponse struct {
	ID              uint64             `json:"id"`
	Vendor          string             `json:"vendor"`
	VendorReleaseID string             `json:"vendor_release_id"`
	Name            *string            `json:"name,omitempty"`
	TotalMints      *int64             `json:"total_mints,omitempty"`
	Members         *TokenListResponse `json:"members,omitempty"`
}

// ReleaseListResponse represents a paginated list of releases without member tokens.
type ReleaseListResponse struct {
	Items  []ReleaseResponse `json:"items"`
	Offset *uint64           `json:"offset,omitempty"`
}

// MapReleaseToDTO maps a schema.Release to ReleaseResponse without members.
func MapReleaseToDTO(release *schema.Release) *ReleaseResponse {
	if release == nil {
		return nil
	}

	return &ReleaseResponse{
		ID:              release.ID,
		Vendor:          string(release.Vendor),
		VendorReleaseID: release.VendorReleaseID,
		Name:            release.Name,
		TotalMints:      release.TotalMints,
	}
}
