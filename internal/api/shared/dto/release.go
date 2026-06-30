package dto

// ReleaseResponse represents a cross-vendor release with optional member tokens.
type ReleaseResponse struct {
	ID              uint64             `json:"id"`
	Vendor          string             `json:"vendor"`
	VendorReleaseID string             `json:"vendor_release_id"`
	Members         *TokenListResponse `json:"members,omitempty"`
}
