package dto

// TokenDisplayResponse represents unified token display data
// It merges metadata and enrichment source, preferring enrichment source when available
type TokenDisplayResponse struct {
	ImageURL     *string            `json:"image_url,omitempty"`
	AnimationURL *string            `json:"animation_url,omitempty"`
	Name         *string            `json:"name,omitempty"`
	Description  *string            `json:"description,omitempty"`
	Publisher    *PublisherResponse `json:"publisher,omitempty"`
	Artists      []ArtistResponse   `json:"artists,omitempty"`
	MimeType     *string            `json:"mime_type,omitempty"`
}

// MediaURLs returns the media URLs from the token display
func (t *TokenDisplayResponse) MediaURLs() []string {
	var urls []string
	if t.ImageURL != nil && *t.ImageURL != "" {
		urls = append(urls, *t.ImageURL)
	}
	if t.AnimationURL != nil && *t.AnimationURL != "" {
		urls = append(urls, *t.AnimationURL)
	}
	return urls
}

// MergeTokenDisplay creates a unified display response by merging metadata and enrichment source
// Enrichment source takes precedence over metadata when both have the same field
func MergeTokenDisplay(metadata *TokenMetadataResponse, enrichment *EnrichmentSourceResponse) *TokenDisplayResponse {
	// If both are nil, return nil
	if metadata == nil && enrichment == nil {
		return nil
	}

	display := &TokenDisplayResponse{}

	// Start with metadata as base
	if metadata != nil {
		display.ImageURL = metadata.ImageURL
		display.AnimationURL = metadata.AnimationURL
		display.Name = metadata.Name
		display.Description = metadata.Description
		display.Publisher = metadata.Publisher
		display.Artists = metadata.Artists
		display.MimeType = metadata.MimeType
	}

	// Override with enrichment source if available
	if enrichment != nil {
		if enrichment.ImageURL != nil {
			display.ImageURL = enrichment.ImageURL
		}
		if enrichment.AnimationURL != nil {
			display.AnimationURL = enrichment.AnimationURL
		}
		if enrichment.Name != nil {
			display.Name = enrichment.Name
		}
		if enrichment.Description != nil {
			display.Description = enrichment.Description
		}
		// Note: EnrichmentSource doesn't have Publisher field, so keep metadata's publisher
		if len(enrichment.Artists) > 0 {
			display.Artists = enrichment.Artists
		}
		if enrichment.MimeType != nil {
			display.MimeType = enrichment.MimeType
		}
	}

	return display
}
