package release

import (
	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/providers/vendors/feralfile"
)

// Metadata holds optional release display fields extracted from vendor enrichment data.
type Metadata struct {
	Name       *string
	TotalMints *int64
}

// MetadataFromArtBlocksProject builds release metadata from AB project fields.
func MetadataFromArtBlocksProject(name, artistName string, maxInvocations int) *Metadata {
	formatted := FormatReleaseName(name, artistName)
	if formatted == "" && maxInvocations <= 0 {
		return nil
	}
	meta := &Metadata{}
	if formatted != "" {
		meta.Name = &formatted
	}
	if maxInvocations > 0 {
		total := int64(maxInvocations)
		meta.TotalMints = &total
	}
	return meta
}

type abVendorProject struct {
	Name           string `json:"name"`
	ArtistName     string `json:"artist_name"`
	MaxInvocations int    `json:"max_invocations"`
}

// MetadataFromArtBlocksVendorJSON parses stored AB vendor JSON for release metadata.
// Returns nil when vendor JSON is empty or unparseable.
func MetadataFromArtBlocksVendorJSON(vendorJSON []byte, json adapter.JSON) *Metadata {
	if len(vendorJSON) == 0 {
		return nil
	}
	var project abVendorProject
	if err := json.Unmarshal(vendorJSON, &project); err != nil {
		return nil
	}
	return MetadataFromArtBlocksProject(project.Name, project.ArtistName, project.MaxInvocations)
}

// MetadataFromFeralFileArtwork builds release metadata from a FF artwork response.
func MetadataFromFeralFileArtwork(artwork *feralfile.Artwork) *Metadata {
	if artwork == nil {
		return nil
	}
	formatted := FormatReleaseName(artwork.Series.Title, artwork.Series.Artist.AlumniAccount.Alias)
	maxArtwork := artwork.Series.Settings.MaxArtwork
	if formatted == "" && maxArtwork <= 0 {
		return nil
	}
	meta := &Metadata{}
	if formatted != "" {
		meta.Name = &formatted
	}
	if maxArtwork > 0 {
		meta.TotalMints = &maxArtwork
	}
	return meta
}

type ffVendorArtworkMetadata struct {
	Series struct {
		Title    string `json:"title"`
		Settings struct {
			MaxArtwork int64 `json:"maxArtwork"`
		} `json:"settings"`
		Artist struct {
			AlumniAccount struct {
				Alias string `json:"alias"`
			} `json:"alumniAccount"`
		} `json:"artist"`
	} `json:"series"`
}

// MetadataFromFeralFileVendorJSON parses stored FF vendor JSON for release metadata.
func MetadataFromFeralFileVendorJSON(vendorJSON []byte, json adapter.JSON) *Metadata {
	if len(vendorJSON) == 0 {
		return nil
	}
	var artwork ffVendorArtworkMetadata
	if err := json.Unmarshal(vendorJSON, &artwork); err != nil {
		return nil
	}
	formatted := FormatReleaseName(artwork.Series.Title, artwork.Series.Artist.AlumniAccount.Alias)
	maxArtwork := artwork.Series.Settings.MaxArtwork
	if formatted == "" && maxArtwork <= 0 {
		return nil
	}
	meta := &Metadata{}
	if formatted != "" {
		meta.Name = &formatted
	}
	if maxArtwork > 0 {
		meta.TotalMints = &maxArtwork
	}
	return meta
}
