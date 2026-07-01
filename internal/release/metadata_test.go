package release

import (
	"testing"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
)

func TestMetadataFromArtBlocksVendorJSON(t *testing.T) {
	t.Parallel()

	json := adapter.NewJSON()
	meta := MetadataFromArtBlocksVendorJSON([]byte(`{
		"name": "Fidenza",
		"artist_name": "Tyler Hobbs",
		"max_invocations": 999
	}`), json)
	if meta == nil || meta.Name == nil || meta.TotalMints == nil {
		t.Fatalf("expected populated metadata, got %#v", meta)
	}
	if *meta.Name != "Fidenza by Tyler Hobbs" {
		t.Errorf("name = %q", *meta.Name)
	}
	if *meta.TotalMints != 999 {
		t.Errorf("total_mints = %d", *meta.TotalMints)
	}
}

func TestMetadataFromFeralFileVendorJSON(t *testing.T) {
	t.Parallel()

	json := adapter.NewJSON()
	meta := MetadataFromFeralFileVendorJSON([]byte(`{
		"series": {
			"title": "1DE94",
			"settings": { "maxArtwork": 75 },
			"artist": { "alumniAccount": { "alias": "Raven Kwok" } }
		}
	}`), json)
	if meta == nil || meta.Name == nil || meta.TotalMints == nil {
		t.Fatalf("expected populated metadata, got %#v", meta)
	}
	if *meta.Name != "1DE94 by Raven Kwok" {
		t.Errorf("name = %q", *meta.Name)
	}
	if *meta.TotalMints != 75 {
		t.Errorf("total_mints = %d", *meta.TotalMints)
	}
}
