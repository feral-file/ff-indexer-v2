package release

import (
	"strings"
)

// FormatReleaseName builds a cross-vendor release display name from a project/series
// title and artist. When artist is non-empty and not already part of the title,
// it appends " by {artist}" (e.g. "Fidenza by Tyler Hobbs", "1DE94 by Raven Kwok").
func FormatReleaseName(title, artist string) string {
	title = strings.TrimSpace(title)
	artist = strings.TrimSpace(artist)
	if title == "" {
		return artist
	}
	if artist == "" {
		return title
	}
	if strings.Contains(strings.ToLower(title), strings.ToLower(artist)) {
		return title
	}
	return title + " by " + artist
}
