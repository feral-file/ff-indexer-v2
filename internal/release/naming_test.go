package release

import "testing"

func TestFormatReleaseName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		title  string
		artist string
		want   string
	}{
		{"Fidenza", "Tyler Hobbs", "Fidenza by Tyler Hobbs"},
		{"1DE94", "Raven Kwok", "1DE94 by Raven Kwok"},
		{"Fidenza by Tyler Hobbs", "Tyler Hobbs", "Fidenza by Tyler Hobbs"},
		{"Untitled", "", "Untitled"},
		{"", "Artist Only", "Artist Only"},
		{"  Spaced  ", "  Name  ", "Spaced by Name"},
	}

	for _, tc := range tests {
		got := FormatReleaseName(tc.title, tc.artist)
		if got != tc.want {
			t.Errorf("FormatReleaseName(%q, %q) = %q, want %q", tc.title, tc.artist, got, tc.want)
		}
	}
}
