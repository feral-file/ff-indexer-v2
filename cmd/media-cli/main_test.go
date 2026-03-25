package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseURLs_UsesSingleValue(t *testing.T) {
	urls := parseURLs("  https://example.com/a.png  ", "")
	require.Equal(t, []string{"https://example.com/a.png"}, urls)
}

func TestParseURLs_MultipleURLsIncludingDataURI(t *testing.T) {
	input := "https://example.com/a.png, data:image/png;base64,AAAA,https://example.com/b.png"
	urls := parseURLs("", input)
	require.Equal(t, []string{
		"https://example.com/a.png",
		"data:image/png;base64,AAAA",
		"https://example.com/b.png",
	}, urls)
}

func TestParseURLs_EmptyInput(t *testing.T) {
	urls := parseURLs("", "   ")
	require.Nil(t, urls)
}
