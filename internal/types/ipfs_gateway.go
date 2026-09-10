package types

import (
	"net/url"
	"strings"
)

// IsBrowserIPFSGateway reports gateways that cannot serve production media
// reliably, including their CID subdomains.
//
// Reason: ipfs.io/dweb.link redirect navigations to the inbrowser.link viewer
// while native fetches intermittently throttle. A successful server probe does
// not make these suitable playback URLs (feral-file/feral-file#3526).
// Constraints: hostname boundaries matter; lookalike domains are not included.
func IsBrowserIPFSGateway(rawURL string) bool {
	u, err := url.Parse(rawURL)
	if err != nil || u.User != nil || (u.Scheme != "http" && u.Scheme != "https") {
		return false
	}
	host := strings.TrimSuffix(strings.ToLower(u.Hostname()), ".")
	for _, gateway := range []string{"ipfs.io", "dweb.link", "inbrowser.link"} {
		if host == gateway || strings.HasSuffix(host, "."+gateway) {
			return true
		}
	}
	return false
}
