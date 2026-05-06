// Package ssrf validates outbound HTTP(S) URLs for server-side request forgery risks used by
// components that fetch attacker-influenced URLs (media health sweeper, media worker downloads).
//
// Reason: Stored media URLs may point at loopback, RFC-private space, link-local, or cloud metadata.
// Trade-offs: DNS resolution adds latency; allowlisted hosts skip resolution so operators must trust DNS.
// Constraints: Only http/https; validates resolved addresses for non-allowlisted hosts.
package ssrf

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"net/url"
	"slices"
	"strings"
)

// ErrBlocked indicates the URL target failed SSRF policy checks.
var ErrBlocked = errors.New("ssrf: blocked URL")

// ErrResolutionFailed indicates DNS (or dual-stack) resolution yielded no usable addresses.
// It is distinct from ErrBlocked so callers can treat resolver outages as transient fetch failures
// rather than SSRF policy violations.
var ErrResolutionFailed = errors.New("ssrf: host resolution failed")

// Resolver resolves hostnames to IP addresses (injectable for tests).
type Resolver interface {
	LookupNetIP(ctx context.Context, network, host string) ([]netip.Addr, error)
}

type netResolver struct{}

func (netResolver) LookupNetIP(ctx context.Context, network, host string) ([]netip.Addr, error) {
	return net.DefaultResolver.LookupNetIP(ctx, network, host)
}

// Options configures SSRF validation behavior.
type Options struct {
	// BlockMulticast adds multicast CIDRs (IPv4 224.0.0.0/4, IPv6 ff00::/8) when true.
	BlockMulticast bool

	// AllowDomains are hostnames that bypass hostname/IP blocklists (lowercase FQDNs).
	AllowDomains []string

	// AllowIPs are literal IPs that bypass IP blocklists (IPv4 or IPv6).
	AllowIPs []netip.Addr
}

// Validator validates raw HTTP(S) URLs before outbound requests.
type Validator struct {
	resolver Resolver
	opts     Options

	fixedRanges []netip.Prefix // critical + high priority ranges (always)
	mcastRanges []netip.Prefix // optional multicast
}

// NewValidator builds a Validator with default resolver and merged Options.
func NewValidator(opts Options) *Validator {
	v := &Validator{
		resolver:    netResolver{},
		opts:        opts,
		fixedRanges: append([]netip.Prefix(nil), defaultBlockedPrefixes...),
	}
	if opts.BlockMulticast {
		v.mcastRanges = append(v.mcastRanges, multicastIPv4, multicastIPv6)
	}
	return v
}

// NewValidatorWithResolver is like NewValidator but uses a custom Resolver (tests).
func NewValidatorWithResolver(resolver Resolver, opts Options) *Validator {
	v := NewValidator(opts)
	v.resolver = resolver
	return v
}

// ValidateHTTPURL ensures rawURL is safe for an outbound HTTP(S) fetch.
//
// Constraints:
//   - Scheme must be http or https.
//   - Userinfo (http://user@host) is rejected.
//   - Host must not match blocked hostname patterns unless allowlisted.
//   - Host literals and DNS results must not fall into blocked IP ranges unless allowlisted.
//
// Allowlisted domains bypass hostname checks and DNS resolution entirely; operators must trust DNS for those names.
//
// DNS resolution failures surface as ErrResolutionFailed (not ErrBlocked) so health checks can retry
// without marking SSRFBlocked.
func (v *Validator) ValidateHTTPURL(ctx context.Context, rawURL string) error {
	u, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("%w: parse URL: %w", ErrBlocked, err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("%w: unsupported scheme %q", ErrBlocked, u.Scheme)
	}
	if u.User != nil {
		return fmt.Errorf("%w: URL userinfo is not allowed", ErrBlocked)
	}
	host := u.Hostname()
	if host == "" {
		return fmt.Errorf("%w: missing host", ErrBlocked)
	}
	host = strings.ToLower(host)

	if v.hostAllowlisted(host) {
		return nil
	}
	if err := v.checkBlockedHostname(host); err != nil {
		return err
	}

	if ip, err := netip.ParseAddr(host); err == nil {
		if v.ipAllowlisted(ip) {
			return nil
		}
		return v.checkAddr(ip)
	}

	var ips []netip.Addr
	var lookupErrs []error
	for _, network := range []string{"ip4", "ip6"} {
		addrs, errLookup := v.resolver.LookupNetIP(ctx, network, host)
		if errLookup != nil {
			lookupErrs = append(lookupErrs, errLookup)
			continue
		}
		ips = append(ips, addrs...)
	}
	if len(ips) == 0 {
		return fmt.Errorf("%w: DNS resolution failed for host %q: %w", ErrResolutionFailed, host, errors.Join(lookupErrs...))
	}

	for _, ip := range ips {
		if v.ipAllowlisted(ip) {
			continue
		}
		if err := v.checkAddr(ip); err != nil {
			return err
		}
	}
	return nil
}

func (v *Validator) hostAllowlisted(host string) bool {
	for _, d := range v.opts.AllowDomains {
		d = strings.ToLower(strings.TrimSpace(d))
		if d == "" {
			continue
		}
		if host == d || strings.HasSuffix(host, "."+d) {
			return true
		}
	}
	return false
}

func (v *Validator) ipAllowlisted(ip netip.Addr) bool {
	if slices.Contains(v.opts.AllowIPs, ip) {
		return true
	}
	if mapped, ok := extractEmbeddedIPv4(ip); ok {
		return slices.Contains(v.opts.AllowIPs, mapped)
	}
	return false
}

// ValidateAllowlistDomainEntry rejects unsafe allowlist.domain patterns at config load.
// Entries must contain at least one dot so bare public suffixes like "com" cannot whitelist entire TLDs.
func ValidateAllowlistDomainEntry(raw string) error {
	d := strings.ToLower(strings.TrimSpace(raw))
	if d == "" {
		return nil
	}
	if !strings.Contains(d, ".") {
		return fmt.Errorf("domain %q must contain at least one dot (reject overly broad suffix entries)", raw)
	}
	return nil
}

func (v *Validator) checkBlockedHostname(host string) error {
	switch host {
	case "localhost", "localhost.localdomain", "metadata", "metadata.google.internal", "metadata.azure.internal", "169.254.169.254":
		return fmt.Errorf("%w: forbidden hostname %q", ErrBlocked, host)
	}
	if strings.HasSuffix(host, ".localhost") || strings.HasSuffix(host, ".local") {
		return fmt.Errorf("%w: forbidden hostname %q", ErrBlocked, host)
	}
	if host == "localtest.me" || strings.HasSuffix(host, ".localtest.me") {
		return fmt.Errorf("%w: forbidden hostname %q", ErrBlocked, host)
	}
	if host == "lvh.me" || strings.HasSuffix(host, ".lvh.me") {
		return fmt.Errorf("%w: forbidden hostname %q", ErrBlocked, host)
	}
	return nil
}

func (v *Validator) checkAddr(ip netip.Addr) error {
	if mapped, ok := extractEmbeddedIPv4(ip); ok {
		return v.checkAddr(mapped)
	}

	for _, pfx := range v.fixedRanges {
		if pfx.Contains(ip) {
			return fmt.Errorf("%w: address %s matches blocked range %s", ErrBlocked, ip, pfx)
		}
	}
	for _, pfx := range v.mcastRanges {
		if pfx.Contains(ip) {
			return fmt.Errorf("%w: address %s matches blocked multicast range %s", ErrBlocked, ip, pfx)
		}
	}
	return nil
}

func extractEmbeddedIPv4(ip netip.Addr) (netip.Addr, bool) {
	if !ip.Is6() {
		return netip.Addr{}, false
	}
	if ip.Is4In6() {
		return ip.Unmap(), true
	}
	b := ip.As16()
	// 64:ff9b::/96 — IPv4/IPv6 translation (RFC 6052); validate embedded IPv4.
	const prefix64ff9b = 0x0064ff9b00000000
	if ipBitPrefix128(b) == prefix64ff9b {
		v4, ok := netip.AddrFromSlice(b[12:16])
		if !ok {
			return netip.Addr{}, false
		}
		return v4, true
	}
	return netip.Addr{}, false
}

func ipBitPrefix128(b [16]byte) uint64 {
	return uint64(b[0])<<56 | uint64(b[1])<<48 | uint64(b[2])<<40 | uint64(b[3])<<32 |
		uint64(b[4])<<24 | uint64(b[5])<<16 | uint64(b[6])<<8 | uint64(b[7])
}

var (
	multicastIPv4 = mustPrefix("224.0.0.0/4")
	multicastIPv6 = mustPrefix("ff00::/8")

	defaultBlockedPrefixes = []netip.Prefix{
		mustPrefix("0.0.0.0/8"),
		mustPrefix("10.0.0.0/8"),
		mustPrefix("100.64.0.0/10"),
		mustPrefix("127.0.0.0/8"),
		mustPrefix("169.254.0.0/16"),
		mustPrefix("172.16.0.0/12"),
		mustPrefix("192.0.0.0/24"),
		mustPrefix("192.0.2.0/24"),
		mustPrefix("192.168.0.0/16"),
		mustPrefix("198.18.0.0/15"),
		mustPrefix("198.51.100.0/24"),
		mustPrefix("203.0.113.0/24"),
		mustPrefix("240.0.0.0/4"),
		mustPrefix("255.255.255.255/32"),
		mustPrefix("::1/128"),
		mustPrefix("::/128"),
		mustPrefix("64:ff9b::/96"),
		mustPrefix("100::/64"),
		mustPrefix("2001::/32"),
		mustPrefix("2001:10::/28"),
		mustPrefix("2001:db8::/32"),
		mustPrefix("fc00::/7"),
		mustPrefix("fe80::/10"),
	}
)

func mustPrefix(s string) netip.Prefix {
	pfx, err := netip.ParsePrefix(s)
	if err != nil {
		panic(err)
	}
	return pfx
}
