package ssrf_test

import (
	"context"
	"errors"
	"net/netip"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/feral-file/ff-indexer-v2/internal/security/ssrf"
)

type mapResolver map[string][]netip.Addr

func (m mapResolver) LookupNetIP(_ context.Context, network, host string) ([]netip.Addr, error) {
	host = strings.ToLower(host)
	key := network + "/" + host
	addrs, ok := m[key]
	if !ok {
		return nil, &netError{msg: "lookup failed"}
	}
	return addrs, nil
}

type netError struct{ msg string }

func (e *netError) Error() string   { return e.msg }
func (e *netError) Timeout() bool   { return false }
func (e *netError) Temporary() bool { return false }

func TestValidator_publicHTTPSucceeds(t *testing.T) {
	t.Parallel()
	pub := netip.MustParseAddr("8.8.8.8")
	r := mapResolver{
		"ip4/public.example": {pub},
	}
	v := ssrf.NewValidatorWithResolver(r, ssrf.Options{})
	ctx := context.Background()
	require.NoError(t, v.ValidateHTTPURL(ctx, "https://public.example/path"))
}

func TestValidator_localhostHostnameBlocked(t *testing.T) {
	t.Parallel()
	v := ssrf.NewValidator(ssrf.Options{})
	ctx := context.Background()
	err := v.ValidateHTTPURL(ctx, "http://localhost:8080/")
	require.Error(t, err)
	require.ErrorIs(t, err, ssrf.ErrBlocked)
}

func TestValidator_literalLoopbackBlocked(t *testing.T) {
	t.Parallel()
	v := ssrf.NewValidator(ssrf.Options{})
	ctx := context.Background()
	require.ErrorIs(t, v.ValidateHTTPURL(ctx, "http://127.0.0.1/"), ssrf.ErrBlocked)
	require.ErrorIs(t, v.ValidateHTTPURL(ctx, "http://[::1]/"), ssrf.ErrBlocked)
}

func TestValidator_privateIPv4Blocked(t *testing.T) {
	t.Parallel()
	v := ssrf.NewValidator(ssrf.Options{})
	ctx := context.Background()
	require.ErrorIs(t, v.ValidateHTTPURL(ctx, "http://192.168.1.1/"), ssrf.ErrBlocked)
	require.ErrorIs(t, v.ValidateHTTPURL(ctx, "http://10.0.0.5/"), ssrf.ErrBlocked)
	require.ErrorIs(t, v.ValidateHTTPURL(ctx, "http://172.16.0.1/"), ssrf.ErrBlocked)
}

func TestValidator_cloudMetadataBlocked(t *testing.T) {
	t.Parallel()
	v := ssrf.NewValidator(ssrf.Options{})
	ctx := context.Background()
	require.ErrorIs(t, v.ValidateHTTPURL(ctx, "http://169.254.169.254/latest/meta-data/"), ssrf.ErrBlocked)
	require.ErrorIs(t, v.ValidateHTTPURL(ctx, "http://metadata.google.internal/computeMetadata/v1/"), ssrf.ErrBlocked)
}

func TestValidator_blockedHostnameTrailingRootLabelDot(t *testing.T) {
	t.Parallel()
	v := ssrf.NewValidator(ssrf.Options{})
	ctx := context.Background()
	require.ErrorIs(t, v.ValidateHTTPURL(ctx, "http://metadata.google.internal./computeMetadata/v1/"), ssrf.ErrBlocked)
}

func TestValidator_publicHTTPSucceeds_trailingDotInHostnameUsesCanonicalResolverKey(t *testing.T) {
	t.Parallel()
	pub := netip.MustParseAddr("8.8.8.8")
	r := mapResolver{
		"ip4/public.example": {pub},
	}
	v := ssrf.NewValidatorWithResolver(r, ssrf.Options{})
	ctx := context.Background()
	require.NoError(t, v.ValidateHTTPURL(ctx, "https://public.example./path"))
}

func TestValidator_allowDomainConfigTrailingDotMatches(t *testing.T) {
	t.Parallel()
	v := ssrf.NewValidatorWithResolver(mapResolver{}, ssrf.Options{
		AllowDomains: []string{"internal.company.internal."},
	})
	ctx := context.Background()
	require.NoError(t, v.ValidateHTTPURL(ctx, "https://cdn.internal.company.internal/media"))
}

func TestValidator_localtestMeBlocked(t *testing.T) {
	t.Parallel()
	v := ssrf.NewValidator(ssrf.Options{})
	ctx := context.Background()
	require.ErrorIs(t, v.ValidateHTTPURL(ctx, "http://localtest.me/"), ssrf.ErrBlocked)
	require.ErrorIs(t, v.ValidateHTTPURL(ctx, "http://x.localtest.me/"), ssrf.ErrBlocked)
}

func TestValidator_ipv4MappedBlocked(t *testing.T) {
	t.Parallel()
	v := ssrf.NewValidator(ssrf.Options{})
	ctx := context.Background()
	require.ErrorIs(t, v.ValidateHTTPURL(ctx, "http://[::ffff:192.168.1.1]/"), ssrf.ErrBlocked)
}

// 6to4 (2002::/16) embeds an IPv4 in the IPv6 address; policy must apply to the embedded IPv4.
func TestValidator_6to4EmbedsLoopbackBlocked(t *testing.T) {
	t.Parallel()
	v := ssrf.NewValidator(ssrf.Options{})
	ctx := context.Background()
	// 2002:7f00:1:: encodes 127.0.0.1 per RFC 3056.
	require.ErrorIs(t, v.ValidateHTTPURL(ctx, "http://[2002:7f00:1::]/"), ssrf.ErrBlocked)
}

func TestValidator_6to4EmbedsDocumentationNetBlocked(t *testing.T) {
	t.Parallel()
	v := ssrf.NewValidator(ssrf.Options{})
	ctx := context.Background()
	// 192.0.2.4 -> 2002:c000:0204::
	require.ErrorIs(t, v.ValidateHTTPURL(ctx, "http://[2002:c000:0204::]/"), ssrf.ErrBlocked)
}

func TestValidator_dnsResolvedPrivateBlocked(t *testing.T) {
	t.Parallel()
	priv := netip.MustParseAddr("192.168.2.2")
	r := mapResolver{
		"ip4/evil.example": {priv},
	}
	v := ssrf.NewValidatorWithResolver(r, ssrf.Options{})
	ctx := context.Background()
	err := v.ValidateHTTPURL(ctx, "http://evil.example/")
	require.ErrorIs(t, err, ssrf.ErrBlocked)
}

func TestValidator_allowDomainSkipsResolution(t *testing.T) {
	t.Parallel()
	v := ssrf.NewValidatorWithResolver(mapResolver{}, ssrf.Options{
		AllowDomains: []string{"internal.company.internal"},
	})
	ctx := context.Background()
	require.NoError(t, v.ValidateHTTPURL(ctx, "https://cdn.internal.company.internal/media"))
}

func TestValidator_allowIPBypass(t *testing.T) {
	t.Parallel()
	ip := netip.MustParseAddr("192.168.55.55")
	v := ssrf.NewValidator(ssrf.Options{AllowIPs: []netip.Addr{ip}})
	ctx := context.Background()
	require.NoError(t, v.ValidateHTTPURL(ctx, "http://192.168.55.55:8080/foo"))
}

func TestValidator_userinfoRejected(t *testing.T) {
	t.Parallel()
	v := ssrf.NewValidator(ssrf.Options{})
	ctx := context.Background()
	err := v.ValidateHTTPURL(ctx, "http://user@example.com/")
	require.ErrorIs(t, err, ssrf.ErrBlocked)
}

func TestValidator_multicastOptional(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	vOff := ssrf.NewValidator(ssrf.Options{BlockMulticast: false})
	require.NoError(t, vOff.ValidateHTTPURL(ctx, "http://224.0.0.1/"))

	vOn := ssrf.NewValidator(ssrf.Options{BlockMulticast: true})
	require.ErrorIs(t, vOn.ValidateHTTPURL(ctx, "http://224.0.0.1/"), ssrf.ErrBlocked)
}

func TestValidator_nonHTTPScheme(t *testing.T) {
	t.Parallel()
	v := ssrf.NewValidator(ssrf.Options{})
	ctx := context.Background()
	err := v.ValidateHTTPURL(ctx, "ftp://example.com/")
	require.ErrorIs(t, err, ssrf.ErrBlocked)
}

func TestValidator_dnsResolutionFailsWithErrResolutionFailed(t *testing.T) {
	t.Parallel()
	v := ssrf.NewValidatorWithResolver(mapResolver{}, ssrf.Options{})
	ctx := context.Background()
	err := v.ValidateHTTPURL(ctx, "http://nx-no-such-host.example.invalid/")
	require.Error(t, err)
	require.ErrorIs(t, err, ssrf.ErrResolutionFailed)
	require.False(t, errors.Is(err, ssrf.ErrBlocked))
}

func TestValidator_allowIPv4MappedLiteralMatchesIPv4AllowIP(t *testing.T) {
	t.Parallel()
	ip := netip.MustParseAddr("192.168.55.55")
	v := ssrf.NewValidator(ssrf.Options{AllowIPs: []netip.Addr{ip}})
	ctx := context.Background()
	require.NoError(t, v.ValidateHTTPURL(ctx, "http://[::ffff:192.168.55.55]:8080/foo"))
}

func TestValidateAllowlistDomainEntry_rejectsBareSuffix(t *testing.T) {
	require.NoError(t, ssrf.ValidateAllowlistDomainEntry(""))
	require.NoError(t, ssrf.ValidateAllowlistDomainEntry("   "))
	require.NoError(t, ssrf.ValidateAllowlistDomainEntry("cdn.example.com"))
	require.NoError(t, ssrf.ValidateAllowlistDomainEntry("cdn.example.com."))
	require.Error(t, ssrf.ValidateAllowlistDomainEntry("127.0.0.1"))
	require.Error(t, ssrf.ValidateAllowlistDomainEntry("169.254.169.254"))
	require.Error(t, ssrf.ValidateAllowlistDomainEntry("2001:db8::1"))
	require.Error(t, ssrf.ValidateAllowlistDomainEntry("::ffff:192.168.1.1"))
	require.Error(t, ssrf.ValidateAllowlistDomainEntry("com"))
	require.Error(t, ssrf.ValidateAllowlistDomainEntry("org"))
	require.Error(t, ssrf.ValidateAllowlistDomainEntry("com."))
}
