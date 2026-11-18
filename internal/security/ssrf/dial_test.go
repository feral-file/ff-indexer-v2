package ssrf

import (
	"context"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/dns/dnsmessage"
)

func TestCheckDialAddress(t *testing.T) {
	v := NewValidator(Options{AllowIPs: []netip.Addr{netip.MustParseAddr("10.9.9.9")}})

	blocked := []string{
		"127.0.0.1:80",
		"[::1]:443",
		"169.254.169.254:80",
		"10.0.0.1:5432",
		"172.17.0.1:2375",
		"192.168.1.1:80",
		"[::ffff:127.0.0.1]:80",
		"[fe80::1%lo0]:80",
		"example.com:80", // only IP literals reach a socket; anything else is refused
		"127.0.0.1",
	}
	for _, address := range blocked {
		assert.ErrorIs(t, v.CheckDialAddress(address), ErrBlocked, address)
	}

	allowed := []string{
		"93.184.216.34:443",
		"[2606:2800:220:1:248:1893:25c8:1946]:443",
		"10.9.9.9:80", // allowlisted IP inside a blocked range
	}
	for _, address := range allowed {
		assert.NoError(t, v.CheckDialAddress(address), address)
	}
}

// listenLoopback starts a TCP listener on loopback and reports whether it accepted.
func listenLoopback(t *testing.T) (address string, accepted func() bool) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	got := make(chan struct{}, 1)
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			_ = conn.Close()
			select {
			case got <- struct{}{}:
			default:
			}
		}
	}()
	t.Cleanup(func() { _ = ln.Close() })
	return ln.Addr().String(), func() bool {
		select {
		case <-got:
			return true
		case <-time.After(100 * time.Millisecond):
			return false
		}
	}
}

func TestGuardedDialContextRefusesBlockedPeer(t *testing.T) {
	address, accepted := listenLoopback(t)
	_, port, err := net.SplitHostPort(address)
	require.NoError(t, err)
	dial := NewValidator(Options{}).GuardedDialContext(&net.Dialer{Timeout: 2 * time.Second})

	// An IP literal, and a hostname that resolves to loopback only at dial time.
	for _, target := range []string{address, net.JoinHostPort("localhost", port)} {
		conn, err := dial(context.Background(), "tcp", target)
		if conn != nil {
			_ = conn.Close()
		}
		require.ErrorIs(t, err, ErrBlocked, target)
	}
	assert.False(t, accepted(), "no connection may reach the blocked peer")
}

func TestGuardedDialContextHonoursAllowlists(t *testing.T) {
	address, accepted := listenLoopback(t)
	_, port, err := net.SplitHostPort(address)
	require.NoError(t, err)

	t.Run("allowlisted IP", func(t *testing.T) {
		v := NewValidator(Options{AllowIPs: []netip.Addr{netip.MustParseAddr("127.0.0.1")}})
		conn, err := v.GuardedDialContext(&net.Dialer{Timeout: 2 * time.Second})(context.Background(), "tcp", address)
		require.NoError(t, err)
		_ = conn.Close()
		assert.True(t, accepted())
	})

	t.Run("allowlisted domain skips the IP check", func(t *testing.T) {
		v := NewValidator(Options{AllowDomains: []string{"localhost"}})
		conn, err := v.GuardedDialContext(&net.Dialer{Timeout: 2 * time.Second})(context.Background(), "tcp4", net.JoinHostPort("LOCALHOST.", port))
		require.NoError(t, err)
		_ = conn.Close()
		assert.True(t, accepted())
	})
}

// The transport dials the IDNA ASCII form of a hostname, while URL validation sees it as
// written; an allowlisted internationalized domain must match in both spellings.
func TestAllowlistedUnicodeDomainMatchesBothSpellings(t *testing.T) {
	address, accepted := listenLoopback(t)
	_, port, err := net.SplitHostPort(address)
	require.NoError(t, err)

	for _, entry := range []string{"bücher.review.example", "xn--bcher-kva.review.example"} {
		v := NewValidatorWithResolver(staticResolver{"127.0.0.1"}, Options{AllowDomains: []string{entry}})
		for _, host := range []string{"bücher.review.example", "BÜCHER.review.example.", "xn--bcher-kva.review.example", "cdn.xn--bcher-kva.review.example"} {
			assert.True(t, v.hostAllowlisted(canonicalHost(host)), "%s allows %s", entry, host)
			assert.NoError(t, v.ValidateHTTPURL(context.Background(), "https://"+host+"/x"), "%s allows %s", entry, host)
		}
		assert.False(t, v.hostAllowlisted(canonicalHost("bucher.review.example")), entry)
	}

	// At dial time the exemption is taken from the punycode name the transport passes.
	v := NewValidator(Options{AllowDomains: []string{"bücher.review.example"}})
	base := &net.Dialer{Timeout: 2 * time.Second, Resolver: loopbackResolver(t)}
	conn, err := v.GuardedDialContext(base)(context.Background(), "tcp4", net.JoinHostPort("xn--bcher-kva.review.example", port))
	require.NoError(t, err)
	_ = conn.Close()
	assert.True(t, accepted())

	// A name that is not allowlisted and resolves to loopback is still refused.
	_, err = v.GuardedDialContext(base)(context.Background(), "tcp4", net.JoinHostPort("xn--other-kva.review.example", port))
	require.ErrorIs(t, err, ErrBlocked)
}

// A Unicode spelling of a blocked hostname is blocked like its ASCII form.
func TestBlockedHostnameUnicodeSpelling(t *testing.T) {
	v := NewValidatorWithResolver(staticResolver{"93.184.216.34"}, Options{})
	for _, host := range []string{"ｌｏｃａｌｈｏｓｔ", "metadata.google.internal。"} {
		assert.ErrorIs(t, v.ValidateHTTPURL(context.Background(), "https://"+host+"/"), ErrBlocked, host)
	}
}

// staticResolver answers every lookup with the same address.
type staticResolver struct{ ip string }

func (r staticResolver) LookupNetIP(_ context.Context, network, _ string) ([]netip.Addr, error) {
	if network == "ip6" {
		return nil, nil
	}
	return []netip.Addr{netip.MustParseAddr(r.ip)}, nil
}

// loopbackResolver returns a resolver backed by a local DNS responder that answers every
// A query with 127.0.0.1 and every other query with no records.
func loopbackResolver(t *testing.T) *net.Resolver {
	t.Helper()
	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = pc.Close() })
	go func() {
		buf := make([]byte, 512)
		for {
			n, peer, err := pc.ReadFrom(buf)
			if err != nil {
				return
			}
			var query dnsmessage.Message
			if query.Unpack(buf[:n]) != nil || len(query.Questions) == 0 {
				continue
			}
			question := query.Questions[0]
			reply := dnsmessage.Message{
				Header:    dnsmessage.Header{ID: query.ID, Response: true, RecursionAvailable: true},
				Questions: query.Questions,
			}
			if question.Type == dnsmessage.TypeA {
				reply.Answers = []dnsmessage.Resource{{
					Header: dnsmessage.ResourceHeader{Name: question.Name, Type: dnsmessage.TypeA, Class: dnsmessage.ClassINET, TTL: 1},
					Body:   &dnsmessage.AResource{A: [4]byte{127, 0, 0, 1}},
				}}
			}
			if packed, err := reply.Pack(); err == nil {
				_, _ = pc.WriteTo(packed, peer)
			}
		}
	}()
	return &net.Resolver{
		PreferGo: true,
		Dial: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "udp", pc.LocalAddr().String())
		},
	}
}
