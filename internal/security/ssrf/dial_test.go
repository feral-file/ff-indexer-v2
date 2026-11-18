package ssrf

import (
	"context"
	"net"
	"net/netip"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
