package ssrf

import (
	"context"
	"fmt"
	"net"
	"net/netip"
	"syscall"
)

// CheckDialAddress applies the IP policy to address, the literal "ip:port" a socket is
// about to connect to. It returns an error wrapping ErrBlocked when the IP is refused.
func (v *Validator) CheckDialAddress(address string) error {
	addrPort, err := netip.ParseAddrPort(address)
	if err != nil {
		return fmt.Errorf("%w: dial address %q is not an IP literal: %w", ErrBlocked, address, err)
	}
	ip := addrPort.Addr().WithZone("")
	if v.ipAllowlisted(ip) {
		return nil
	}
	return v.checkAddr(ip)
}

// GuardedDialContext returns a DialContext for an http.Transport that enforces the IP
// policy on the address each connection actually reaches.
//
// Reason: ValidateHTTPURL resolves the hostname before the request, and the transport
// resolves it again when it dials. A DNS-rebinding name answers the first lookup with a
// public address and the second with a loopback, private, or metadata address, so the
// URL check alone lets the connection through. The check here runs in the dialer's
// Control hook, after resolution and before connect, on the IP the socket will use, so
// no second lookup can change the outcome. It also covers every redirect hop and every
// address a dual-stack dial tries.
// Trade-offs: the URL check stays, because it rejects a blocked target before any
// connection attempt and names the reason; this hook is the binding one. A refused
// address surfaces as a dial error wrapping ErrBlocked.
// Constraints: allowlisted domains skip the hook, matching ValidateHTTPURL, where they
// bypass IP checks by design. The hook sees the peer of the TCP connection, so behind an
// HTTP proxy it would judge the proxy's address rather than the target's; outbound
// fetches that need this guard must not run through a proxy.
func (v *Validator) GuardedDialContext(base *net.Dialer) func(ctx context.Context, network, address string) (net.Conn, error) {
	guarded := *base
	guarded.Control = func(_, address string, _ syscall.RawConn) error {
		return v.CheckDialAddress(address)
	}
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		host, _, err := net.SplitHostPort(address)
		if err == nil && v.hostAllowlisted(canonicalHost(host)) {
			return base.DialContext(ctx, network, address)
		}
		return guarded.DialContext(ctx, network, address)
	}
}
