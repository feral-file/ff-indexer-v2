package probe

import (
	"context"
	"time"
)

// VerifyNoEgressToForTest exposes the injectable-address egress check so tests can point
// it at a local listener instead of the real metadata endpoint.
var VerifyNoEgressToForTest = func(ctx context.Context, addrs []string, timeout time.Duration) error {
	return verifyNoEgressTo(ctx, addrs, timeout)
}
