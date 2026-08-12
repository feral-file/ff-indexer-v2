package probe

import (
	"context"
	"encoding/base64"
	"fmt"
	"net"
	"time"

	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
)

// Self-check fixtures are data: URLs so the checks need no network and no server: the
// bytes are the URL. Each one exists to catch a distinct runtime failure that unit tests
// (mocked chromedp) and CI (generic runner, not the target image) structurally cannot:
//
//   - selfCheckGoodHTML: the capture path end to end — chromium launches, paints,
//     screenshots, and the frame classifies rendered_ok. Catches a missing/broken
//     chromium binary, sandbox refusal, and capture regressions in the deployed image.
//   - selfCheckWebGLHTML: the software WebGL backend. It paints ONLY through a WebGL
//     context over a black fallback, so a runtime without a usable backend (the
//     disable-software-rasterizer class of launch-flag regression) classifies blank
//     here instead of render-gating real generative art after deploy.
//   - selfCheckBlankHTML: blank detection itself — a uniform frame must classify blank,
//     or the deployed threshold is not doing what the operator believes.
const (
	selfCheckGoodHTML = `<html><body style="margin:0">` +
		`<div style="width:100vw;height:100vh;background:linear-gradient(135deg,#0af,#f0a)"></div>` +
		`</body></html>`

	selfCheckWebGLHTML = `<html><body style="margin:0;background:#000">` +
		`<canvas id="c" width="512" height="512" style="width:100vw;height:100vh"></canvas>` +
		`<script>
		const gl = document.getElementById('c').getContext('webgl')
			|| document.getElementById('c').getContext('experimental-webgl');
		if (gl) {
			gl.enable(gl.SCISSOR_TEST);
			gl.scissor(0, 0, 256, 512);
			gl.clearColor(0.0, 0.67, 1.0, 1.0);
			gl.clear(gl.COLOR_BUFFER_BIT);
			gl.scissor(256, 0, 256, 512);
			gl.clearColor(1.0, 0.0, 0.67, 1.0);
			gl.clear(gl.COLOR_BUFFER_BIT);
		}
		</script></body></html>`

	selfCheckBlankHTML = `<html><body style="margin:0;background:#000"></body></html>`

	// selfCheckSettleMs keeps the three startup renders short: data: fixtures paint
	// immediately (the WebGL one only needs SwiftShader warm-up), so the production
	// settle window would just slow startup by half a minute for nothing.
	selfCheckSettleMs = 2500
)

// dataURL wraps fixture HTML as a base64 data: URL.
func dataURL(html string) string {
	return "data:text/html;base64," + base64.StdEncoding.EncodeToString([]byte(html))
}

// SelfCheck renders the built-in fixtures through the real renderer and verifies each
// classifies as designed. It is the known-good/known-bad scenario executed in the
// deployment's actual runtime, automatically, before the probe may gate anything.
//
// Reason: unit tests mock chromedp and CI runs a generic runner, so nothing before this
// point has proven the deployed image's chromium binary, sandbox, capture path, WebGL
// backend, or blank threshold. A runtime where any of those is broken does not produce
// errors — it produces wrong verdicts, which after the debounce hide healthy artworks.
// Failing startup converts that silent corpus-wide misclassification into a visible
// deploy failure. Constraints: blankThreshold <= 0 disables blank classification by
// definition, so the blank fixture is skipped rather than reported as a failure.
func SelfCheck(ctx context.Context, r Renderer, blankThreshold float64) error {
	verdictOf := func(name, html string) (schema.RenderProbeVerdict, error) {
		capture, err := r.RenderProbe(ctx, dataURL(html), selfCheckSettleMs)
		if err != nil {
			return "", fmt.Errorf("render probe self-check: %s fixture failed to render: %w", name, err)
		}
		cls, err := Classify(capture.Image, nil, blankThreshold)
		if err != nil {
			return "", fmt.Errorf("render probe self-check: %s fixture failed to classify: %w", name, err)
		}
		return cls.Verdict, nil
	}

	if v, err := verdictOf("known-good", selfCheckGoodHTML); err != nil {
		return err
	} else if v != schema.RenderProbeVerdictRenderedOK {
		return fmt.Errorf("render probe self-check: known-good fixture classified %q, want rendered_ok — the capture path in this runtime cannot be trusted to judge artworks", v)
	}

	if v, err := verdictOf("webgl", selfCheckWebGLHTML); err != nil {
		return err
	} else if v != schema.RenderProbeVerdictRenderedOK {
		return fmt.Errorf("render probe self-check: WebGL fixture classified %q, want rendered_ok — no usable software WebGL backend in this runtime; enabling the probe would render-gate WebGL artworks", v)
	}

	if blankThreshold > 0 {
		if v, err := verdictOf("known-bad", selfCheckBlankHTML); err != nil {
			return err
		} else if v != schema.RenderProbeVerdictBlank {
			return fmt.Errorf("render probe self-check: blank fixture classified %q, want blank — blank detection is not functioning at the configured threshold", v)
		}
	}

	return nil
}

// metadataEndpoints are the cloud metadata addresses no media worker may ever reach.
// One shared address serves AWS, GCP, DigitalOcean, and Azure alike.
var metadataEndpoints = []string{
	"169.254.169.254:80",
	"169.254.169.254:443",
}

// VerifyNoMetadataEgress cross-checks the egress_restricted attestation against the one
// destination whose reachability is unambiguous: the cloud metadata service.
//
// Reason: egress_restricted is an operator claim that network-level egress restriction
// exists; a claim the runtime can partially falsify should be falsified, not trusted.
// Broader private-range sampling is deliberately NOT hard-checked here — a media worker
// may legitimately share a network with its own database — but the metadata endpoint has
// no legitimate reader in this process, is the canonical SSRF credential target, and is
// reachable by default from containers on most cloud hosts. If it connects, the
// attestation is factually false and the probe must not start. Remediation for docker
// hosts is one rule: iptables -I DOCKER-USER -d 169.254.169.254 -j DROP. Constraints:
// unreachability of two sampled ports proves nothing about the rest of the policy — the
// full verification remains the documented pre-deployment gate.
func VerifyNoMetadataEgress(ctx context.Context) error {
	return verifyNoEgressTo(ctx, metadataEndpoints, 1500*time.Millisecond)
}

// verifyNoEgressTo fails if any address accepts a TCP connection within timeout.
func verifyNoEgressTo(ctx context.Context, addrs []string, timeout time.Duration) error {
	dialer := &net.Dialer{Timeout: timeout}
	for _, addr := range addrs {
		conn, err := dialer.DialContext(ctx, "tcp", addr)
		if err == nil {
			_ = conn.Close()
			return fmt.Errorf("render probe self-check: %s is reachable from this worker — the egress_restricted attestation is false; "+
				"block the metadata range at the network layer (docker: iptables -I DOCKER-USER -d 169.254.169.254 -j DROP) "+
				"or disable the render probe", addr)
		}
	}
	return nil
}
