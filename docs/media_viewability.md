# Media Viewability: Content-Validated Health Checks

Status: L0 and L1 implemented (L1 enabled by default; it runs only on media-enabled deployments and requires the `render_probe.egress_restricted` attestation).

Context: feral-file/feral-file#3485 defines four correctness levels for artwork probes.
This document is the indexer-side contract for Level 0 (byte-level content validation)
and the agreed boundaries for Level 1 (render probe). Related bugs: feral-file#3482
(directory CID listings reported healthy), ff-indexer-v2#76 (assets healthy while 500 in
a browser), ff-indexer-v2#96 (viewable tokens serving broken animation URLs).

## Problem

The health probe treated any 2xx response as healthy and never read the body. "Healthy"
therefore meant "some server answered", not "the artwork's bytes are being served". Every
false-positive class in the linked bugs shares this mechanism.

## Level 0 contract

A URL is healthy only when all of the following hold:

1. The fetch succeeds (2xx; 429 and retryable transport errors are `transient_error`,
   retried next sweep and never persisted).
2. The body's first bytes (up to `uri.probe_max_bytes`, default 32KB, fetched with a
   single ranged GET) pass content validation:
   - not empty, not shorter than the declared length (`zero_length`, `truncated`)
   - not an IPFS gateway directory listing (`directory_listing`). Detection requires a
     conjunction of two independent Kubo template signals, never a lone substring:
     modern Kubo (0.13+) by its gateway meta description plus the path-as-title (both
     inside the probe window even for multi-MB listings, unlike the file table, which
     sits far past it); legacy Kubo by a contiguous "Index of /ipfs..." heading plus
     quoted `class="ipfs-hash"` cells
   - not a configured known-bad gateway error page (`known_error_page`,
     `uri.known_bad_page_markers`)
   - a declared `image/*`, `video/*`, or `audio/*` type must not sniff as HTML, plain
     text, or JSON (`type_mismatch`; JSON covers gateway error bodies served with 200).
     Text-based media subtypes — `+xml`/`+json` suffixes (SVG), playlists, ASCII
     bitmaps — are exempt: for them a text body is expected, and the sniffer inspects
     only a bounded window (3072 bytes at the pinned `mimetype` version), so e.g. an
     SVG with a large preamble before its root element legitimately sniffs as plain
     text
   - recognized image containers must parse: PNG IHDR, GIF screen descriptor, WebP RIFF
     fourcc, JPEG segment structure (`container_invalid`). MP4 is left to magic-byte
     sniffing — `moov` may legitimately sit at the end of the file, beyond the probe
     window.

**Conservative-rules contract:** every rule errs healthy on ambiguity. Unknown container
types, insufficient bytes, cross-image-format mislabeling, and declared-HTML/JSON/text
bodies are never flagged. A false broken hides a real artwork from FF1, which is worse
than letting a broken one through until the render probe catches it.

**Sniffer version sensitivity:** L0 verdicts depend on the pinned
`github.com/gabriel-vasile/mimetype` version — matcher behavior changes between releases
(v1.4.9 detects SVG by scanning for `<svg` in its 3072-byte window; later versions use a
structural matcher that classifies some inputs differently). Bumping the dependency is a
behavior change for the whole corpus: re-run the validator suite deliberately and expect
verdict shifts. Regression tests assert verdicts, never sniffed types, so they hold across
bumps.

The same validated probe drives gateway selection (`FindWorking*Gateway`, used by both
the health checker's fallback and the URI resolver): a gateway "works" only if its
content validates, so a directory listing can no longer be stored as a working URL.

SSRF policy refusals are final and never trigger gateway fallback. DNS failures are
`broken`/`dns` (not retried every tick). Failure classifications are persisted in
`token_media_health.failure_reason` with the observed and sniffed content types
(see `docs/schema.md`).

## Failure taxonomy

| failure_reason | Meaning | Source |
|---|---|---|
| `http_status` | non-2xx response | probe |
| `dns` | host resolution failed | probe |
| `ssrf` | SSRF policy refused the fetch | probe |
| `type_mismatch` | declared binary media type, text/JSON body | validator |
| `container_invalid` | recognized container, corrupt header | validator |
| `directory_listing` | IPFS gateway directory listing | validator |
| `known_error_page` | configured error-page marker matched | validator |
| `zero_length` | empty body | validator |
| `truncated` | body ended before declared length | validator |
| `render_*` | **reserved for the L1 render probe** | render probe |

`failure_reason` is NULL for healthy/unknown rows, and for broken rows whose transport
error has no taxonomy entry (e.g. connection refused). Unclassified is honest: the
column records only causes the probe can actually distinguish.

## L0/L1 ownership rule

Rows whose `failure_reason` starts with `render_` belong to the L1 render probe: the
byte-level sweep must not re-check them (they would pass a ranged GET and flap back to
healthy). Healing a render-gated row is exclusively the render probe's job. This rule
becomes load-bearing when L1 lands; L0 reserves the namespace now so the contract is
stable.

## Level 1: render probe

Headless-chromium render of L0-healthy URLs, gating viewability. Coverage: HTML
documents, animation-source URLs, and images — the classes a render can judge; video and
audio are excluded (the L0 container check is the meaningful probe there; note the
exclusion keys on what the bytes *are*, so a video URL served an HTML error page enters
as HTML). Scheduling runs in three priority tiers, because the corpus is far larger than
render capacity (~356k eligible URLs at rollout) and whatever ranks last waits weeks:

1. **Urgent re-probes** — URLs holding an active gate (the probe is their only healer;
   queueing a heal behind the seeding pass hides a recovered token for weeks) and
   pending blank/stalled debounces (starving the second look makes every accumulated
   first-failure gate in one burst when seeding drains, instead of spread out at the
   designed retry cadence).
2. **Never-probed coverage** — HTML/animation before images (byte checks are weakest for
   HTML, so render coverage matters most there).
3. **Routine rechecks** of rendered_ok URLs. Last deliberately: as seeded URLs come due
   again, ranking any re-probe above coverage would let the seeded prefix monopolize
   capacity and stall the seeding tail indefinitely. A stale re-confirmation of a good
   render is the cheapest thing to postpone.

Flow: the media health sweeper enqueues `RenderMediaProbe` jobs (unique-keyed per URL)
onto the media queue at the end of each sweep cycle; the CGO media worker renders at a
fixed viewport, waits for the page to settle, screenshots, and classifies:

- **known_bad_fingerprint** — the frame's pHash is within a configured Hamming distance
  of a known-bad render (directory listing, gateway error page, placeholder). Gates
  immediately: unambiguous, works on the first observation, no baseline needed.
- **blank** — near-zero luminance variance. Gates only after
  `failure_gate_threshold` (default 2) consecutive failures, because slow WebGL under
  software GL and intentionally dark works produce false blanks.
- **stalled** — navigation failure or timeout. Debounced like blank.
- **rendered_ok** — resets the failure counter; if the URL was render-gated, heals it
  (the probe is the *only* healer of `render_*` rows — see the ownership rule).

The settle window is per-class. `settle_ms` (default 15s) is sized for generative works
that keep painting after load — the blank debounce only protects against *transient*
blanks, so a work that deterministically needs longer than the window would gate on its
second probe no matter how many chances it gets. A static raster image paints on decode,
and images are the majority of the corpus, so holding a browser slot through the full
window for them roughly halves total render throughput for nothing: URLs whose every
health-row signal says static raster image (`IsStaticImageRenderClass`) use
`image_settle_ms` (default 2s) instead. The check is conservative in every ambiguous
direction — SVG is excluded (image by sniff, but SMIL/CSS/script animation needs the
full window), an animation media_source on any row excludes the URL, and unknown or
mixed signals keep the full settle. A wrong shortcut manufactures a blank verdict on
real art; a wrong full settle only costs seconds.

Gating writes `token_media_health.failure_reason` (`render_blank` / `render_stalled` /
`render_known_bad`) through the same `BatchUpdateTokensViewability` + webhook path as
L0, so consumers see one consistent viewability stream.

**The gate is URL-level state, held on `media_render_probes.health_gated`.** Token health
rows are transient — they are deleted when a token stops referencing a URL — so the gate
cannot live only in them. New health rows inherit an active gate from the probe row, and
L1 gate writes also reach `unknown` rows, which is what a token indexed between a probe
being scheduled and its gate write produces. Without both, a newly indexed token sharing
a known-bad URL would be served viewable until the next render recheck.

**Ownership is enforced in the store, not by convention.** `MediaHealthUpdate.RenderProbeWrite`
marks L1's writes; every other writer is filtered against `failure_reason NOT LIKE 'render_%'`,
so a byte-level healthy verdict — from a metadata reindex, or from a URL re-entering the
sweep because another token added a row for it — cannot clear a browser-confirmed gate.
L1 writes also leave the L0 content-type classification intact, since the render-due query
and the API depend on it surviving a gate.

That filter is a check on the health row, and it evaluates under read-committed semantics:
an L0 write whose subquery ran before the gate committed can still land after it. So the
gate is enforced a second time where it actually matters — `BatchUpdateTokensViewability`
excludes URLs holding `health_gated`, in both the animation and the image branch. A URL
chromium has confirmed bad cannot be computed viewable regardless of what its health row
momentarily says. Reconciliation errors after a gate or release commits fail the probe job
rather than being logged: the health state is durable at that point but `tokens.is_viewable`
is not, and no sweep revisits a gated URL to fix it.

**A gated URL is always eligible for its healing probe.** Because the marker locks L0 out
of healing the row, the render probe is the only way back — so render-due eligibility
accepts *either* gate signal: the durable `health_gated` marker, or a `render_%` health
reason. Requiring the health reason alone strands the case where the gate was acquired
over a row L0 already owned (say `failure_reason=http_status`): L1 correctly declines to
overwrite that row, so nothing in it looks render-gated, while the marker still blocks L0
from healing it. That URL would then never be scheduled again and its tokens would stay
non-viewable until manual intervention.

**Disabling the probe releases its gates.** A gate's only healer is a successful render,
so turning the probe off (rollback, misconfigured fingerprints, decommission) would
otherwise strand every gated token as permanently non-viewable — false positives
included. When `render_probe` is disabled, the sweeper releases active gates in batches
instead of enqueueing probes: health rows return to `unknown` and the next L0 sweep
re-verifies the bytes. Turning the probe off withdraws the browser evidence behind the
gates, so the gates are withdrawn with it — released tokens are judged on byte evidence
alone, the pre-L1 status quo. Already-queued `RenderMediaProbe` jobs no-op against a nil
executor rather than failing.

**Scheduling is independent of L0.** Render probes are enqueued on every sweep cycle,
including cycles with no L0 work, because L1 has its own cadence and render-gated rows are
excluded from the L0 query by design. The render-due query selects never-probed URLs by L0
health and class, and already-probed URLs by `next_check_at` alone — which is what lets a
render-gated row (health `broken`) come back for the successful render that is its only
healing path. Routine (non-gated) re-probes still require current L0 health and a
renderable class, so a URL that has since failed L0 stops consuming render capacity.

Job cancellation and worker shutdown are bridged into the browser context and leave all
probe state untouched: a cancelled probe is not evidence about the artwork.

Every capture stores `phash` + `engine_version` + `viewport` in `media_render_probes`
(see `docs/schema.md`); `baseline_phash` keeps the first successful capture and is never
overwritten. Successive-capture drift comparison is deliberately out of scope
(capture-only, per #3485) — the stored history makes it a switch-on later, not a
backfill.

SECURITY: chromium performs its own network I/O, outside the Go HTTP client and its
SSRF RoundTripper. Every browser-initiated request — the navigation, each redirect hop,
and every subresource — is paused via the CDP Fetch domain and validated against the SSRF
policy before it proceeds; refused requests are failed with `AccessDenied` and counted on
the capture. The probe additionally validates the URL up front so an obviously blocked
target never launches a browser context.

The probe launches chromium with its own flags (`probe.AllocatorOptions`), deliberately
**without** `--disable-web-security` — unlike the SVG rasterizer, which only renders bytes
we fetched and validated ourselves. Running untrusted remote pages with web security
disabled would let a hostile page read cross-origin (including private) responses.

Interception is installed on the page target. What it does and does not reach was
**measured against real chromium**, not assumed (`TestEgressVectors` in the smoke suite):

| Vector | Covered by interception |
|---|---|
| main frame, redirects, subresources, iframes | yes |
| dedicated workers, incl. nested | yes |
| `navigator.sendBeacon` | yes |
| popup (`window.open`, `target=_blank`) | **no** — own target |
| shared workers | **no** — own session |
| WebSocket, WebRTC | **no** — CDP Fetch cannot police them |

Rendering one artwork frame needs none of the uncovered kinds, so they are forbidden
rather than policed: `--block-new-web-contents`, `--disable-shared-workers`,
`--disable-features=ServiceWorker`, and a document-start script that removes
`WebSocket`/`RTCPeerConnection`/`EventSource` before any page script runs.

That script also **re-installs itself in every worker**. A worker's global scope is not
the document's, so a page guard alone leaves `WebSocket` intact there — and wrapping only
the page's `Worker` leaves a worker-created worker unguarded. Both were measured
escaping. The guard now serializes its own source as each worker's prologue, so the
protection propagates to arbitrary depth; the original worker script is then pulled in
with `importScripts`, an ordinary fetch that interception still vets, and workers that
cannot be wrapped are refused rather than run unguarded. The smoke test
asserts zero escapes by counting server-side hits on a path the validator refuses, so a
regression shows up as a real request rather than a silent zero.

**This is defense in depth, not a complete boundary.** Two limits remain, both inherent
rather than fixable in this code:

- `ValidateHTTPURL` resolves a hostname at validation time; chromium dials later, so DNS
  rebinding between the two is possible. This is the same documented TOCTOU limitation the
  Go HTTP client's SSRF RoundTripper carries (`internal/adapter/http.go`), not something
  the probe introduces.
- Only connect-time policy can cover every future browser egress path.

The definitive control is therefore **network-level egress restriction for the media
worker** — it should not be able to route to loopback, private, link-local, or
cloud-metadata ranges at all. The in-browser controls above are the layer that makes
ordinary cases fail closed; they are not a substitute.

This is enforced at startup rather than left to documentation: enabling the probe
requires `render_probe.egress_restricted: true`, and the process refuses to start
otherwise. The flag does not implement the control — it attests that the deployment has
it, so shipping the probe without egress restriction is a deliberate decision rather than
an oversight.

Chromium runs **sandboxed** for the probe: unlike the SVG rasterizer, the probe omits
`--single-process` (which disables the renderer sandbox by design) and does not pass
`--no-sandbox`. Runtimes that cannot support the sandbox can set
`render_probe.no_sandbox: true`, which logs a warning at startup — but an unsandboxed
renderer exploit gains the media worker's process access, so prefer fixing the runtime.

Captures are viewport-bounded (`CaptureScreenshot`, not `FullScreenshot`): an untrusted
page can make its document arbitrarily tall, which would make both the work and the pHash
unbounded and unrelated to the recorded viewport. Screenshots are additionally rejected
above 16MB encoded or 16M decoded pixels, checked from the PNG header before the pixel
buffer is allocated.

Data URIs are **out of L1 scope**: their bytes are inline and already validated by L0, and
chromium navigation for them is refused by the SSRF policy. They are excluded in the
render-due query rather than looping as stalled.

Fingerprint workflow for operators: capture the offending page once, compute its pHash,
add it to `render_probe.known_bad_fingerprints` with a small `max_distance` (4-8) and a
label. A loose tolerance matches real art and hides it — the worst failure mode.

### Pre-deployment gate (requires the target runtime)

Unit tests mock chromedp and CI has no chromium, so browser behavior is structurally
unverifiable in this repo's pipeline — the verification belongs to the deployment. The
`egress_restricted` startup requirement is the enforcement hook: the flag attests that
the items below were done for the environment being deployed, and the process refuses to
run the probe until someone sets it. Setting it without doing them is a false
attestation.

`scripts/render-probe-preflight.sh` is the executable form of this gate: run it inside
the target image, in the deployment's network position, and keep the output — a passing
run is the evidence the attestation refers to. It covers steps 1 and 3 below; step 2 is
a manual scenario check.

Before setting `render_probe.egress_restricted: true` in an environment:

1. **Run the smoke suite inside the target CGO image** (not a developer laptop — the
   chromium build, sandbox support, and fontconfig differ). It covers navigation,
   viewport capture, pHash stability, SSRF request refusal, and measures every egress
   vector the interception claims to cover:

   ```
   go test -tags="cgo chromium" ./internal/media/probe/ -run TestChromiumSmoke -v
   go test -tags="cgo chromium" ./internal/media/probe/ -run TestEgressVectors -v
   ```

2. **Probe known-good and known-bad URLs** through a worker running in the environment:
   a real HTML artwork must record `rendered_ok` with a stored pHash; a gateway error
   page probed twice must gate its token (`viewable=false` via the API). This is the
   scenario check that catches runtime-specific rendering differences no unit test can.

3. **Verify the network egress control itself** — from inside the media worker's network
   position, connections to loopback, RFC1918, link-local, and the cloud metadata range
   must fail at the network layer, not merely at the in-browser validator. If that
   control cannot be built for the topology (e.g. chromium shares a container and
   network with services it must reach), leaving the flag unset — and therefore the
   probe off — is the correct state; enabling anyway is a documented risk acceptance,
   not an oversight.

The smoke suite is skipped without the `chromium` tag so ordinary `make check` runs stay
hermetic.

## Delta measurement

Run immediately **before** deploying the L0 change and keep the output ("reported
healthy today"):

```sql
SELECT media_source, health_status, COUNT(*) AS n
FROM token_media_health
GROUP BY media_source, health_status
ORDER BY media_source, health_status;
```

After deploy plus one full sweep, re-run the query above (the corrected number) and
break the gap down by cause:

```sql
SELECT failure_reason, COUNT(*) AS n
FROM token_media_health
WHERE health_status = 'broken'
GROUP BY failure_reason
ORDER BY n DESC;
```

The pair (plus the L1 render check on a sample, when it lands) gives the three numbers
agreed on #3485: previously-reported healthy, validated healthy, actually renders.

## Operational notes

- A new gateway error page: add a marker to `uri.known_bad_page_markers` (config
  reload/deploy, no code change). The scheduled sweeper inherits root `uri` probe
  settings unless the nested `media_health_sweeper.uri` section overrides them, so the
  root key is the single place to set markers. Keep markers specific — exact error
  titles, not generic words — they are matched against HTML artwork bodies too.
- Bandwidth: bounded by sweeper batch size × `probe_max_bytes` per cycle; the probe
  closes without draining bodies larger than the window.
- Rollout: enforcement is immediate by design (user decision on #3485 follow-up). If a
  validation rule misfires in production, the per-reason breakdown identifies affected
  rows exactly; the next sweep after a fix re-heals them.
