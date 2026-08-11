# Media Viewability: Content-Validated Health Checks

Status: L0 and L1 implemented (L1 disabled by default via `render_probe.enabled`).

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
   - not an IPFS gateway directory listing (`directory_listing`, built-in Kubo markers)
   - not a configured known-bad gateway error page (`known_error_page`,
     `uri.known_bad_page_markers`)
   - a declared `image/*`, `video/*`, or `audio/*` type must not sniff as HTML or plain
     text (`type_mismatch`)
   - recognized image containers must parse: PNG IHDR, GIF screen descriptor, WebP RIFF
     fourcc, JPEG segment structure (`container_invalid`). MP4 is left to magic-byte
     sniffing — `moov` may legitimately sit at the end of the file, beyond the probe
     window.

**Conservative-rules contract:** every rule errs healthy on ambiguity. Unknown container
types, insufficient bytes, cross-image-format mislabeling, and declared-HTML/JSON/text
bodies are never flagged. A false broken hides a real artwork from FF1, which is worse
than letting a broken one through until the render probe catches it.

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
| `type_mismatch` | declared media type, text body | validator |
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
audio are excluded (the L0 container check is the meaningful probe there). Never-probed
URLs go first, HTML/animation before images.

Flow: the media health sweeper enqueues `RenderMediaProbe` jobs (unique-keyed per URL)
onto the media queue at the end of each sweep cycle; the CGO media worker renders at a
fixed viewport, waits `settle_ms`, screenshots, and classifies:

- **known_bad_fingerprint** — the frame's pHash is within a configured Hamming distance
  of a known-bad render (directory listing, gateway error page, placeholder). Gates
  immediately: unambiguous, works on the first observation, no baseline needed.
- **blank** — near-zero luminance variance. Gates only after
  `failure_gate_threshold` (default 2) consecutive failures, because slow WebGL under
  software GL and intentionally dark works produce false blanks.
- **stalled** — navigation failure or timeout. Debounced like blank.
- **rendered_ok** — resets the failure counter; if the URL was render-gated, heals it
  (the probe is the *only* healer of `render_*` rows — see the ownership rule).

Gating writes `token_media_health.failure_reason` (`render_blank` / `render_stalled` /
`render_known_bad`) through the same `BatchUpdateTokensViewability` + webhook path as
L0, so consumers see one consistent viewability stream.

**Ownership is enforced in the store, not by convention.** `MediaHealthUpdate.RenderProbeWrite`
marks L1's writes; every other writer is filtered against `failure_reason NOT LIKE 'render_%'`,
so a byte-level healthy verdict — from a metadata reindex, or from a URL re-entering the
sweep because another token added a row for it — cannot clear a browser-confirmed gate.
L1 writes also leave the L0 content-type classification intact, since the render-due query
and the API depend on it surviving a gate.

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

Interception is installed on the page target. Measured against real chromium (see the
smoke test below), that covers the main frame, iframes, and dedicated workers; a popup
would get its own, uncovered target, so new web contents and service/shared workers are
forbidden at launch (`--block-new-web-contents`, `--disable-features=ServiceWorker,SharedWorker`).
Rendering one artwork frame never legitimately needs them.

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

### Pre-enablement smoke (requires a real browser)

Unit tests mock chromedp, so browser behavior is unverified by CI. Before enabling the
probe in an environment, run the build-tagged smoke against the real chromium in the CGO
image — it covers navigation, viewport capture, pHash stability, and SSRF request refusal:

```
go test -tags="cgo chromium" ./internal/media/probe/ -run TestChromiumSmoke -v
```

It is skipped without the `chromium` tag so ordinary `make check` runs stay hermetic.

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
