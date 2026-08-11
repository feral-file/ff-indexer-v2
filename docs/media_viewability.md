# Media Viewability: Content-Validated Health Checks

Status: L0 implemented. L1 (render probe) planned — see "Level 1" below.

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

## Level 1 (planned)

Headless-chromium render of L0-healthy HTML/animation and image URLs, gating viewability:
known-bad render fingerprint (pHash match against directory listings, error pages,
placeholders) gates immediately; blank/stalled verdicts gate after 2 consecutive
failures. Every capture stores `phash` + `engine_version` + viewport; successive-capture
drift comparison is deliberately out of scope for v1 (capture-only, per #3485).

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
