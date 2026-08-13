#!/bin/sh
# render-probe-preflight.sh — the executable pre-deployment gate for the L1 render probe.
#
# Run this INSIDE the target CGO image (the container the media worker will run as), in
# the network position it will deploy to. A passing run is the evidence that
# render_probe.egress_restricted attests to; attach the output to the deploy.
# See docs/media_viewability.md "Pre-deployment gate".
#
# Exit code 0 = every check passed. Any failure exits non-zero with the failing section
# printed, so the output is a recordable pass/fail artifact.
set -u

fail=0
section() { printf '\n=== %s ===\n' "$1"; }

section "1/3 chromium smoke suite (real browser, target image)"
if ! go test -tags="cgo chromium" -count=1 -timeout 10m ./internal/media/probe/ -run 'TestChromiumSmoke' -v; then
    echo "FAIL: smoke suite (rendering / capture / classification / interception)"
    fail=1
fi

section "2/3 egress vector measurement (real browser, target image)"
if ! go test -tags="cgo chromium" -count=1 -timeout 10m ./internal/media/probe/ -run 'TestEgressVectors' -v; then
    echo "FAIL: egress vectors (an uncovered browser egress path leaked a request)"
    fail=1
fi

section "3/3 network-layer egress restriction (this network position)"
# The definitive control is the network, not the browser: from where the media worker
# runs, these destinations must be unreachable AT THE NETWORK LAYER. The in-browser
# validator also blocks them, but only the network closes the DNS-rebinding TOCTOU.
# Connection attempts must fail fast (refused/unreachable/filtered), not connect.
for target in "169.254.169.254 80" "10.0.0.1 80" "192.168.0.1 80" "172.16.0.1 80"; do
    host=${target% *}; port=${target#* }
    if nc -z -w 3 "$host" "$port" 2>/dev/null; then
        echo "FAIL: $host:$port is reachable — network egress restriction is NOT in place"
        fail=1
    else
        echo "ok: $host:$port unreachable"
    fi
done
# Loopback: the worker's own ports are expected open from inside the container, so probe
# a port nothing should serve. A hit means loopback scanning is possible from the
# browser's network position.
if nc -z -w 3 127.0.0.1 25 2>/dev/null; then
    echo "WARN: 127.0.0.1:25 reachable — verify no internal service is exposed on loopback"
fi

section "result"
if [ "$fail" -ne 0 ]; then
    echo "PREFLIGHT FAILED — do not set render_probe.egress_restricted for this environment"
    exit 1
fi
echo "PREFLIGHT PASSED — this run is the evidence render_probe.egress_restricted attests to"
echo "Remaining manual step: probe one known-good and one known-bad URL through a worker"
echo "in this environment (docs/media_viewability.md, Pre-deployment gate, step 2)."
