// Command probeaudit re-probes production render-probe failures on this machine using
// the indexer's OWN renderer and classifier, under a 2x2 of GL backend x settle window.
//
// Purpose: a prod "stalled"/"blank" verdict is an observation from one specific machine —
// headless, software GL (swiftshader), containerized, SSRF-intercepted. A player renders
// on real hardware. This tool separates three explanations for a failing URL:
//
//	needs a GPU        -> fails on swiftshader, succeeds with GPU
//	paints late        -> fails at settle 15s, succeeds at settle 45s
//	genuinely broken   -> fails in every configuration
//
// All configs share one generous timeout so the budget is not a variable; elapsed time is
// recorded instead, which tells you retroactively whether prod's 90s would have sufficed.
//
// Usage:
//
//	go run ./tools/probeaudit -in candidates.csv -out report -shots
//	go run ./tools/probeaudit -url https://example.com/work.html
package main

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"flag"
	"fmt"
	"image/png"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/feral-file/ff-indexer-v2/internal/adapter"
	"github.com/feral-file/ff-indexer-v2/internal/media/probe"

	"github.com/chromedp/chromedp"
)

// prodFingerprints mirrors render_probe.known_bad_fingerprints in the deployed config,
// so a frame this tool classifies gets the same verdict ladder production applies.
var prodFingerprints = []struct {
	phash string
	dist  int
	label string
}{
	{"0x9f1f2f616060687d", 6, "kubo-error-page-500"},
	{"0x9f1f3f7170706070", 6, "kubo-error-page-504"},
	{"0xcfcf6630303899d9", 6, "arweave-404"},
	{"0x9c1e1e1e1e1e1e1e", 6, "onchfs-not-found"},
	{"0x8f0f0f0f0f078787", 6, "onchfs-bad-uri"},
	{"0xe76f18183d697868", 6, "ipfs-dir-listing"},
}

const (
	blankVarianceThreshold = 0.001 // render_probe.blank_variance_threshold
	viewportW              = 1024  // render_probe.viewport_width
	viewportH              = 1024  // render_probe.viewport_height
	prodTimeoutMs          = 90000 // render_probe.timeout_ms after #138
	prodSettleMs           = 15000 // render_probe.settle_ms
	auditTimeoutMs         = 180000
	longSettleMs           = 45000
)

// variant is one cell of the 2x2. Timeout is held constant across all of them so the
// only manipulated factors are the GL backend and the settle window.
type variant struct {
	name     string
	gpu      bool // true = let chromium pick a real backend (player-like)
	settleMs int
}

var variants = []variant{
	{"swiftshader/settle15", false, prodSettleMs}, // faithful to production today
	{"swiftshader/settle45", false, longSettleMs},
	{"gpu/settle15", true, prodSettleMs},
	{"gpu/settle45", true, longSettleMs},
}

// result is one (url, variant) observation.
type result struct {
	URL       string  `json:"url"`
	BaseURL   string  `json:"base_url"`
	Cause     string  `json:"prod_cause"`
	Variant   string  `json:"variant"`
	Verdict   string  `json:"verdict"`
	ElapsedMs int64   `json:"elapsed_ms"`
	MainHTTP  int     `json:"main_http"`
	Variance  float64 `json:"variance"`
	Phash     string  `json:"phash,omitempty"`
	Matched   string  `json:"matched_fingerprint,omitempty"`
	Blocked   int     `json:"blocked_requests"`
	Err       string  `json:"error,omitempty"`
	Shot      string  `json:"screenshot,omitempty"`
}

// playerLikeOptions keeps every containment flag the probe uses but drops the two that
// force software GL, so the only difference from production is the graphics backend.
func playerLikeOptions() []chromedp.ExecAllocatorOption {
	out := make([]chromedp.ExecAllocatorOption, 0, 16)
	out = append(out, probe.AllocatorOptions()...)
	// Later flags win in chromedp's allocator, so re-enabling here overrides the
	// DisableGPU/swiftshader pair set by AllocatorOptions.
	out = append(out,
		chromedp.Flag("disable-gpu", false),
		chromedp.Flag("use-angle", "default"),
		chromedp.Flag("enable-unsafe-swiftshader", false),
	)
	return out
}

func buildFingerprints() ([]probe.Fingerprint, error) {
	fps := make([]probe.Fingerprint, 0, len(prodFingerprints))
	for _, f := range prodFingerprints {
		p, err := probe.ParseFingerprint(f.phash, f.dist, f.label)
		if err != nil {
			return nil, err
		}
		fps = append(fps, p)
	}
	return fps, nil
}

type task struct{ url, base, cause string }

func main() {
	var (
		in       = flag.String("in", "", "candidates CSV (needs a media_url column; base_url/cause optional)")
		one      = flag.String("url", "", "probe a single URL instead of a CSV")
		outDir   = flag.String("out", "probeaudit-out", "output directory")
		shots    = flag.Bool("shots", false, "save a PNG per successful capture")
		limit    = flag.Int("limit", 0, "max URLs (0 = all)")
		parallel = flag.Int("parallel", 2, "URLs in flight; browsers are heavy, keep low")
		only     = flag.String("cause", "", "only rows with this prod cause (timeout|blank|aborted)")
	)
	flag.Parse()

	tasks, err := loadTasks(*in, *one, *only, *limit)
	if err != nil {
		fatal(err)
	}
	if len(tasks) == 0 {
		fatal(fmt.Errorf("no URLs to probe"))
	}
	if err := os.MkdirAll(*outDir, 0o750); err != nil {
		fatal(err)
	}

	fps, err := buildFingerprints()
	if err != nil {
		fatal(err)
	}

	// One renderer per variant, reused across URLs — mirrors how the media worker holds a
	// single allocator and opens a browser context per probe.
	renderers := make(map[string]probe.Renderer, len(variants))
	for _, v := range variants {
		opts := probe.AllocatorOptions()
		if v.gpu {
			opts = playerLikeOptions()
		}
		renderers[v.name] = probe.NewRenderer(adapter.NewChromedpClient(), &probe.RendererConfig{
			ViewportWidth:    viewportW,
			ViewportHeight:   viewportH,
			TimeoutMs:        auditTimeoutMs,
			SettleMs:         v.settleMs,
			AllocatorOptions: opts,
			// SSRFValidator deliberately nil: this machine is not the worker's network
			// position, so interception here would measure a different thing than prod.
			// Recorded as a known divergence in the report header.
		})
	}
	defer func() {
		for _, r := range renderers {
			_ = r.Close()
		}
	}()

	fmt.Fprintf(os.Stderr, "probing %d URLs x %d variants (timeout %ds)\n\n",
		len(tasks), len(variants), auditTimeoutMs/1000)

	var (
		mu      sync.Mutex
		results []result
		done    int
	)
	sem := make(chan struct{}, *parallel)
	var wg sync.WaitGroup

	for _, t := range tasks {
		wg.Add(1)
		go func(t task) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			var local []result
			for _, v := range variants {
				local = append(local, runOne(renderers[v.name], v, t, fps, *outDir, *shots))
			}
			mu.Lock()
			results = append(results, local...)
			done++
			fmt.Fprintf(os.Stderr, "[%d/%d] %s\n  %s\n", done, len(tasks), short(t.url), verdictLine(local))
			mu.Unlock()
		}(t)
	}
	wg.Wait()

	if err := writeReports(*outDir, results); err != nil {
		fatal(err)
	}
	summarize(results)
	fmt.Fprintf(os.Stderr, "\nreports written to %s/\n", *outDir)
}

// runOne executes a single (url, variant) probe through the production renderer and
// classifier, translating a render error into the same "stalled" mapping the executor uses.
func runOne(r probe.Renderer, v variant, t task, fps []probe.Fingerprint, outDir string, shots bool) result {
	res := result{URL: t.url, BaseURL: t.base, Cause: t.cause, Variant: v.name}

	ctx, cancel := context.WithTimeout(context.Background(),
		time.Duration(auditTimeoutMs)*time.Millisecond+30*time.Second)
	defer cancel()

	start := time.Now()
	capture, err := r.RenderProbe(ctx, t.url, v.settleMs)
	res.ElapsedMs = time.Since(start).Milliseconds()

	if err != nil {
		res.Verdict = "stalled"
		res.Err = compact(err.Error())
		return res
	}
	res.MainHTTP = capture.MainStatus
	res.Blocked = capture.BlockedRequests

	// Production refuses to classify a non-2xx document; mirror that exactly.
	if capture.MainStatus != 0 && (capture.MainStatus < 200 || capture.MainStatus >= 300) {
		res.Verdict = "no_evidence"
		res.Err = fmt.Sprintf("main document returned HTTP %d", capture.MainStatus)
		return res
	}

	cl, err := probe.Classify(capture.Image, fps, blankVarianceThreshold)
	if err != nil {
		res.Verdict = "classify_error"
		res.Err = compact(err.Error())
		return res
	}
	res.Verdict = cl.Verdict.String()
	res.Variance = cl.Variance
	res.Phash = fmt.Sprintf("%016x", cl.Phash)
	res.Matched = cl.MatchedLabel

	if shots {
		name := fmt.Sprintf("%s__%s.png", safeName(t.url), strings.NewReplacer("/", "-").Replace(v.name))
		path := filepath.Join(outDir, "shots", name)
		if err := os.MkdirAll(filepath.Dir(path), 0o750); err == nil {
			if f, err := os.Create(path); err == nil { // #nosec G304 -- operator-chosen output dir
				if png.Encode(f, capture.Image) == nil {
					res.Shot = path
				}
				_ = f.Close()
			}
		}
	}
	return res
}

// summarize prints the verdict this audit reaches for each URL: the diagnosis comes from
// WHICH variants succeeded, not from any single run.
func summarize(all []result) {
	byURL := map[string][]result{}
	order := []string{}
	for _, r := range all {
		if _, seen := byURL[r.URL]; !seen {
			order = append(order, r.URL)
		}
		byURL[r.URL] = append(byURL[r.URL], r)
	}

	counts := map[string]int{}
	type row struct{ url, diag, detail string }
	var rows []row

	for _, u := range order {
		rs := byURL[u]
		ok := map[string]result{}
		for _, r := range rs {
			if r.Verdict == "rendered_ok" {
				ok[r.Variant] = r
			}
		}
		swOK := len(ok["swiftshader/settle15"].Variant) > 0
		swLongOK := len(ok["swiftshader/settle45"].Variant) > 0
		gpuOK := len(ok["gpu/settle15"].Variant) > 0
		gpuLongOK := len(ok["gpu/settle45"].Variant) > 0

		var diag, detail string
		switch {
		case swOK:
			e := ok["swiftshader/settle15"].ElapsedMs
			diag = "RENDERS IN PROD CONFIG"
			detail = fmt.Sprintf("%.1fs elapsed; prod 90s budget %s", float64(e)/1000,
				map[bool]string{true: "SUFFICIENT — prod verdict is stale or flaky", false: "TOO SHORT"}[e < prodTimeoutMs])
		case swLongOK:
			diag = "PAINTS LATE"
			detail = fmt.Sprintf("ok at settle 45s (%.1fs) — raise settle_ms, not timeout_ms",
				float64(ok["swiftshader/settle45"].ElapsedMs)/1000)
		case gpuOK || gpuLongOK:
			v := "gpu/settle15"
			if !gpuOK {
				v = "gpu/settle45"
			}
			diag = "NEEDS REAL GPU"
			detail = fmt.Sprintf("ok only with hardware GL (%s, %.1fs) — a player would display this; the probe cannot",
				v, float64(ok[v].ElapsedMs)/1000)
		default:
			diag = "FAILS EVERYWHERE"
			detail = firstErr(rs)
		}
		counts[diag]++
		rows = append(rows, row{u, diag, detail})
	}

	fmt.Println("\n================ SUMMARY ================")
	keys := make([]string, 0, len(counts))
	for k := range counts {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool { return counts[keys[i]] > counts[keys[j]] })
	for _, k := range keys {
		fmt.Printf("%-26s %4d urls\n", k, counts[k])
	}
	fmt.Println("\n---------------- PER URL ----------------")
	for _, r := range rows {
		fmt.Printf("%-26s %s\n      %s\n", r.diag, short(r.url), r.detail)
	}
	fmt.Println(`
NOTE: this machine differs from the prod worker in OS, CPU, container, network
position, and SSRF interception. "NEEDS REAL GPU" and "PAINTS LATE" are strong
signals; "FAILS EVERYWHERE" still warrants an eyeball before calling art broken.`)
}

func writeReports(dir string, rs []result) error {
	jf, err := os.Create(filepath.Join(dir, "results.jsonl")) // #nosec G304 -- operator-chosen output dir
	if err != nil {
		return err
	}
	defer func() { _ = jf.Close() }()
	enc := json.NewEncoder(jf)
	for _, r := range rs {
		if err := enc.Encode(r); err != nil {
			return err
		}
	}

	cf, err := os.Create(filepath.Join(dir, "results.csv")) // #nosec G304 -- operator-chosen output dir
	if err != nil {
		return err
	}
	defer func() { _ = cf.Close() }()
	w := csv.NewWriter(cf)
	defer w.Flush()
	if err := w.Write([]string{"url", "base_url", "prod_cause", "variant", "verdict",
		"elapsed_ms", "main_http", "variance", "phash", "matched", "error"}); err != nil {
		return err
	}
	for _, r := range rs {
		if err := w.Write([]string{r.URL, r.BaseURL, r.Cause, r.Variant, r.Verdict,
			fmt.Sprint(r.ElapsedMs), fmt.Sprint(r.MainHTTP), fmt.Sprintf("%.6f", r.Variance),
			r.Phash, r.Matched, r.Err}); err != nil {
			return err
		}
	}
	return nil
}

func loadTasks(in, one, onlyCause string, limit int) ([]task, error) {
	if one != "" {
		return []task{{url: one, base: one, cause: "manual"}}, nil
	}
	if in == "" {
		return nil, fmt.Errorf("need -in <csv> or -url <url>")
	}
	f, err := os.Open(in) // #nosec G304 -- operator-supplied candidates CSV
	if err != nil {
		return nil, err
	}
	defer func() { _ = f.Close() }()

	rows, err := csv.NewReader(f).ReadAll()
	if err != nil {
		return nil, err
	}
	if len(rows) < 2 {
		return nil, fmt.Errorf("%s: no data rows", in)
	}
	idx := map[string]int{}
	for i, h := range rows[0] {
		idx[strings.TrimSpace(strings.ToLower(h))] = i
	}
	uCol, ok := idx["media_url"]
	if !ok {
		return nil, fmt.Errorf("%s: no media_url column", in)
	}
	get := func(rec []string, name string) string {
		if i, ok := idx[name]; ok && i < len(rec) {
			return rec[i]
		}
		return ""
	}

	var out []task
	for _, rec := range rows[1:] {
		if uCol >= len(rec) || rec[uCol] == "" {
			continue
		}
		c := get(rec, "cause")
		if onlyCause != "" && c != onlyCause {
			continue
		}
		out = append(out, task{url: rec[uCol], base: get(rec, "base_url"), cause: c})
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out, nil
}

func verdictLine(rs []result) string {
	parts := make([]string, 0, len(rs))
	for _, r := range rs {
		s := fmt.Sprintf("%s=%s", strings.SplitN(r.Variant, "/", 2)[0][:2]+strings.TrimPrefix(r.Variant[strings.Index(r.Variant, "/")+1:], "settle"), r.Verdict)
		parts = append(parts, fmt.Sprintf("%s(%.0fs)", s, float64(r.ElapsedMs)/1000))
	}
	return strings.Join(parts, "  ")
}

func firstErr(rs []result) string {
	for _, r := range rs {
		if r.Err != "" {
			return r.Err
		}
	}
	if len(rs) > 0 {
		return fmt.Sprintf("verdict %s, variance %.6f", rs[0].Verdict, rs[0].Variance)
	}
	return ""
}

func compact(s string) string {
	s = strings.ReplaceAll(s, "\n", " ")
	if len(s) > 200 {
		s = s[:200] + "..."
	}
	return s
}

func short(u string) string {
	if len(u) <= 96 {
		return u
	}
	return u[:60] + "..." + u[len(u)-33:]
}

func safeName(u string) string {
	r := strings.NewReplacer("://", "_", "/", "_", "?", "_", "&", "_", "=", "_", ":", "_")
	s := r.Replace(u)
	if len(s) > 120 {
		s = s[:120]
	}
	return s
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "probeaudit:", err)
	os.Exit(1)
}
