//go:build integration

package store

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	migration032Path = "../../db/migrations/032_feralfile_cdn_move.sql"
	schemaPath       = "../../db/init_pg_db.sql"
)

// migration032Fixture seeds one case per behavior the backfill must preserve. Token ids
// are fixed so assertions can name them.
const migration032Fixture = `
INSERT INTO tokens (id, token_cid, chain, standard, contract_address, token_number)
SELECT i, 'eip155:1:erc721:0xabc:' || i, 'eip155:1', 'erc721', '0xabc', i::text
FROM generate_series(1, 7) i;

-- T1: Feral File enrichment with a thumbnail, a directory-style preview embedding the CDN
-- in a query parameter, and health rows (one healthy, one broken by the DNS outage).
INSERT INTO enrichment_sources (token_id, vendor, image_url, image_url_hash, animation_url, animation_url_hash)
VALUES (1, 'feralfile',
        'https://cdn.feralfileassets.com/thumbnails/a/1', md5('https://cdn.feralfileassets.com/thumbnails/a/1'),
        'https://cdn.feralfileassets.com/previews/a/2/?e=1&clip_directory_url=https://cdn.feralfileassets.com/misc/c',
        md5('https://cdn.feralfileassets.com/previews/a/2/?e=1&clip_directory_url=https://cdn.feralfileassets.com/misc/c'));
INSERT INTO token_media_health (token_id, media_url, media_url_hash, media_source, health_status, failure_reason, last_error)
VALUES (1, 'https://cdn.feralfileassets.com/thumbnails/a/1', md5('https://cdn.feralfileassets.com/thumbnails/a/1'),
        'enrichment_image', 'healthy', NULL, NULL),
       (1, 'https://cdn.feralfileassets.com/previews/a/2/?e=1&clip_directory_url=https://cdn.feralfileassets.com/misc/c',
        md5('https://cdn.feralfileassets.com/previews/a/2/?e=1&clip_directory_url=https://cdn.feralfileassets.com/misc/c'),
        'enrichment_animation', 'broken', 'dns', 'Get "https://cdn.feralfileassets.com/previews/a/2/": no such host');

-- T2: on-chain metadata only.
INSERT INTO token_metadata (token_id, image_url, image_url_hash)
VALUES (2, 'https://cdn.feralfileassets.com/thumbnails/b/1', md5('https://cdn.feralfileassets.com/thumbnails/b/1'));

-- F1 (round 1): old URLs converging on one target in every keyed table.
INSERT INTO token_media_health (token_id, media_url, media_url_hash, media_source, health_status)
VALUES (3, 'https://cdn.feralfileassets.com/x/?e=1', md5('https://cdn.feralfileassets.com/x/?e=1'), 'enrichment_animation', 'healthy'),
       (3, 'https://cdn.feralfileassets.com/x/index.html?e=1', md5('https://cdn.feralfileassets.com/x/index.html?e=1'), 'enrichment_animation', 'healthy');
INSERT INTO media_render_probes (media_url_hash, media_url, verdict, captured_at)
VALUES (md5('https://cdn.feralfileassets.com/y/'), 'https://cdn.feralfileassets.com/y/', 'blank', now() - interval '2 days'),
       (md5('https://cdn.feralfileassets.com/y/index.html'), 'https://cdn.feralfileassets.com/y/index.html', 'rendered_ok', now() - interval '1 day');
INSERT INTO media_assets (source_url, source_url_hash, provider, provider_asset_id, variant_urls)
VALUES ('https://cdn.feralfileassets.com/v/', md5('https://cdn.feralfileassets.com/v/'), 'cloudflare', 'v-1', '{}'),
       ('https://cdn.feralfileassets.com/v/index.html', md5('https://cdn.feralfileassets.com/v/index.html'), 'cloudflare', 'v-2', '{}');

-- F2a (round 1): a gate already held on the NEW url; T4's healthy old row moves onto it.
INSERT INTO media_render_probes (media_url_hash, media_url, verdict, health_gated, last_error, next_check_at)
VALUES (md5('https://cdn.artworks.feralfile.io/w/b.png'), 'https://cdn.artworks.feralfile.io/w/b.png',
        'known_bad_fingerprint', true, 'fingerprint match', now() + interval '6 hours');
INSERT INTO token_media_health (token_id, media_url, media_url_hash, media_source, health_status)
VALUES (4, 'https://cdn.feralfileassets.com/w/b.png', md5('https://cdn.feralfileassets.com/w/b.png'), 'enrichment_image', 'healthy');

-- F2b (round 1): an OLD gated probe moves onto a new url that T5 already uses ungated.
INSERT INTO media_render_probes (media_url_hash, media_url, verdict, health_gated, last_error, next_check_at)
VALUES (md5('https://cdn.feralfileassets.com/z/a.html'), 'https://cdn.feralfileassets.com/z/a.html',
        'blank', true, 'blank frame', now() + interval '6 hours');
INSERT INTO token_media_health (token_id, media_url, media_url_hash, media_source, health_status)
VALUES (5, 'https://cdn.artworks.feralfile.io/z/a.html', md5('https://cdn.artworks.feralfile.io/z/a.html'), 'enrichment_animation', 'healthy');

-- Round 2: new-host probes exist (R1 ungated, R2 gated); T6/T7 still hold old urls, and
-- a render job will gate those old urls mid-run (see migration032Race).
INSERT INTO media_render_probes (media_url_hash, media_url, verdict, health_gated, last_error, next_check_at)
VALUES (md5('https://cdn.artworks.feralfile.io/r1/p.html'), 'https://cdn.artworks.feralfile.io/r1/p.html',
        'rendered_ok', false, NULL, now() + interval '7 days'),
       (md5('https://cdn.artworks.feralfile.io/r2/p.html'), 'https://cdn.artworks.feralfile.io/r2/p.html',
        'known_bad_fingerprint', true, 'fingerprint match', now() + interval '6 hours');
INSERT INTO token_media_health (token_id, media_url, media_url_hash, media_source, health_status)
VALUES (6, 'https://cdn.feralfileassets.com/r1/p.html', md5('https://cdn.feralfileassets.com/r1/p.html'), 'enrichment_animation', 'healthy'),
       (7, 'https://cdn.feralfileassets.com/r2/p.html', md5('https://cdn.feralfileassets.com/r2/p.html'), 'enrichment_animation', 'healthy');
`

// migration032Race simulates render jobs that started against old URLs and call
// AcquireRenderGate after the first render-probe pass: the old-host probe row reappears
// gated and the old URL's health rows turn broken/render_blank.
const migration032Race = `
INSERT INTO media_render_probes (media_url_hash, media_url, verdict, health_gated, last_error, next_check_at)
SELECT md5(u), u, 'blank', true, 'dead host blank', now() + interval '6 hours'
FROM unnest(ARRAY['https://cdn.feralfileassets.com/r1/p.html', 'https://cdn.feralfileassets.com/r2/p.html']) u;
UPDATE token_media_health
SET health_status = 'broken', failure_reason = 'render_blank', last_error = 'dead host blank'
WHERE media_url IN ('https://cdn.feralfileassets.com/r1/p.html', 'https://cdn.feralfileassets.com/r2/p.html');
`

// TestMigration032FeralFileCDNMove runs db/migrations/032_feralfile_cdn_move.sql through
// psql, exactly as it is deployed (own transaction control, psql meta-commands), against a
// throwaway database. It covers the rewrite, event emission, idempotency, and the bot
// review cases on #159: converging URLs (round 1 F1), render-gate inheritance (round 1
// F2), and a render job gating an old URL mid-run (round 2 F1).
func TestMigration032FeralFileCDNMove(t *testing.T) {
	if _, err := exec.LookPath("psql"); err != nil {
		t.Skip("psql not available")
	}

	dbName := fmt.Sprintf("m032_%d", time.Now().UnixNano())
	require.NoError(t, testDB.Exec("CREATE DATABASE "+dbName).Error)
	t.Cleanup(func() { testDB.Exec("DROP DATABASE IF EXISTS " + dbName) })
	conn := dsnForDatabase(t, testDSN, dbName)

	runPSQLFile(t, conn, schemaPath)
	runPSQL(t, conn, migration032Fixture)

	// First run with the race injected between the first render-probe pass and the
	// token batches.
	migration, err := os.ReadFile(migration032Path)
	require.NoError(t, err)
	anchor := "CALL migration_032_move_tokens();\n"
	require.Equal(t, 1, strings.Count(string(migration), anchor))
	raced := strings.Replace(string(migration), anchor, migration032Race+anchor, 1)
	racedPath := filepath.Join(t.TempDir(), "032_raced.sql")
	require.NoError(t, os.WriteFile(racedPath, []byte(raced), 0o600)) //nolint:gosec // G703: path is under t.TempDir()
	runPSQLFile(t, conn, racedPath)

	q := func(sql string) string { return runPSQL(t, conn, sql) }

	t.Run("no old-host rows remain and hashes match", func(t *testing.T) {
		assert.Equal(t, "0", q(`SELECT
			(SELECT count(*) FROM enrichment_sources WHERE image_url LIKE '%feralfileassets%' OR animation_url LIKE '%feralfileassets%')
		  + (SELECT count(*) FROM token_metadata WHERE image_url LIKE '%feralfileassets%' OR animation_url LIKE '%feralfileassets%')
		  + (SELECT count(*) FROM token_media_health WHERE media_url LIKE '%feralfileassets%')
		  + (SELECT count(*) FROM media_render_probes WHERE media_url LIKE '%feralfileassets%')`))
		assert.Equal(t, "0", q(`SELECT
			(SELECT count(*) FROM token_media_health WHERE media_url_hash <> md5(media_url))
		  + (SELECT count(*) FROM media_render_probes WHERE media_url_hash <> md5(media_url))
		  + (SELECT count(*) FROM media_assets WHERE source_url_hash <> md5(source_url))
		  + (SELECT count(*) FROM enrichment_sources WHERE image_url_hash <> md5(image_url) OR animation_url_hash <> md5(animation_url))`))
	})

	t.Run("enrichment rewrite emits the store's event", func(t *testing.T) {
		assert.Equal(t,
			"https://cdn.artworks.feralfile.io/thumbnails/a/1|"+
				"https://cdn.artworks.feralfile.io/previews/a/2/index.html?e=1&clip_directory_url=https://cdn.artworks.feralfile.io/misc/c",
			q(`SELECT image_url || '|' || animation_url FROM enrichment_sources WHERE token_id = 1`))
		assert.Equal(t, `enrichment_updated||{"vendor": "feralfile", "changed_fields": ["image_url", "animation_url"]}`,
			q(`SELECT event_type || '|' || coalesce(owner_address, '') || '|' || metadata::text FROM token_events WHERE token_id = 1`))
		assert.Equal(t, `metadata_updated|{"changed_fields": ["image_url"]}`,
			q(`SELECT event_type || '|' || metadata::text FROM token_events WHERE token_id = 2`))
	})

	t.Run("DNS-outage rows reset for a prompt re-probe, healthy rows keep status", func(t *testing.T) {
		assert.Equal(t, "enrichment_animation|unknown||1970|enrichment_image|healthy||",
			q(`SELECT string_agg(media_source || '|' || health_status || '|' || coalesce(failure_reason, '') || '|' ||
			          CASE WHEN last_checked_at = 'epoch' THEN '1970' ELSE '' END, '|' ORDER BY media_source)
			   FROM token_media_health WHERE token_id = 1`))
	})

	t.Run("round 1 F1: converging URLs keep one row per target", func(t *testing.T) {
		assert.Equal(t, "1", q(`SELECT count(*) FROM token_media_health WHERE token_id = 3`))
		assert.Equal(t, "rendered_ok",
			q(`SELECT verdict FROM media_render_probes WHERE media_url = 'https://cdn.artworks.feralfile.io/y/index.html'`))
		assert.Equal(t, "v-1|https://cdn.artworks.feralfile.io/v/index.html",
			q(`SELECT provider_asset_id || '|' || source_url FROM media_assets WHERE source_url LIKE '%artworks%'`))
	})

	t.Run("round 1 F2: moved rows inherit the new URL's gate", func(t *testing.T) {
		assert.Equal(t, "broken|render_known_bad|fingerprint match",
			q(`SELECT health_status || '|' || failure_reason || '|' || last_error FROM token_media_health WHERE token_id = 4`))
		assert.Equal(t, "broken|render_blank|blank frame",
			q(`SELECT health_status || '|' || failure_reason || '|' || last_error FROM token_media_health WHERE token_id = 5`))
		assert.Equal(t, "t",
			q(`SELECT bool_and(next_check_at <= now()) FROM media_render_probes
			   WHERE media_url IN ('https://cdn.artworks.feralfile.io/w/b.png', 'https://cdn.artworks.feralfile.io/z/a.html')`))
	})

	t.Run("round 2 F1: a mid-run old-URL gate does not strand the moved row", func(t *testing.T) {
		// R1: the new URL holds no gate -> released to unknown for L0, new probe untouched.
		assert.Equal(t, "unknown|",
			q(`SELECT health_status || '|' || coalesce(failure_reason, '') FROM token_media_health WHERE token_id = 6`))
		assert.Equal(t, "rendered_ok|false|true",
			q(`SELECT verdict || '|' || health_gated || '|' || (next_check_at > now() + interval '6 days')
			   FROM media_render_probes WHERE media_url = 'https://cdn.artworks.feralfile.io/r1/p.html'`))
		// R2: the new URL holds a gate -> the row carries that gate, not the dead host's.
		assert.Equal(t, "broken|render_known_bad|fingerprint match",
			q(`SELECT health_status || '|' || failure_reason || '|' || last_error FROM token_media_health WHERE token_id = 7`))
	})

	t.Run("re-running is a no-op", func(t *testing.T) {
		before := q(`SELECT count(*) FROM token_events`)
		runPSQLFile(t, conn, migration032Path)
		assert.Equal(t, before, q(`SELECT count(*) FROM token_events`))
	})
}

// dsnForDatabase points the harness DSN (key=value or URL form) at another database.
func dsnForDatabase(t *testing.T, dsn, dbName string) string {
	t.Helper()
	if strings.Contains(dsn, "://") {
		return regexp.MustCompile(`/[^/?]+(\?|$)`).ReplaceAllString(dsn, "/"+dbName+"$1")
	}
	re := regexp.MustCompile(`dbname=\S+`)
	require.True(t, re.MatchString(dsn), "DSN has no dbname")
	return re.ReplaceAllString(dsn, "dbname="+dbName)
}

// runPSQL executes SQL through psql with ON_ERROR_STOP and returns trimmed unaligned output.
func runPSQL(t *testing.T, conn, sql string) string {
	t.Helper()
	out, err := exec.Command("psql", conn, "-X", "-q", "-At", "-v", "ON_ERROR_STOP=1", "-c", sql).CombinedOutput() //nolint:gosec // G204: test-only, fixed binary, test-controlled args
	require.NoError(t, err, string(out))
	return strings.TrimSpace(string(out))
}

// runPSQLFile executes a SQL file through psql with ON_ERROR_STOP, as deployments do.
func runPSQLFile(t *testing.T, conn, path string) {
	t.Helper()
	out, err := exec.Command("psql", conn, "-X", "-q", "-v", "ON_ERROR_STOP=1", "-f", path).CombinedOutput() //nolint:gosec // G204: test-only, fixed binary, test-controlled args
	require.NoError(t, err, string(out))
}
