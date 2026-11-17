//go:build integration

package store

import (
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/oklog/ulid/v2"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	pgdriver "gorm.io/driver/postgres"
	"gorm.io/gorm"

	"github.com/feral-file/ff-indexer-v2/internal/webhook"
)

const migration035Path = "../../db/migrations/035_ipfs_directory_slash.sql"

// runMigration035 runs the file as the runbook does, with the default token queue.
func runMigration035(t *testing.T, conn string) string {
	t.Helper()
	out, err := migration035Cmd(conn, "-v", "token_queue=token_index").CombinedOutput()
	require.NoError(t, err, string(out))
	return string(out)
}

// migration035Cmd builds the runbook's psql invocation with extra arguments.
func migration035Cmd(conn string, extra ...string) *exec.Cmd {
	args := append([]string{conn, "-X", "-q", "-v", "ON_ERROR_STOP=1"}, extra...)
	args = append(args, "-f", migration035Path)
	return exec.Command("psql", args...) //nolint:gosec // G204: test-only, fixed binary, test-controlled args
}

// newMigration035DB creates a throwaway database with the schema and the given fixture.
func newMigration035DB(t *testing.T, fixture string) (conn, dbName string) {
	t.Helper()
	if _, err := exec.LookPath("psql"); err != nil {
		t.Skip("psql not available")
	}
	dbName = fmt.Sprintf("m035_%d", time.Now().UnixNano())
	require.NoError(t, testDB.Exec("CREATE DATABASE "+dbName).Error)
	t.Cleanup(func() { testDB.Exec("DROP DATABASE IF EXISTS " + dbName) })
	conn = dsnForDatabase(t, testDSN, dbName)
	runPSQLFile(t, conn, schemaPath)
	runPSQL(t, conn, fixture)
	return conn, dbName
}

// migration035OneToken is a single gentk token on the bare shape, healthy.
const migration035OneToken = `
INSERT INTO tokens (id, token_cid, chain, standard, contract_address, token_number, is_viewable)
VALUES (1, 'tezos:mainnet:fa2:KT1KEa8z6vWXDJrVqtMrAeDVzsvxat3kHaCE:1', 'tezos:mainnet', 'fa2', 'KT1KEa8z6vWXDJrVqtMrAeDVzsvxat3kHaCE', '1', true);
INSERT INTO token_metadata (token_id, animation_url, animation_url_hash)
VALUES (1, 'https://ipfs.filebase.io/ipfs/QmUuKiWb5BBK5dERnfZBRebDrtxxkvwFrNYZGs7uwbwSon?fxhash=oo1',
        md5('https://ipfs.filebase.io/ipfs/QmUuKiWb5BBK5dERnfZBRebDrtxxkvwFrNYZGs7uwbwSon?fxhash=oo1'));
INSERT INTO token_media_health (token_id, media_url, media_url_hash, media_source, health_status)
VALUES (1, 'https://ipfs.filebase.io/ipfs/QmUuKiWb5BBK5dERnfZBRebDrtxxkvwFrNYZGs7uwbwSon?fxhash=oo1',
        md5('https://ipfs.filebase.io/ipfs/QmUuKiWb5BBK5dERnfZBRebDrtxxkvwFrNYZGs7uwbwSon?fxhash=oo1'), 'metadata_animation', 'healthy');
`

// TestMigration035RequiresTokenQueue pins that the file refuses to run without the
// deployment's jobs.token_queue, before touching any row: a guessed queue would leave its
// viewability webhooks unclaimed.
func TestMigration035RequiresTokenQueue(t *testing.T) {
	conn, _ := newMigration035DB(t, migration035OneToken)
	out, err := migration035Cmd(conn).CombinedOutput()
	require.Error(t, err)
	assert.Contains(t, string(out), "pass -v token_queue=")
	assert.Equal(t, "0", runPSQL(t, conn, `SELECT count(*) FROM token_metadata WHERE animation_url LIKE '%/?%'`), "nothing was rewritten")
}

// TestMigration035ConcurrentPromotionDoesNotDeadlock replays the interleaving of a live
// gateway promotion (UpdateMediaURLAndPropagate: gate lock, health rows, then
// token_metadata) against the backfill. The promotion holds the token's health row while
// the backfill starts, then updates token_metadata. The backfill must wait on the health
// row without holding token_metadata, so neither side is chosen as a deadlock victim.
func TestMigration035ConcurrentPromotionDoesNotDeadlock(t *testing.T) {
	conn, dbName := newMigration035DB(t, migration035OneToken)
	db, err := gorm.Open(pgdriver.Open(conn), &gorm.Config{})
	require.NoError(t, err)
	sqlDB, err := db.DB()
	require.NoError(t, err)
	t.Cleanup(func() { _ = sqlDB.Close() })

	const bare = "https://ipfs.filebase.io/ipfs/QmUuKiWb5BBK5dERnfZBRebDrtxxkvwFrNYZGs7uwbwSon?fxhash=oo1"
	const promoted = "https://ipfs.feralfile.com/ipfs/QmUuKiWb5BBK5dERnfZBRebDrtxxkvwFrNYZGs7uwbwSon/?fxhash=oo1"

	promotion := db.Begin()
	require.NoError(t, promotion.Exec("SET LOCAL lock_timeout = '20s'").Error)
	require.NoError(t, promotion.Exec("SELECT pg_advisory_xact_lock(hashtext(md5(?)))", promoted).Error)
	require.NoError(t, promotion.Exec(`UPDATE token_media_health SET media_url = ?, media_url_hash = md5(?)
		WHERE media_url_hash = md5(?)`, promoted, promoted, bare).Error)

	type result struct {
		out []byte
		err error
	}
	done := make(chan result, 1)
	go func() {
		out, err := migration035Cmd(conn, "-v", "token_queue=token_index").CombinedOutput()
		done <- result{out, err}
	}()

	// Wait until the backfill is blocked on the promotion's lock.
	require.Eventually(t, func() bool {
		var waiting int64
		testDB.Raw(`SELECT count(*) FROM pg_stat_activity WHERE datname = ? AND wait_event_type = 'Lock'`, dbName).Scan(&waiting)
		return waiting > 0
	}, 30*time.Second, 50*time.Millisecond, "the backfill never reached the promotion's lock")

	err = promotion.Exec(`UPDATE token_metadata SET animation_url = ?, animation_url_hash = md5(?)
		WHERE animation_url_hash = md5(?) AND animation_url IS NOT NULL`, promoted, promoted, bare).Error
	require.NoError(t, err, "the promotion must not be chosen as a deadlock victim")
	require.NoError(t, promotion.Commit().Error)

	res := <-done
	require.NoError(t, res.err, string(res.out))
	assert.NotContains(t, string(res.out), "deadlock", "no retry was needed")
	assert.Equal(t, promoted+"|"+promoted,
		runPSQL(t, conn, `SELECT m.animation_url || '|' || h.media_url FROM token_metadata m
		                  JOIN token_media_health h USING (token_id) WHERE token_id = 1`),
		"the promotion's URL stands; the backfill finds nothing left to rewrite")
}

// migration035Fixture seeds one case per behavior the backfill must preserve, using the
// row shapes of the three blank fxhash works on the PR #178 playlist. Token ids are fixed
// so assertions can name them. B is the bare gentk shape, S its slashed replacement.
const migration035Fixture = `
INSERT INTO tokens (id, token_cid, chain, standard, contract_address, token_number)
SELECT i, 'tezos:mainnet:fa2:KT1KEa8z6vWXDJrVqtMrAeDVzsvxat3kHaCE:' || i, 'tezos:mainnet', 'fa2', 'KT1KEa8z6vWXDJrVqtMrAeDVzsvxat3kHaCE', i::text
FROM generate_series(1, 8) i;

-- T1: gentk work. On-chain animation_url is the bare shape, the objkt enrichment repeats
-- it. The metadata row is healthy (the probe followed the redirect, as in production);
-- the enrichment row holds a blank-render gate, which must not travel. The bare URL has a
-- render probe. The thumbnail is a plain file URL.
INSERT INTO token_metadata (token_id, image_url, image_url_hash, animation_url, animation_url_hash, mime_type)
VALUES (1, 'https://ipfs.example.com/ipfs/QmA', md5('https://ipfs.example.com/ipfs/QmA'),
        'https://ipfs.filebase.io/ipfs/QmUuKiWb5BBK5dERnfZBRebDrtxxkvwFrNYZGs7uwbwSon?fxhash=ooCaGyZk',
        md5('https://ipfs.filebase.io/ipfs/QmUuKiWb5BBK5dERnfZBRebDrtxxkvwFrNYZGs7uwbwSon?fxhash=ooCaGyZk'),
        'text/html');
INSERT INTO enrichment_sources (token_id, vendor, image_url, image_url_hash, animation_url, animation_url_hash)
VALUES (1, 'objkt', 'https://ipfs.example.com/ipfs/QmA', md5('https://ipfs.example.com/ipfs/QmA'),
        'https://ipfs.filebase.io/ipfs/QmUuKiWb5BBK5dERnfZBRebDrtxxkvwFrNYZGs7uwbwSon?fxhash=ooCaGyZk',
        md5('https://ipfs.filebase.io/ipfs/QmUuKiWb5BBK5dERnfZBRebDrtxxkvwFrNYZGs7uwbwSon?fxhash=ooCaGyZk'));
INSERT INTO token_media_health (token_id, media_url, media_url_hash, media_source, health_status, failure_reason, last_error,
                                observed_content_type, last_checked_at)
SELECT 1, u, md5(u), src, st::media_health_status, fr, le, 'text/html; charset=utf-8', '2026-11-16 12:00:00+00'
FROM (VALUES ('https://ipfs.filebase.io/ipfs/QmUuKiWb5BBK5dERnfZBRebDrtxxkvwFrNYZGs7uwbwSon?fxhash=ooCaGyZk')) v(u),
     (VALUES ('metadata_animation', 'healthy', NULL, NULL),
             ('enrichment_animation', 'broken', 'render_blank', 'uniform frame')) s(src, st, fr, le);
INSERT INTO token_media_health (token_id, media_url, media_url_hash, media_source, health_status)
VALUES (1, 'https://ipfs.example.com/ipfs/QmA', md5('https://ipfs.example.com/ipfs/QmA'), 'metadata_image', 'healthy');
INSERT INTO media_render_probes (media_url_hash, media_url, verdict, health_gated)
VALUES (md5('https://ipfs.filebase.io/ipfs/QmUuKiWb5BBK5dERnfZBRebDrtxxkvwFrNYZGs7uwbwSon?fxhash=ooCaGyZk'),
        'https://ipfs.filebase.io/ipfs/QmUuKiWb5BBK5dERnfZBRebDrtxxkvwFrNYZGs7uwbwSon?fxhash=ooCaGyZk', 'blank', true),
       (md5('https://example.com/kept.html'), 'https://example.com/kept.html', 'rendered_ok', false);

-- T2: a re-index on the new code already wrote the slashed row next to the bare row; the
-- existing slashed row must win the unique key.
INSERT INTO token_media_health (token_id, media_url, media_url_hash, media_source, health_status)
VALUES (2, 'https://ipfs.filebase.io/ipfs/QmT78zSuBmuS4z925WZfrqQ1qHaJ56DQaTfyMUF7F8ff5o/?fxhash=oo2',
        md5('https://ipfs.filebase.io/ipfs/QmT78zSuBmuS4z925WZfrqQ1qHaJ56DQaTfyMUF7F8ff5o/?fxhash=oo2'), 'metadata_animation', 'healthy'),
       (2, 'https://ipfs.filebase.io/ipfs/QmT78zSuBmuS4z925WZfrqQ1qHaJ56DQaTfyMUF7F8ff5o?fxhash=oo2',
        md5('https://ipfs.filebase.io/ipfs/QmT78zSuBmuS4z925WZfrqQ1qHaJ56DQaTfyMUF7F8ff5o?fxhash=oo2'), 'metadata_animation', 'healthy');

-- T3: a CIDv1 subdomain gateway URL with a fragment.
INSERT INTO token_metadata (token_id, animation_url, animation_url_hash)
VALUES (3, 'https://bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi.ipfs.dweb.link#view',
        md5('https://bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi.ipfs.dweb.link#view'));

-- T4: processed preview images. The bare one moves; a second bare one whose slashed target
-- already has an asset stays put. Plus an unrelated asset.
INSERT INTO media_assets (source_url, source_url_hash, provider, provider_asset_id, variant_urls)
VALUES ('https://ipfs.example.com/ipfs/QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG?filename=a.png',
        md5('https://ipfs.example.com/ipfs/QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG?filename=a.png'), 'cloudflare', 'a-1', '{}'),
       ('https://ipfs.example.com/ipfs/QmZTR5bcpQD7cFgTorqxZDYaew1Wqgfbd2ud9QqGPAkK2V?filename=b.png',
        md5('https://ipfs.example.com/ipfs/QmZTR5bcpQD7cFgTorqxZDYaew1Wqgfbd2ud9QqGPAkK2V?filename=b.png'), 'cloudflare', 'b-old', '{}'),
       ('https://ipfs.example.com/ipfs/QmZTR5bcpQD7cFgTorqxZDYaew1Wqgfbd2ud9QqGPAkK2V/?filename=b.png',
        md5('https://ipfs.example.com/ipfs/QmZTR5bcpQD7cFgTorqxZDYaew1Wqgfbd2ud9QqGPAkK2V/?filename=b.png'), 'cloudflare', 'b-new', '{}'),
       ('https://example.com/other.png', md5('https://example.com/other.png'), 'cloudflare', 'o-1', '{}');

-- T5: shapes the application leaves alone stay untouched: an entry path before the query,
-- a bare delimiter with nothing after it, a non-gateway URL with a query.
INSERT INTO token_metadata (token_id, image_url, image_url_hash, animation_url, animation_url_hash)
VALUES (5, 'https://ipfs.filebase.io/ipfs/QmUuKiWb5BBK5dERnfZBRebDrtxxkvwFrNYZGs7uwbwSon?',
        md5('https://ipfs.filebase.io/ipfs/QmUuKiWb5BBK5dERnfZBRebDrtxxkvwFrNYZGs7uwbwSon?'),
        'https://ipfs.filebase.io/ipfs/QmUuKiWb5BBK5dERnfZBRebDrtxxkvwFrNYZGs7uwbwSon/index.html?fxhash=oo5',
        md5('https://ipfs.filebase.io/ipfs/QmUuKiWb5BBK5dERnfZBRebDrtxxkvwFrNYZGs7uwbwSon/index.html?fxhash=oo5'));
INSERT INTO enrichment_sources (token_id, vendor, animation_url, animation_url_hash)
VALUES (5, 'opensea', 'https://generator.artblocks.io/1/0xabc/88?x=1', md5('https://generator.artblocks.io/1/0xabc/88?x=1'));

-- T6: after #178 deployed, L1 gated the SLASHED URL (fingerprint) for token 4, which a
-- re-index already moved; token 6 is still on the healthy bare URL and viewable. The gate
-- must travel to token 6, with token 4's reason.
INSERT INTO token_media_health (token_id, media_url, media_url_hash, media_source, health_status, failure_reason, last_error)
VALUES (4, 'https://ipfs.filebase.io/ipfs/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n/?fxhash=oo6',
        md5('https://ipfs.filebase.io/ipfs/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n/?fxhash=oo6'),
        'metadata_animation', 'broken', 'render_known_bad', 'matched fingerprint error-page'),
       (6, 'https://ipfs.filebase.io/ipfs/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n?fxhash=oo6',
        md5('https://ipfs.filebase.io/ipfs/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n?fxhash=oo6'),
        'metadata_animation', 'healthy', NULL, NULL);
INSERT INTO media_render_probes (media_url_hash, media_url, verdict, health_gated, last_error)
VALUES (md5('https://ipfs.filebase.io/ipfs/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n/?fxhash=oo6'),
        'https://ipfs.filebase.io/ipfs/QmdfTbBqBPQ7VNxZEYEj14VmRuZBkqFbiwReogJgS1zR1n/?fxhash=oo6',
        'known_bad_fingerprint', true, 'matched fingerprint error-page');

-- T7: a gated slashed URL no health row references yet (blank verdict); token 7's bare
-- animation row is healthy and its image too. The reason derives from the verdict, and
-- the token stays non-viewable because a gated animation exists.
INSERT INTO token_media_health (token_id, media_url, media_url_hash, media_source, health_status)
VALUES (7, 'https://ipfs.filebase.io/ipfs/QmNsX6YWEtKB87XHNrg357MMQREAShYcVD6qPsT31N6sbP?seed=7',
        md5('https://ipfs.filebase.io/ipfs/QmNsX6YWEtKB87XHNrg357MMQREAShYcVD6qPsT31N6sbP?seed=7'), 'metadata_animation', 'healthy'),
       (7, 'https://ipfs.example.com/ipfs/QmB', md5('https://ipfs.example.com/ipfs/QmB'), 'metadata_image', 'healthy');
INSERT INTO media_render_probes (media_url_hash, media_url, verdict, health_gated, last_error)
VALUES (md5('https://ipfs.filebase.io/ipfs/QmNsX6YWEtKB87XHNrg357MMQREAShYcVD6qPsT31N6sbP/?seed=7'),
        'https://ipfs.filebase.io/ipfs/QmNsX6YWEtKB87XHNrg357MMQREAShYcVD6qPsT31N6sbP/?seed=7', 'blank', true, 'blank frame');
-- T8: a healthy bare animation row next to an existing slashed row that is broken and
-- gated (a re-index on the new code wrote it, then L1 gated it). Dedupe keeps the slashed
-- row, so the token loses its only healthy animation and must be recomputed.
INSERT INTO token_media_health (token_id, media_url, media_url_hash, media_source, health_status, failure_reason, last_error)
VALUES (8, 'https://ipfs.filebase.io/ipfs/QmXdAGaiPKVYZageZKA21i1u9iG6smLAHeLk4e1D1LW5jY?tokenHash=oo8',
        md5('https://ipfs.filebase.io/ipfs/QmXdAGaiPKVYZageZKA21i1u9iG6smLAHeLk4e1D1LW5jY?tokenHash=oo8'),
        'metadata_animation', 'healthy', NULL, NULL),
       (8, 'https://ipfs.filebase.io/ipfs/QmXdAGaiPKVYZageZKA21i1u9iG6smLAHeLk4e1D1LW5jY/?tokenHash=oo8',
        md5('https://ipfs.filebase.io/ipfs/QmXdAGaiPKVYZageZKA21i1u9iG6smLAHeLk4e1D1LW5jY/?tokenHash=oo8'),
        'metadata_animation', 'broken', 'render_blank', 'blank frame');
INSERT INTO media_render_probes (media_url_hash, media_url, verdict, health_gated, last_error)
VALUES (md5('https://ipfs.filebase.io/ipfs/QmXdAGaiPKVYZageZKA21i1u9iG6smLAHeLk4e1D1LW5jY/?tokenHash=oo8'),
        'https://ipfs.filebase.io/ipfs/QmXdAGaiPKVYZageZKA21i1u9iG6smLAHeLk4e1D1LW5jY/?tokenHash=oo8', 'blank', true, 'blank frame');
-- Viewability as BatchUpdateTokensViewability last left it.
UPDATE tokens SET is_viewable = true WHERE id IN (1, 2, 6, 7, 8);
`

// TestMigration035IPFSDirectorySlash runs db/migrations/035_ipfs_directory_slash.sql
// through psql, exactly as it is deployed, against a throwaway database: the rewrite,
// event emission, health reset, unique-key dedupe, render-probe and asset handling, and
// idempotency.
func TestMigration035IPFSDirectorySlash(t *testing.T) {
	if _, err := exec.LookPath("psql"); err != nil {
		t.Skip("psql not available")
	}

	dbName := fmt.Sprintf("m035_%d", time.Now().UnixNano())
	require.NoError(t, testDB.Exec("CREATE DATABASE "+dbName).Error)
	t.Cleanup(func() { testDB.Exec("DROP DATABASE IF EXISTS " + dbName) })
	conn := dsnForDatabase(t, testDSN, dbName)

	runPSQLFile(t, conn, schemaPath)
	runPSQL(t, conn, migration035Fixture)
	runMigration035(t, conn)

	q := func(sql string) string { return runPSQL(t, conn, sql) }
	const slashed = "https://ipfs.filebase.io/ipfs/QmUuKiWb5BBK5dERnfZBRebDrtxxkvwFrNYZGs7uwbwSon/?fxhash=ooCaGyZk"

	t.Run("no bare rows remain and hashes match", func(t *testing.T) {
		assert.Equal(t, "0", q(`SELECT
			(SELECT count(*) FROM enrichment_sources WHERE animation_url ~ '/ipfs/Qm[1-9A-HJ-NP-Za-km-z]{44}[?#].')
		  + (SELECT count(*) FROM token_metadata WHERE animation_url ~ '/ipfs/Qm[1-9A-HJ-NP-Za-km-z]{44}[?#].')
		  + (SELECT count(*) FROM token_media_health WHERE media_url ~ '/ipfs/Qm[1-9A-HJ-NP-Za-km-z]{44}[?#].')
		  + (SELECT count(*) FROM media_render_probes WHERE media_url ~ '/ipfs/Qm[1-9A-HJ-NP-Za-km-z]{44}[?#].')`))
		assert.Equal(t, "0", q(`SELECT
			(SELECT count(*) FROM token_media_health WHERE media_url_hash <> md5(media_url))
		  + (SELECT count(*) FROM media_assets WHERE source_url_hash <> md5(source_url))
		  + (SELECT count(*) FROM token_metadata WHERE animation_url_hash <> md5(animation_url))
		  + (SELECT count(*) FROM enrichment_sources WHERE animation_url_hash <> md5(animation_url))`))
	})

	t.Run("URLs gain the directory slash with the seed kept", func(t *testing.T) {
		assert.Equal(t, slashed+"|"+slashed+"|text/html",
			q(`SELECT m.animation_url || '|' || e.animation_url || '|' || m.mime_type
			   FROM token_metadata m JOIN enrichment_sources e USING (token_id) WHERE token_id = 1`))
		assert.Equal(t, "https://ipfs.example.com/ipfs/QmA",
			q(`SELECT image_url FROM token_metadata WHERE token_id = 1`), "a URL without a query is untouched")
		assert.Equal(t, "https://bafybeigdyrzt5sfp7udm7hu76uh7y26nf3efuylqabf3oclgtqy55fbzdi.ipfs.dweb.link/#view",
			q(`SELECT animation_url FROM token_metadata WHERE token_id = 3`), "subdomain gateways keep their host")
	})

	t.Run("each rewrite emits the store's event", func(t *testing.T) {
		assert.Equal(t,
			`enrichment_updated|{"vendor": "objkt", "changed_fields": ["animation_url"]}`+"\n"+
				`metadata_updated|{"changed_fields": ["animation_url"]}`,
			q(`SELECT event_type || '|' || metadata::text FROM token_events WHERE token_id = 1 ORDER BY event_type`))
	})

	t.Run("moved health rows keep their verdict and only a render gate is reset", func(t *testing.T) {
		// The API serves an animation URL only while its row is healthy, so a healthy row
		// must stay healthy (with its observations) or the work stops displaying.
		assert.Equal(t,
			"enrichment_animation|unknown|||1970|text/html; charset=utf-8,"+
				"metadata_animation|healthy|||kept|text/html; charset=utf-8,"+
				"metadata_image|healthy||||",
			q(`SELECT string_agg(media_source || '|' || health_status || '|' || coalesce(failure_reason, '') || '|' ||
			          coalesce(last_error, '') || '|' ||
			          CASE WHEN last_checked_at = 'epoch' THEN '1970'
			               WHEN last_checked_at = '2026-11-16 12:00:00+00' THEN 'kept' ELSE '' END || '|' ||
			          coalesce(observed_content_type, ''),
			          ',' ORDER BY media_source)
			   FROM token_media_health WHERE token_id = 1`))
		assert.Equal(t, slashed+","+slashed,
			q(`SELECT string_agg(media_url, ',') FROM token_media_health WHERE token_id = 1 AND media_source LIKE '%animation'`))
		assert.Equal(t, "https://example.com/kept.html", q(`SELECT string_agg(media_url, ',') FROM media_render_probes WHERE NOT health_gated`),
			"the bare URL's probe is dropped")
	})

	t.Run("a gate the slashed URL already holds travels to the moved row", func(t *testing.T) {
		assert.Equal(t, "broken|render_known_bad|matched fingerprint error-page",
			q(`SELECT health_status || '|' || failure_reason || '|' || last_error FROM token_media_health WHERE token_id = 6`),
			"the reason comes from the sibling row already on the gated URL")
		assert.Equal(t, "broken|render_blank|blank frame",
			q(`SELECT health_status || '|' || failure_reason || '|' || last_error FROM token_media_health
			   WHERE token_id = 7 AND media_source = 'metadata_animation'`),
			"with no sibling, the reason derives from the probe verdict")
		assert.Equal(t, "6|false,7|false", q(`SELECT string_agg(id || '|' || is_viewable, ',' ORDER BY id) FROM tokens WHERE id IN (6, 7)`))
		assert.Equal(t, `6|{"is_viewable": false}`+"\n"+`7|{"is_viewable": false}`,
			q(`SELECT token_id || '|' || metadata::text FROM token_events
			   WHERE event_type = 'viewability_changed' AND token_id IN (6, 7) ORDER BY token_id`))
		assert.Equal(t, "t", q(`SELECT is_viewable FROM tokens WHERE id = 1`), "tokens without an inherited gate are not recomputed")
	})

	t.Run("a token whose healthy bare row is deduplicated away is recomputed", func(t *testing.T) {
		assert.Equal(t, "https://ipfs.filebase.io/ipfs/QmXdAGaiPKVYZageZKA21i1u9iG6smLAHeLk4e1D1LW5jY/?tokenHash=oo8|broken|render_blank",
			q(`SELECT string_agg(media_url || '|' || health_status || '|' || failure_reason, ',') FROM token_media_health WHERE token_id = 8`))
		assert.Equal(t, `false|{"is_viewable": false}`,
			q(`SELECT t.is_viewable || '|' || e.metadata::text FROM tokens t
			   JOIN token_events e ON e.token_id = t.id AND e.event_type = 'viewability_changed' WHERE t.id = 8`))
		assert.Equal(t, "true|0",
			q(`SELECT (SELECT is_viewable FROM tokens WHERE id = 2) || '|' ||
			          (SELECT count(*) FROM token_events WHERE token_id = 2 AND event_type = 'viewability_changed')`),
			"a dedupe that keeps a healthy row changes nothing")
	})

	t.Run("each viewability change enqueues the sweeper's webhook job", func(t *testing.T) {
		// Downstream search drops a token only on token.viewability.changed; a re-index
		// emits token.indexing.* instead, so the file must enqueue the event itself.
		rows := strings.Split(q(`SELECT queue || '|' || unique_key || '|' || payload::text FROM jobs
		                          WHERE kind = 'NotifyWebhookClients' ORDER BY payload->0->'data'->>'token_number'`), "\n")
		require.Len(t, rows, 3, "one job per changed token (6, 7, 8)")
		for i, row := range rows {
			parts := strings.SplitN(row, "|", 3)
			require.Len(t, parts, 3)
			assert.Equal(t, "token_index", parts[0])

			var args []webhook.WebhookEvent
			require.NoError(t, json.Unmarshal([]byte(parts[2]), &args), "the worker decodes the payload as its job args")
			require.Len(t, args, 1)
			ev := args[0]
			_, err := ulid.ParseStrict(ev.EventID)
			require.NoError(t, err, "event_id is a ULID like the application's")
			assert.Equal(t, "webhook-notify-"+ev.EventID, parts[1])
			assert.Equal(t, webhook.EventTypeTokenViewabilityChanged, ev.EventType)
			assert.WithinDuration(t, time.Now(), ev.Timestamp, 10*time.Minute)

			raw, err := json.Marshal(ev.Data)
			require.NoError(t, err)
			var data webhook.TokenViewabilityChanged
			require.NoError(t, json.Unmarshal(raw, &data))
			number := fmt.Sprint(6 + i)
			assert.Equal(t, webhook.TokenViewabilityChanged{
				EventData: webhook.EventData{
					TokenCID:    "tezos:mainnet:fa2:KT1KEa8z6vWXDJrVqtMrAeDVzsvxat3kHaCE:" + number,
					Chain:       "tezos:mainnet",
					Standard:    "fa2",
					Contract:    "KT1KEa8z6vWXDJrVqtMrAeDVzsvxat3kHaCE",
					TokenNumber: number,
				},
				IsViewable: false,
			}, data)
		}
	})

	t.Run("an existing slashed row wins the unique key", func(t *testing.T) {
		assert.Equal(t, "https://ipfs.filebase.io/ipfs/QmT78zSuBmuS4z925WZfrqQ1qHaJ56DQaTfyMUF7F8ff5o/?fxhash=oo2|healthy",
			q(`SELECT string_agg(media_url || '|' || health_status, ',') FROM token_media_health WHERE token_id = 2`))
	})

	t.Run("media assets move unless the slashed target already has one", func(t *testing.T) {
		assert.Equal(t,
			"a-1|https://ipfs.example.com/ipfs/QmYwAPJzv5CZsnA625s3Xf2nemtYgPpHdWEz79ojWnPbdG/?filename=a.png,"+
				"b-new|https://ipfs.example.com/ipfs/QmZTR5bcpQD7cFgTorqxZDYaew1Wqgfbd2ud9QqGPAkK2V/?filename=b.png,"+
				"b-old|https://ipfs.example.com/ipfs/QmZTR5bcpQD7cFgTorqxZDYaew1Wqgfbd2ud9QqGPAkK2V?filename=b.png,"+
				"o-1|https://example.com/other.png",
			q(`SELECT string_agg(provider_asset_id || '|' || source_url, ',' ORDER BY provider_asset_id) FROM media_assets`))
	})

	t.Run("shapes the application leaves alone are untouched", func(t *testing.T) {
		assert.Equal(t,
			"https://ipfs.filebase.io/ipfs/QmUuKiWb5BBK5dERnfZBRebDrtxxkvwFrNYZGs7uwbwSon?|"+
				"https://ipfs.filebase.io/ipfs/QmUuKiWb5BBK5dERnfZBRebDrtxxkvwFrNYZGs7uwbwSon/index.html?fxhash=oo5|"+
				"https://generator.artblocks.io/1/0xabc/88?x=1",
			q(`SELECT m.image_url || '|' || m.animation_url || '|' || e.animation_url
			   FROM token_metadata m JOIN enrichment_sources e USING (token_id) WHERE token_id = 5`))
		assert.Equal(t, "0", q(`SELECT count(*) FROM token_events WHERE token_id = 5`))
	})

	t.Run("re-running is a no-op", func(t *testing.T) {
		before := q(`SELECT (SELECT count(*) FROM token_events) || '|' || (SELECT count(*) FROM jobs)`)
		runMigration035(t, conn)
		assert.Equal(t, before, q(`SELECT (SELECT count(*) FROM token_events) || '|' || (SELECT count(*) FROM jobs)`))
	})
}
