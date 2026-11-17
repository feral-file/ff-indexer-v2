//go:build integration

package store

import (
	"fmt"
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const migration035Path = "../../db/migrations/035_ipfs_directory_slash.sql"

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
	runPSQLFile(t, conn, migration035Path)

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
		before := q(`SELECT count(*) FROM token_events`)
		runPSQLFile(t, conn, migration035Path)
		assert.Equal(t, before, q(`SELECT count(*) FROM token_events`))
	})
}
