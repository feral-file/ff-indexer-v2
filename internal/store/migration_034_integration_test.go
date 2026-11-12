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

const migration034Path = "../../db/migrations/034_onchfs_canonical_uri.sql"

// migration034Fixture seeds one case per behavior the backfill must preserve, using the
// row shapes measured in production on 2026-11-12. Token ids are fixed so assertions can
// name them. R is a 64-hex onchfs root, G its gateway prefix.
const migration034Fixture = `
INSERT INTO tokens (id, token_cid, chain, standard, contract_address, token_number)
SELECT i, 'eip155:1:erc721:0xabc:' || i, 'eip155:1', 'erc721', '0xabc', i::text
FROM generate_series(1, 5) i;

-- T1: Ethereum fxhash work. On-chain animation_url carries the gateway (with the iteration
-- query), OpenSea enrichment repeats it, both broken by DNS; images are healthy IPFS.
INSERT INTO token_metadata (token_id, image_url, image_url_hash, animation_url, animation_url_hash, mime_type)
VALUES (1, 'https://ipfs.example.com/ipfs/QmA', md5('https://ipfs.example.com/ipfs/QmA'),
        'https://onchfs.fxhash2.xyz/b12f18fcd51a21cc3ace5090fa808c8d4e4552d9e2239a3886b89f8ea12ff685/?fxhash=0xb56f&fxiteration=44',
        md5('https://onchfs.fxhash2.xyz/b12f18fcd51a21cc3ace5090fa808c8d4e4552d9e2239a3886b89f8ea12ff685/?fxhash=0xb56f&fxiteration=44'),
        'text/html; charset=utf-8');
INSERT INTO enrichment_sources (token_id, vendor, image_url, image_url_hash, animation_url, animation_url_hash)
VALUES (1, 'opensea', 'https://ipfs.example.com/ipfs/QmA', md5('https://ipfs.example.com/ipfs/QmA'),
        'https://onchfs.fxhash2.xyz/b12f18fcd51a21cc3ace5090fa808c8d4e4552d9e2239a3886b89f8ea12ff685/?fxhash=0xb56f&fxiteration=44',
        md5('https://onchfs.fxhash2.xyz/b12f18fcd51a21cc3ace5090fa808c8d4e4552d9e2239a3886b89f8ea12ff685/?fxhash=0xb56f&fxiteration=44'));
INSERT INTO token_media_health (token_id, media_url, media_url_hash, media_source, health_status, failure_reason, last_error)
SELECT 1, u, md5(u), src, 'broken', 'dns', 'lookup onchfs.fxhash2.xyz: no such host'
FROM (VALUES ('https://onchfs.fxhash2.xyz/b12f18fcd51a21cc3ace5090fa808c8d4e4552d9e2239a3886b89f8ea12ff685/?fxhash=0xb56f&fxiteration=44')) v(u),
     (VALUES ('metadata_animation'), ('enrichment_animation')) s(src);
INSERT INTO token_media_health (token_id, media_url, media_url_hash, media_source, health_status)
VALUES (1, 'https://ipfs.example.com/ipfs/QmA', md5('https://ipfs.example.com/ipfs/QmA'), 'metadata_image', 'healthy');

-- T2: a re-index on the new code already wrote the onchfs:// row next to the old gateway
-- row; the existing onchfs:// row must win the unique key.
INSERT INTO token_media_health (token_id, media_url, media_url_hash, media_source, health_status)
VALUES (2, 'onchfs://c3ed55178eb24cf44b5e8866b07cacb73abf3d23a945e55e47f8878f7f55ec28', md5('onchfs://c3ed55178eb24cf44b5e8866b07cacb73abf3d23a945e55e47f8878f7f55ec28'),
        'metadata_animation', 'healthy'),
       (2, 'https://onchfs.fxhash2.xyz/c3ed55178eb24cf44b5e8866b07cacb73abf3d23a945e55e47f8878f7f55ec28', md5('https://onchfs.fxhash2.xyz/c3ed55178eb24cf44b5e8866b07cacb73abf3d23a945e55e47f8878f7f55ec28'),
        'metadata_animation', 'broken');

-- T3: a gateway row held by a render gate on the old URL; the gate does not travel.
INSERT INTO token_media_health (token_id, media_url, media_url_hash, media_source, health_status, failure_reason, last_error)
VALUES (3, 'https://onchfs.fxhash2.xyz/9fc69d7442768a03445a082aa3472e83f4fff18648c64aa7c424d1b8f69f61d5', md5('https://onchfs.fxhash2.xyz/9fc69d7442768a03445a082aa3472e83f4fff18648c64aa7c424d1b8f69f61d5'),
        'enrichment_animation', 'broken', 'render_stalled', 'stalled');
INSERT INTO media_render_probes (media_url_hash, media_url, verdict, health_gated)
VALUES (md5('https://onchfs.fxhash2.xyz/9fc69d7442768a03445a082aa3472e83f4fff18648c64aa7c424d1b8f69f61d5'),
        'https://onchfs.fxhash2.xyz/9fc69d7442768a03445a082aa3472e83f4fff18648c64aa7c424d1b8f69f61d5', 'stalled', true),
       (md5('https://example.com/kept.html'), 'https://example.com/kept.html', 'rendered_ok', false);

-- T4: a Cloudflare copy of a single-file root, and an unrelated asset.
INSERT INTO media_assets (source_url, source_url_hash, provider, provider_asset_id, variant_urls)
VALUES ('https://onchfs.fxhash2.xyz/a7657bf4b294ef9d22cc12f6dc8099c1ca249b7dc8fb8f5f29935f8bd0f08784',
        md5('https://onchfs.fxhash2.xyz/a7657bf4b294ef9d22cc12f6dc8099c1ca249b7dc8fb8f5f29935f8bd0f08784'), 'cloudflare', 'a-1', '{}'),
       ('https://example.com/other.png', md5('https://example.com/other.png'), 'cloudflare', 'o-1', '{}');

-- T5: a non-onchfs host that only looks similar stays untouched.
INSERT INTO token_metadata (token_id, animation_url, animation_url_hash)
VALUES (5, 'https://example.com/b12f18fcd51a21cc3ace5090fa808c8d4e4552d9e2239a3886b89f8ea12ff685',
        md5('https://example.com/b12f18fcd51a21cc3ace5090fa808c8d4e4552d9e2239a3886b89f8ea12ff685'));
`

// TestMigration034OnChFSCanonicalURI runs db/migrations/034_onchfs_canonical_uri.sql through
// psql, exactly as it is deployed, against a throwaway database: the rewrite, event
// emission, health reset, unique-key dedupe, render-probe and asset handling, and
// idempotency.
func TestMigration034OnChFSCanonicalURI(t *testing.T) {
	if _, err := exec.LookPath("psql"); err != nil {
		t.Skip("psql not available")
	}

	dbName := fmt.Sprintf("m034_%d", time.Now().UnixNano())
	require.NoError(t, testDB.Exec("CREATE DATABASE "+dbName).Error)
	t.Cleanup(func() { testDB.Exec("DROP DATABASE IF EXISTS " + dbName) })
	conn := dsnForDatabase(t, testDSN, dbName)

	runPSQLFile(t, conn, schemaPath)
	runPSQL(t, conn, migration034Fixture)
	runPSQLFile(t, conn, migration034Path)

	q := func(sql string) string { return runPSQL(t, conn, sql) }
	const root1 = "b12f18fcd51a21cc3ace5090fa808c8d4e4552d9e2239a3886b89f8ea12ff685"

	t.Run("no gateway rows remain and hashes match", func(t *testing.T) {
		assert.Equal(t, "0", q(`SELECT
			(SELECT count(*) FROM enrichment_sources WHERE image_url LIKE '%fxhash2%' OR animation_url LIKE '%fxhash2%')
		  + (SELECT count(*) FROM token_metadata WHERE image_url LIKE '%fxhash2%' OR animation_url LIKE '%fxhash2%')
		  + (SELECT count(*) FROM token_media_health WHERE media_url LIKE '%fxhash2%')
		  + (SELECT count(*) FROM media_render_probes WHERE media_url LIKE '%fxhash2%')
		  + (SELECT count(*) FROM media_assets WHERE source_url LIKE '%fxhash2%')`))
		assert.Equal(t, "0", q(`SELECT
			(SELECT count(*) FROM token_media_health WHERE media_url_hash <> md5(media_url))
		  + (SELECT count(*) FROM media_assets WHERE source_url_hash <> md5(source_url))
		  + (SELECT count(*) FROM token_metadata WHERE animation_url_hash <> md5(animation_url))
		  + (SELECT count(*) FROM enrichment_sources WHERE animation_url_hash <> md5(animation_url))`))
	})

	t.Run("URLs become the on-chain onchfs URI with the iteration query", func(t *testing.T) {
		want := "onchfs://" + root1 + "/?fxhash=0xb56f&fxiteration=44"
		assert.Equal(t, want+"|"+want+"|text/html; charset=utf-8",
			q(`SELECT m.animation_url || '|' || e.animation_url || '|' || m.mime_type
			   FROM token_metadata m JOIN enrichment_sources e USING (token_id) WHERE token_id = 1`))
		assert.Equal(t, "https://ipfs.example.com/ipfs/QmA",
			q(`SELECT image_url FROM token_metadata WHERE token_id = 1`), "non-onchfs media is untouched")
	})

	t.Run("each rewrite emits the store's event", func(t *testing.T) {
		assert.Equal(t,
			`enrichment_updated|{"vendor": "opensea", "changed_fields": ["animation_url"]}`+"\n"+
				`metadata_updated|{"changed_fields": ["animation_url"]}`,
			q(`SELECT event_type || '|' || metadata::text FROM token_events WHERE token_id = 1 ORDER BY event_type`))
	})

	t.Run("moved health rows reset for a prompt local re-check", func(t *testing.T) {
		assert.Equal(t, "enrichment_animation|unknown|||1970,metadata_animation|unknown|||1970,metadata_image|healthy|||",
			q(`SELECT string_agg(media_source || '|' || health_status || '|' || coalesce(failure_reason, '') || '|' ||
			          coalesce(last_error, '') || '|' || CASE WHEN last_checked_at = 'epoch' THEN '1970' ELSE '' END,
			          ',' ORDER BY media_source)
			   FROM token_media_health WHERE token_id = 1`))
	})

	t.Run("an existing onchfs row wins the unique key", func(t *testing.T) {
		assert.Equal(t, "onchfs://c3ed55178eb24cf44b5e8866b07cacb73abf3d23a945e55e47f8878f7f55ec28|healthy",
			q(`SELECT string_agg(media_url || '|' || health_status, ',') FROM token_media_health WHERE token_id = 2`))
	})

	t.Run("an old-URL render gate does not travel and its probe is dropped", func(t *testing.T) {
		assert.Equal(t, "unknown|",
			q(`SELECT health_status || '|' || coalesce(failure_reason, '') FROM token_media_health WHERE token_id = 3`))
		assert.Equal(t, "https://example.com/kept.html", q(`SELECT string_agg(media_url, ',') FROM media_render_probes`))
	})

	t.Run("media assets move to the onchfs URI", func(t *testing.T) {
		assert.Equal(t, "a-1|onchfs://a7657bf4b294ef9d22cc12f6dc8099c1ca249b7dc8fb8f5f29935f8bd0f08784,o-1|https://example.com/other.png",
			q(`SELECT string_agg(provider_asset_id || '|' || source_url, ',' ORDER BY provider_asset_id) FROM media_assets`))
	})

	t.Run("lookalike hosts are untouched", func(t *testing.T) {
		assert.Equal(t, "https://example.com/"+root1, q(`SELECT animation_url FROM token_metadata WHERE token_id = 5`))
		assert.Equal(t, "0", q(`SELECT count(*) FROM token_events WHERE token_id = 5`))
	})

	t.Run("re-running is a no-op", func(t *testing.T) {
		before := q(`SELECT count(*) FROM token_events`)
		runPSQLFile(t, conn, migration034Path)
		assert.Equal(t, before, q(`SELECT count(*) FROM token_events`))
	})
}
