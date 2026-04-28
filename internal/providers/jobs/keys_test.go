package jobs

import (
	"testing"

	"github.com/feral-file/ff-indexer-v2/internal/types"
)

func TestUniqueKeyFormatters(t *testing.T) {
	t.Parallel()
	if g := keyProcessEvent("ethereum", "0xtx", 7); g != "process-event-ethereum-0xtx-7" {
		t.Fatalf("process event: %q", g)
	}
	if g := keyIndexTokenOwner("tezos", "tz1abc"); g != "index-token-owner-tezos-tz1abc" {
		t.Fatalf("token owner: %q", g)
	}
	u := "https://example.com/a"
	if g := keyMediaFromURL(u); g != "media-"+types.MD5Hash(u) {
		t.Fatalf("media: %q", g)
	}
	if g := keyWebhookNotify("evt-1"); g != "webhook-notify-evt-1" {
		t.Fatalf("webhook notify: %q", g)
	}
	if g := keyWebhookDeliver("cli", "evt-2"); g != "webhook-deliver-cli-evt-2" {
		t.Fatalf("webhook deliver: %q", g)
	}
	if g := keyIndexMetadata("cid-9"); g != "index-metadata-cid-9" {
		t.Fatalf("index metadata: %q", g)
	}
}
