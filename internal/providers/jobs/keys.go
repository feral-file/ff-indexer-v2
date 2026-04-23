package jobs

import (
	"fmt"
	"strconv"

	"github.com/feral-file/ff-indexer-v2/internal/types"
)

// Unique-key helpers (deduplication) for jobs rows. They stay package-private; producers outside
// this package can mirror the same string templates (see product queue documentation).

func keyProcessEvent(chain, txHash string, logIndex uint64) string {
	return fmt.Sprintf("process-event-%s-%s-%s", chain, txHash, strconv.FormatUint(logIndex, 10))
}

func keyIndexTokenOwner(chain, address string) string {
	return fmt.Sprintf("index-token-owner-%s-%s", chain, address)
}

func keyMediaFromURL(u string) string {
	return "media-" + types.MD5Hash(u)
}

func keyWebhookNotify(eventID string) string {
	return "webhook-notify-" + eventID
}

func keyWebhookDeliver(clientID, eventID string) string {
	return fmt.Sprintf("webhook-deliver-%s-%s", clientID, eventID)
}

func keyIndexMetadata(tokenCID string) string {
	return "index-metadata-" + tokenCID
}
