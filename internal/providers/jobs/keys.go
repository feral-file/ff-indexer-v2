package jobs

import (
	"fmt"
	"strconv"

	"github.com/feral-file/ff-indexer-v2/internal/domain"
	"github.com/feral-file/ff-indexer-v2/internal/types"
)

// Unique-key helpers (deduplication) for jobs rows.

func keyProcessEvent(chain, txHash string, logIndex uint64) string {
	return fmt.Sprintf("process-event-%s-%s-%s", chain, txHash, strconv.FormatUint(logIndex, 10))
}

// ProcessEventUniqueKey is the active-job deduplication key for chain-ingestion enqueues.
func ProcessEventUniqueKey(chain domain.Chain, txHash string, logIndex uint64) string {
	return keyProcessEvent(string(chain), txHash, logIndex)
}

func keyIndexTokenOwner(chain, address string) string {
	return fmt.Sprintf("index-token-owner-%s-%s", chain, address)
}

// IndexTokenOwnerUniqueKey is the active-job key for per-address owner indexing.
func IndexTokenOwnerUniqueKey(chain domain.Chain, address string) string {
	return keyIndexTokenOwner(string(chain), address)
}

func keyMediaFromURL(u string) string {
	return "media-" + types.MD5Hash(u)
}

func keyWebhookNotify(eventID string) string {
	return "webhook-notify-" + eventID
}

// WebhookNotifyUniqueKey is the active-job key for viewability-driven webhook batch notifications.
func WebhookNotifyUniqueKey(eventID string) string {
	return keyWebhookNotify(eventID)
}

func keyWebhookDeliver(clientID, eventID string) string {
	return fmt.Sprintf("webhook-deliver-%s-%s", clientID, eventID)
}

func keyIndexMetadata(tokenCID string) string {
	return "index-metadata-" + tokenCID
}
