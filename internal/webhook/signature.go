package webhook

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"
)

// GenerateSignedPayload generates a signed webhook payload with HMAC-SHA256 signature
// Returns the JSON payload, signature header value, timestamp, and any error
func GenerateSignedPayload(secret string, event WebhookEvent) (payload []byte, signature string, timestamp int64, err error) {
	// Serialize event to JSON
	payload, err = json.Marshal(event)
	if err != nil {
		return nil, "", 0, fmt.Errorf("failed to marshal event: %w", err)
	}

	// Generate timestamp (current Unix timestamp)
	timestamp = time.Now().Unix()

	// Create signature payload: {timestamp}.{event_id}.{json_body}
	// This format allows clients to verify:
	// 1. The timestamp to prevent replay attacks
	// 2. The event ID for deduplication
	// 3. The entire payload integrity
	signaturePayload := fmt.Sprintf("%d.%s.%s", timestamp, event.EventID, string(payload))

	// Generate HMAC-SHA256 signature
	h := hmac.New(sha256.New, []byte(secret))
	h.Write([]byte(signaturePayload))
	signatureBytes := h.Sum(nil)

	// Format as hex string with algorithm prefix
	// Format: "sha256=<hex_signature>"
	signature = "sha256=" + hex.EncodeToString(signatureBytes)

	return payload, signature, timestamp, nil
}
