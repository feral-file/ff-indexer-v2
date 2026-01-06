package webhook_test

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/feral-file/ff-indexer-v2/internal/webhook"
)

func TestGenerateSignedPayload(t *testing.T) {
	t.Run("generates valid payload and signature", func(t *testing.T) {
		hexSecret := "746573742d7365637265742d6b6579"
		event := webhook.WebhookEvent{
			EventID:   "01JG8XAMPLE1234567890123456",
			EventType: "token.indexing.queryable",
			Timestamp: time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC),
			Data: webhook.EventData{
				TokenCID:    "eip155:1:erc721:0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D:1",
				Chain:       "eip155:1",
				Standard:    "erc721",
				Contract:    "0xBC4CA0EdA7647A8aB7C2061c2E118A18a936f13D",
				TokenNumber: "1",
			},
		}

		payload, signature, timestamp, err := webhook.GenerateSignedPayload(hexSecret, event)
		require.NoError(t, err)

		// Verify payload is valid JSON
		var parsedEvent webhook.WebhookEvent
		err = json.Unmarshal(payload, &parsedEvent)
		require.NoError(t, err)
		assert.Equal(t, event.EventID, parsedEvent.EventID)
		assert.Equal(t, event.EventType, parsedEvent.EventType)

		// Verify signature format
		assert.Contains(t, signature, "sha256=")
		assert.Greater(t, len(signature), 7) // "sha256=" + hash

		// Verify timestamp is reasonable (within last few seconds)
		now := time.Now().Unix()
		assert.GreaterOrEqual(t, now, timestamp)
		assert.Less(t, now-timestamp, int64(5))

		// Verify signature can be validated
		signaturePayload := fmt.Sprintf("%d.%s.%s", timestamp, event.EventID, string(payload))
		secretBytes, err := hex.DecodeString(hexSecret)
		require.NoError(t, err)
		h := hmac.New(sha256.New, secretBytes)
		h.Write([]byte(signaturePayload))
		expectedSignature := "sha256=" + hex.EncodeToString(h.Sum(nil))
		assert.Equal(t, expectedSignature, signature)
	})

	t.Run("different events produce different signatures", func(t *testing.T) {
		hexSecret := "746573742d7365637265742d6b6579"

		event1 := webhook.WebhookEvent{
			EventID:   "01JG8XAMPLE1111111111111111",
			EventType: "token.indexing.queryable",
			Timestamp: time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC),
			Data: webhook.EventData{
				TokenCID:    "eip155:1:erc721:0xABC:1",
				Chain:       "eip155:1",
				Standard:    "erc721",
				Contract:    "0xABC",
				TokenNumber: "1",
			},
		}

		event2 := webhook.WebhookEvent{
			EventID:   "01JG8XAMPLE2222222222222222",
			EventType: "token.indexing.viewable",
			Timestamp: time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC),
			Data: webhook.EventData{
				TokenCID:    "eip155:1:erc721:0xDEF:2",
				Chain:       "eip155:1",
				Standard:    "erc721",
				Contract:    "0xDEF",
				TokenNumber: "2",
			},
		}

		_, signature1, _, err := webhook.GenerateSignedPayload(hexSecret, event1)
		require.NoError(t, err)

		_, signature2, _, err := webhook.GenerateSignedPayload(hexSecret, event2)
		require.NoError(t, err)

		// Signatures should be different
		assert.NotEqual(t, signature1, signature2)
	})

	t.Run("different secrets produce different signatures", func(t *testing.T) {
		event := webhook.WebhookEvent{
			EventID:   "01JG8XAMPLE1234567890123456",
			EventType: "token.indexing.queryable",
			Timestamp: time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC),
			Data: webhook.EventData{
				TokenCID:    "eip155:1:erc721:0xABC:1",
				Chain:       "eip155:1",
				Standard:    "erc721",
				Contract:    "0xABC",
				TokenNumber: "1",
			},
		}

		// Hex-encoded secrets (hex encodings of "secret1" and "secret2")
		_, signature1, _, err := webhook.GenerateSignedPayload("73656372657431", event) // "secret1" in hex
		require.NoError(t, err)

		_, signature2, _, err := webhook.GenerateSignedPayload("73656372657432", event) // "secret2" in hex
		require.NoError(t, err)

		// Signatures should be different
		assert.NotEqual(t, signature1, signature2)
	})

	t.Run("signature includes event_id to prevent replay", func(t *testing.T) {
		hexSecret := "746573742d7365637265742d6b6579"

		// Same event data but different event IDs
		baseData := webhook.EventData{
			TokenCID:    "eip155:1:erc721:0xABC:1",
			Chain:       "eip155:1",
			Standard:    "erc721",
			Contract:    "0xABC",
			TokenNumber: "1",
		}

		event1 := webhook.WebhookEvent{
			EventID:   "01JG8XAMPLE1111111111111111",
			EventType: "token.indexing.queryable",
			Timestamp: time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC),
			Data:      baseData,
		}

		event2 := webhook.WebhookEvent{
			EventID:   "01JG8XAMPLE2222222222222222",
			EventType: "token.indexing.queryable",
			Timestamp: time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC),
			Data:      baseData,
		}

		_, signature1, _, err := webhook.GenerateSignedPayload(hexSecret, event1)
		require.NoError(t, err)

		_, signature2, _, err := webhook.GenerateSignedPayload(hexSecret, event2)
		require.NoError(t, err)

		// Signatures should be different because event IDs are different
		assert.NotEqual(t, signature1, signature2, "Different event IDs should produce different signatures")
	})

	t.Run("empty secret still produces valid signature", func(t *testing.T) {
		hexSecret := ""
		event := webhook.WebhookEvent{
			EventID:   "01JG8XAMPLE1234567890123456",
			EventType: "token.indexing.queryable",
			Timestamp: time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC),
			Data: webhook.EventData{
				TokenCID:    "eip155:1:erc721:0xABC:1",
				Chain:       "eip155:1",
				Standard:    "erc721",
				Contract:    "0xABC",
				TokenNumber: "1",
			},
		}

		payload, signature, timestamp, err := webhook.GenerateSignedPayload(hexSecret, event)
		require.NoError(t, err)
		assert.NotEmpty(t, payload)
		assert.NotEmpty(t, signature)
		assert.NotZero(t, timestamp)
	})

	t.Run("signature can be verified by client", func(t *testing.T) {
		hexSecret := "746573742d7365637265742d6b6579"
		event := webhook.WebhookEvent{
			EventID:   "01JG8XAMPLE1234567890123456",
			EventType: "token.indexing.queryable",
			Timestamp: time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC),
			Data: webhook.EventData{
				TokenCID:    "eip155:1:erc721:0xABC:1",
				Chain:       "eip155:1",
				Standard:    "erc721",
				Contract:    "0xABC",
				TokenNumber: "1",
			},
		}

		payload, signature, timestamp, err := webhook.GenerateSignedPayload(hexSecret, event)
		require.NoError(t, err)

		// Client-side verification
		signaturePayload := fmt.Sprintf("%d.%s.%s", timestamp, event.EventID, string(payload))
		secretBytes, err := hex.DecodeString(hexSecret)
		require.NoError(t, err)
		h := hmac.New(sha256.New, secretBytes)
		h.Write([]byte(signaturePayload))
		clientSignature := "sha256=" + hex.EncodeToString(h.Sum(nil))

		assert.Equal(t, signature, clientSignature, "Client should be able to reproduce the signature")
	})

	t.Run("invalid hex secret returns error", func(t *testing.T) {
		invalidHexSecret := "not-valid-hex-string" //nolint:gosec,G101
		event := webhook.WebhookEvent{
			EventID:   "01JG8XAMPLE1234567890123456",
			EventType: "token.indexing.queryable",
			Timestamp: time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC),
			Data: webhook.EventData{
				TokenCID:    "eip155:1:erc721:0xABC:1",
				Chain:       "eip155:1",
				Standard:    "erc721",
				Contract:    "0xABC",
				TokenNumber: "1",
			},
		}

		_, _, _, err := webhook.GenerateSignedPayload(invalidHexSecret, event)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to decode hex secret")
	})
}
