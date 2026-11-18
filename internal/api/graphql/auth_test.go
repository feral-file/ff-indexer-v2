package graphql

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/feral-file/ff-indexer-v2/internal/api/middleware"
	"github.com/feral-file/ff-indexer-v2/internal/mocks"
)

const (
	authTestAPIKey = "test-api-key"

	errAuthRequired = "Authentication required for this mutation"
	errAPIKeyOnly   = "This mutation requires API key authentication only (JWT not accepted)"

	// Both mutations carry arguments their resolver rejects, so an operation that
	// passes authentication ends in a validation error instead of reaching the executor.
	webhookField = `createWebhookClient(webhook_url:"", event_filters:[]) { client_id }`
	addressField = `triggerAddressIndexing(addresses:[]) { jobs { __typename } }`
)

// newAuthTestServer returns the production handler's server and a JWT it accepts.
func newAuthTestServer(t *testing.T) (http.Handler, string) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	der, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	require.NoError(t, err)
	publicPEM := string(pem.EncodeToMemory(&pem.Block{Type: "PUBLIC KEY", Bytes: der}))
	token, err := jwt.NewWithClaims(jwt.SigningMethodRS256, jwt.RegisteredClaims{
		Subject:   "tester",
		ExpiresAt: jwt.NewNumericDate(time.Now().Add(time.Hour)),
	}).SignedString(key)
	require.NoError(t, err)

	h, err := NewHandler(false, mocks.NewMockAPIExecutor(gomock.NewController(t)), middleware.AuthConfig{
		JWTPublicKey: publicPEM,
		APIKeys:      []string{authTestAPIKey},
	})
	require.NoError(t, err)
	return h.(*gqlHandler).server, token
}

// postAuthQuery posts query with the Authorization header and returns the error messages.
func postAuthQuery(t *testing.T, srv http.Handler, query, authorization string) []string {
	t.Helper()
	body, err := json.Marshal(map[string]string{"query": query})
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodPost, "/graphql", strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/json")
	if authorization != "" {
		req.Header.Set("Authorization", authorization)
	}
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	var resp struct {
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp), rec.Body.String())
	messages := make([]string, 0, len(resp.Errors))
	for _, e := range resp.Errors {
		messages = append(messages, e.Message)
	}
	return messages
}

// TestMutationAuthCannotBeBypassedByOperationShape covers the shapes that skipped
// authentication when only top-level fields were read: inline fragments, named
// fragments, nesting, aliases, and a weaker mutation listed first.
func TestMutationAuthCannotBeBypassedByOperationShape(t *testing.T) {
	srv, token := newAuthTestServer(t)
	bearer, apiKey := "Bearer "+token, "ApiKey "+authTestAPIKey

	shapes := map[string]string{
		"plain":           `mutation { %s }`,
		"alias":           `mutation { a: %s }`,
		"inline fragment": `mutation { ... on Mutation { %s } }`,
		"untyped inline":  `mutation { ... { %s } }`,
		"named fragment":  `mutation { ...F } fragment F on Mutation { %s }`,
		"nested fragment": `mutation { ...F } fragment F on Mutation { ... on Mutation { ...G } } fragment G on Mutation { %s }`,
		"after typename":  `mutation { __typename %s }`,
	}
	for name, shape := range shapes {
		t.Run(name, func(t *testing.T) {
			webhook := strings.Replace(shape, "%s", webhookField, 1)
			assert.Equal(t, []string{errAuthRequired}, postAuthQuery(t, srv, webhook, ""), "webhook, no credential")
			assert.Equal(t, []string{errAPIKeyOnly}, postAuthQuery(t, srv, webhook, bearer), "webhook, JWT")
			assert.Equal(t, []string{"Validation failed"}, postAuthQuery(t, srv, webhook, apiKey), "webhook, API key")

			address := strings.Replace(shape, "%s", addressField, 1)
			assert.Equal(t, []string{errAuthRequired}, postAuthQuery(t, srv, address, ""), "address, no credential")
			assert.Equal(t, []string{"Validation failed"}, postAuthQuery(t, srv, address, bearer), "address, JWT")
			assert.Equal(t, []string{"Validation failed"}, postAuthQuery(t, srv, address, apiKey), "address, API key")
		})
	}

	t.Run("strictest field wins regardless of order", func(t *testing.T) {
		for _, query := range []string{
			`mutation { ` + addressField + ` ` + webhookField + ` }`,
			`mutation { ` + webhookField + ` ` + addressField + ` }`,
			`mutation { triggerTokenIndexing(token_cids:[]) { job_id } ...F } fragment F on Mutation { ` + webhookField + ` }`,
		} {
			assert.Equal(t, []string{errAuthRequired}, postAuthQuery(t, srv, query, ""), query)
			assert.Equal(t, []string{errAPIKeyOnly}, postAuthQuery(t, srv, query, bearer), query)
		}
	})

	t.Run("a skipped field demands nothing", func(t *testing.T) {
		query := `mutation { __typename ` + strings.Replace(webhookField, ")", ") @skip(if: true)", 1) + ` }`
		assert.Empty(t, postAuthQuery(t, srv, query, ""))
	})
}

// TestPublicMutationsStayOpen keeps the open triggers reachable without a credential.
func TestPublicMutationsStayOpen(t *testing.T) {
	srv, _ := newAuthTestServer(t)
	for _, query := range []string{
		`mutation { triggerTokenIndexing(token_cids:[]) { job_id } }`,
		`mutation { ... on Mutation { triggerTokenIndexing(token_cids:[]) { job_id } } }`,
	} {
		assert.Equal(t, []string{"Validation failed"}, postAuthQuery(t, srv, query, ""), query)
	}
}

// TestEveryMutationHasAnAuthPolicy fails when a mutation is added to the schema
// without an explicit entry in mutationAuthPolicy.
func TestEveryMutationHasAnAuthPolicy(t *testing.T) {
	mutation := NewExecutableSchema(Config{}).Schema().Mutation
	require.NotNil(t, mutation)
	var fields []string
	for _, field := range mutation.Fields {
		if !strings.HasPrefix(field.Name, "__") {
			fields = append(fields, field.Name)
		}
	}
	policies := make([]string, 0, len(mutationAuthPolicy))
	for name := range mutationAuthPolicy {
		policies = append(policies, name)
	}
	assert.ElementsMatch(t, fields, policies)
}
