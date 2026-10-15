package graphql

// Tests for ErrorPresenter: client mistakes caught before execution must reach
// the client as validation errors with their message, not as internal_error
// (issue #82), while resolver failures stay masked.

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/99designs/gqlgen/graphql/errcode"
	"github.com/99designs/gqlgen/graphql/handler"
	"github.com/99designs/gqlgen/graphql/handler/transport"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/vektah/gqlparser/v2/gqlerror"
	"go.uber.org/mock/gomock"

	apierrors "github.com/feral-file/ff-indexer-v2/internal/api/shared/errors"
	"github.com/feral-file/ff-indexer-v2/internal/mocks"
)

type presentedResponse struct {
	Errors []struct {
		Message    string         `json:"message"`
		Extensions map[string]any `json:"extensions"`
	} `json:"errors"`
}

// postQuery runs query through a server wired with the production presenter
// and returns the HTTP status and decoded errors.
func postQuery(t *testing.T, query string) (int, presentedResponse) {
	t.Helper()
	ctrl := gomock.NewController(t)
	srv := handler.New(NewExecutableSchema(Config{Resolvers: NewResolver(false, mocks.NewMockAPIExecutor(ctrl))}))
	srv.AddTransport(transport.POST{})
	srv.SetErrorPresenter(ErrorPresenter)
	srv.SetRecoverFunc(RecoverFunc)

	body, err := json.Marshal(map[string]string{"query": query})
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodPost, "/graphql", strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	srv.ServeHTTP(rec, req)

	var resp presentedResponse
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	return rec.Code, resp
}

func TestErrorPresenterSurfacesSchemaValidationErrors(t *testing.T) {
	t.Parallel()

	// The query reported on #82: TokenList fields live under items { }.
	status, resp := postQuery(t, `{ tokens { token_cid } }`)

	assert.Equal(t, http.StatusUnprocessableEntity, status, "presenting must not downgrade the 422")
	require.NotEmpty(t, resp.Errors)
	first := resp.Errors[0]
	assert.Contains(t, first.Message, `"token_cid"`)
	assert.Equal(t, string(apierrors.ErrCodeValidationFailed), first.Extensions["code"])
	assert.Equal(t, first.Message, first.Extensions["message"])
}

func TestErrorPresenterSurfacesParseErrors(t *testing.T) {
	t.Parallel()

	status, resp := postQuery(t, `{ tokens {`)

	assert.Equal(t, http.StatusUnprocessableEntity, status)
	require.Len(t, resp.Errors, 1)
	assert.NotEqual(t, "Internal server error", resp.Errors[0].Message)
	assert.Equal(t, string(apierrors.ErrCodeValidationFailed), resp.Errors[0].Extensions["code"])
}

func TestErrorPresenterKeepsOtherProtocolCodes(t *testing.T) {
	t.Parallel()

	// APQ clients re-send the full query only when they see this exact code.
	err := gqlerror.Errorf("PersistedQueryNotFound")
	errcode.Set(err, "PERSISTED_QUERY_NOT_FOUND")

	got := ErrorPresenter(context.Background(), err)

	assert.Equal(t, "PersistedQueryNotFound", got.Message)
	assert.Equal(t, "PERSISTED_QUERY_NOT_FOUND", got.Extensions["code"])
}

func TestErrorPresenterDoesNotMutateTheOriginal(t *testing.T) {
	t.Parallel()

	// gqlgen picks the HTTP status and websocket message type from the
	// original list after presentation.
	err := gqlerror.Errorf(`Cannot query field "token_cid" on type "TokenList".`)
	errcode.Set(err, errcode.ValidationFailed)

	got := ErrorPresenter(context.Background(), err)

	assert.Equal(t, string(apierrors.ErrCodeValidationFailed), got.Extensions["code"])
	assert.Equal(t, errcode.ValidationFailed, err.Extensions["code"])
	assert.Equal(t, errcode.KindProtocol, errcode.GetErrorKind(gqlerror.List{err}))
}

func TestErrorPresenterStillMasksResolverFailures(t *testing.T) {
	t.Parallel()

	got := ErrorPresenter(context.Background(), gqlerror.WrapPath(nil, errors.New("pq: connection refused")))

	assert.Equal(t, "Internal server error", got.Message)
	assert.Equal(t, string(apierrors.ErrCodeInternalError), got.Extensions["code"])
}

func TestJSONScalarRejectsUnsupportedInputAsValidationError(t *testing.T) {
	t.Parallel()

	var j JSON
	err := j.UnmarshalGQL(42)

	var apiErr *apierrors.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, apierrors.ErrCodeValidationFailed, apiErr.Code)
}
