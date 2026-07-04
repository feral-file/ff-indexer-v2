package rest

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/feral-file/ff-indexer-v2/internal/api/shared/dto"
	"github.com/feral-file/ff-indexer-v2/internal/mocks"
	"github.com/feral-file/ff-indexer-v2/internal/store/schema"
)

func TestHandlerListReleasesSuccess(t *testing.T) {
	gin.SetMode(gin.TestMode)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockExec := mocks.NewMockAPIExecutor(ctrl)
	h := NewHandler(false, mockExec)

	vendor := schema.VendorArtBlocks
	mockExec.EXPECT().
		ListReleases(gomock.Any(), gomock.Nil(), &vendor, gomock.Nil(), gomock.Nil(), gomock.Any(), gomock.Any()).
		Return(&dto.ReleaseListResponse{
			Items: []dto.ReleaseResponse{{
				ID:              9,
				Vendor:          "artblocks",
				VendorReleaseID: "1-0xabc-1",
			}},
		}, nil)

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/api/v1/releases?vendor=artblocks", nil)

	h.ListReleases(c)

	require.Equal(t, http.StatusOK, w.Code)

	var response dto.ReleaseListResponse
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &response))
	require.Len(t, response.Items, 1)
	assert.Equal(t, uint64(9), response.Items[0].ID)
	assert.Nil(t, response.Items[0].Members)
}

func TestHandlerListReleasesValidationError(t *testing.T) {
	gin.SetMode(gin.TestMode)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockExec := mocks.NewMockAPIExecutor(ctrl)
	h := NewHandler(false, mockExec)

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/api/v1/releases", nil)

	h.ListReleases(c)

	require.Equal(t, http.StatusUnprocessableEntity, w.Code)
}

// ─── TriggerReleaseIndexing handler ──────────────────────────────────────────

func TestHandlerTriggerReleaseIndexingSuccess(t *testing.T) {
	gin.SetMode(gin.TestMode)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockExec := mocks.NewMockAPIExecutor(ctrl)
	h := NewHandler(false, mockExec)

	mockExec.EXPECT().
		TriggerReleaseIndexing(gomock.Any(), "artblocks", "1-0xabc-78", "", []int64{1, 50, 100}).
		Return(&dto.TriggerIndexingResponse{JobID: 42}, nil)

	body := `{"vendor":"artblocks","vendor_release_id":"1-0xabc-78","mint_numbers":[1,50,100]}`
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/api/v1/releases/index", bytes.NewBufferString(body))
	c.Request.Header.Set("Content-Type", "application/json")

	h.TriggerReleaseIndexing(c)

	require.Equal(t, http.StatusAccepted, w.Code)
}

func TestHandlerTriggerReleaseIndexingWithSlug(t *testing.T) {
	gin.SetMode(gin.TestMode)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockExec := mocks.NewMockAPIExecutor(ctrl)
	h := NewHandler(false, mockExec)

	mockExec.EXPECT().
		TriggerReleaseIndexing(gomock.Any(), "feralfile", "", "data-pilgrims-01-769", []int64{5, 10, 50}).
		Return(&dto.TriggerIndexingResponse{JobID: 99}, nil)

	body := `{"vendor":"feralfile","vendor_release_slug":"data-pilgrims-01-769","mint_numbers":[5,10,50]}`
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/api/v1/releases/index", bytes.NewBufferString(body))
	c.Request.Header.Set("Content-Type", "application/json")

	h.TriggerReleaseIndexing(c)

	require.Equal(t, http.StatusAccepted, w.Code)
}

func TestHandlerTriggerReleaseIndexingValidationError_MissingVendor(t *testing.T) {
	gin.SetMode(gin.TestMode)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockExec := mocks.NewMockAPIExecutor(ctrl)
	h := NewHandler(false, mockExec)

	body := `{"vendor_release_id":"1-0xabc-78","mint_numbers":[1,2,3]}`
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/api/v1/releases/index", bytes.NewBufferString(body))
	c.Request.Header.Set("Content-Type", "application/json")

	h.TriggerReleaseIndexing(c)

	require.Equal(t, http.StatusUnprocessableEntity, w.Code)
}

func TestHandlerTriggerReleaseIndexingValidationError_InvalidVendor(t *testing.T) {
	gin.SetMode(gin.TestMode)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockExec := mocks.NewMockAPIExecutor(ctrl)
	h := NewHandler(false, mockExec)

	body := `{"vendor":"superrare","vendor_release_id":"abc","mint_numbers":[1,2]}`
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/api/v1/releases/index", bytes.NewBufferString(body))
	c.Request.Header.Set("Content-Type", "application/json")

	h.TriggerReleaseIndexing(c)

	require.Equal(t, http.StatusUnprocessableEntity, w.Code)
}

func TestHandlerTriggerReleaseIndexingValidationError_EmptyMintNumbers(t *testing.T) {
	gin.SetMode(gin.TestMode)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockExec := mocks.NewMockAPIExecutor(ctrl)
	h := NewHandler(false, mockExec)

	body := `{"vendor":"artblocks","vendor_release_id":"1-0xabc-78","mint_numbers":[]}`
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/api/v1/releases/index", bytes.NewBufferString(body))
	c.Request.Header.Set("Content-Type", "application/json")

	h.TriggerReleaseIndexing(c)

	require.Equal(t, http.StatusUnprocessableEntity, w.Code)
}

func TestHandlerTriggerReleaseIndexingValidationError_InvalidJSON(t *testing.T) {
	gin.SetMode(gin.TestMode)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockExec := mocks.NewMockAPIExecutor(ctrl)
	h := NewHandler(false, mockExec)

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/api/v1/releases/index", bytes.NewBufferString("not-json"))
	c.Request.Header.Set("Content-Type", "application/json")

	h.TriggerReleaseIndexing(c)

	require.Equal(t, http.StatusUnprocessableEntity, w.Code)
}
