package logger

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

func TestConsoleConfigWritesApplicationLogsOnlyToStdout(t *testing.T) {
	t.Parallel()

	for _, debug := range []bool{false, true} {
		cfg := consoleConfig(debug)
		assert.Equal(t, []string{"stdout"}, cfg.OutputPaths)
		assert.Equal(t, []string{"stderr"}, cfg.ErrorOutputPaths)
	}
}

func TestInitializeReplacesZapGlobal(t *testing.T) {
	previous := zap.L()
	t.Cleanup(func() { zap.ReplaceGlobals(previous) })

	require.NoError(t, Initialize(Config{}))
	assert.Same(t, Default(), zap.L())
}

func TestCloudflareCorePostsSchemaNDJSONAndStructuredMessage(t *testing.T) {
	t.Parallel()

	var (
		mu      sync.Mutex
		request capturedRequest
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		request = capturedRequest{
			authorization: r.Header.Get("Authorization"),
			contentType:   r.Header.Get("Content-Type"),
			body:          body,
		}
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	sender := newCloudflareSender(cloudflareSenderConfig{
		streamURL:   server.URL,
		apiToken:    "test-token",
		service:     "ff-indexer",
		environment: "test",
		httpClient:  server.Client(),
	})
	core := newCloudflareCore(zapcore.DebugLevel, sender)
	remoteLogger := zap.New(core)

	remoteLogger.Info("indexed token",
		zap.String("component", ComponentWorkerCore),
		zap.Int64("job_id", 42),
		zap.String("token_cid", "eip155:1:erc721:0xabc:1"),
		zap.String("internal_value", "preserved-verbatim"),
	)
	require.NoError(t, sender.flush(context.Background()))

	mu.Lock()
	got := request
	mu.Unlock()
	assert.Equal(t, "Bearer test-token", got.authorization)
	assert.Equal(t, "application/x-ndjson", got.contentType)

	records := decodeNDJSON(t, got.body)
	require.Len(t, records, 1)
	record := records[0]
	keys := make([]string, 0, len(record))
	for key := range record {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	assert.Equal(t, []string{"environment", "level", "message", "service", "timestamp"}, keys)
	assert.Equal(t, "info", record["level"])
	assert.Equal(t, "ff-indexer", record["service"])
	assert.Equal(t, "test", record["environment"])
	_, err := time.Parse(time.RFC3339Nano, record["timestamp"].(string))
	require.NoError(t, err)

	var message map[string]any
	require.NoError(t, json.Unmarshal([]byte(record["message"].(string)), &message))
	assert.Equal(t, "indexed token", message["message"])
	assert.Equal(t, ComponentWorkerCore, message["component"])
	assert.EqualValues(t, 42, message["job_id"])
	assert.Equal(t, "eip155:1:erc721:0xabc:1", message["token_cid"])
	assert.Equal(t, "preserved-verbatim", message["internal_value"])
}

func TestCloudflareSenderBatchesRecordsUntilFlush(t *testing.T) {
	t.Parallel()

	requests := make(chan []byte, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		requests <- body
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	sender := newCloudflareSender(cloudflareSenderConfig{
		streamURL:   server.URL,
		apiToken:    "test-token",
		service:     "ff-indexer",
		environment: "test",
		httpClient:  server.Client(),
	})
	core := newCloudflareCore(zapcore.InfoLevel, sender)
	remoteLogger := zap.New(core)
	remoteLogger.Info("first")
	remoteLogger.Warn("second")

	require.NoError(t, sender.flush(context.Background()))
	select {
	case body := <-requests:
		records := decodeNDJSON(t, body)
		require.Len(t, records, 2)
		assert.Equal(t, "info", records[0]["level"])
		assert.Equal(t, "warn", records[1]["level"])
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for Cloudflare batch")
	}
}

func TestCloudflareSenderRejectsRecordsBeyondQueuedByteBudget(t *testing.T) {
	t.Parallel()

	sender := &cloudflareSender{
		queue:          make(chan cloudflareQueueItem, 2),
		maxQueuedBytes: 10,
	}
	require.NoError(t, sender.enqueue([]byte("123456"))) // Seven bytes with NDJSON newline.

	err := sender.enqueue([]byte("7890"))
	require.EqualError(t, err, "delivery queue byte limit exceeded: queued=7 record=5 maximum=10")
	assert.Equal(t, int64(7), sender.queuedBytes.Load())
}

type capturedRequest struct {
	authorization string
	contentType   string
	body          []byte
}

func decodeNDJSON(t *testing.T, body []byte) []map[string]any {
	t.Helper()

	var records []map[string]any
	scanner := bufio.NewScanner(strings.NewReader(string(body)))
	for scanner.Scan() {
		var record map[string]any
		require.NoError(t, json.Unmarshal(scanner.Bytes(), &record))
		records = append(records, record)
	}
	require.NoError(t, scanner.Err())
	return records
}
