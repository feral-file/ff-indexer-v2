package logger

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

func TestConsoleConfigWritesApplicationLogsOnlyToStdout(t *testing.T) {
	t.Parallel()

	for _, debug := range []bool{false, true} {
		cfg := consoleConfig(debug)
		assert.Equal(t, []string{"stdout"}, cfg.OutputPaths)
		assert.Equal(t, []string{"stderr"}, cfg.ErrorOutputPaths)
		assert.Nil(t, cfg.Sampling)
	}
}

func TestProductionConsoleDoesNotSampleRepeatedLogs(t *testing.T) {
	t.Parallel()

	outputPath := filepath.Join(t.TempDir(), "stdout.log")
	cfg := consoleConfig(false)
	cfg.OutputPaths = []string{outputPath}
	logger, err := cfg.Build()
	require.NoError(t, err)

	const lineCount = 200
	for range lineCount {
		logger.Info("repeated log")
	}
	require.NoError(t, logger.Sync())

	output, err := os.ReadFile(outputPath) //nolint:gosec // outputPath is inside t.TempDir.
	require.NoError(t, err)
	assert.Equal(t, lineCount, strings.Count(string(output), "\n"))
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

func TestCloudflareSenderRejectsOversizedRecordAndFullQueue(t *testing.T) {
	t.Parallel()

	sender := &cloudflareSender{
		queue:          make(chan cloudflareQueueItem, 1),
		maxQueuedBytes: cloudflareMaxQueuedBytes,
	}
	err := sender.enqueue(make([]byte, cloudflareMaxBatchBytes))
	require.ErrorContains(t, err, "maximum")

	require.NoError(t, sender.enqueue([]byte("first")))
	err = sender.enqueue([]byte("second"))
	require.EqualError(t, err, "delivery queue is full")
	assert.Equal(t, int64(len("first")+1), sender.queuedBytes.Load())
}

func TestCloudflareSenderFlushHonorsContext(t *testing.T) {
	t.Parallel()

	for _, queueCapacity := range []int{0, 1} {
		sender := &cloudflareSender{queue: make(chan cloudflareQueueItem, queueCapacity)}
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		err := sender.flush(ctx)
		cancel()
		require.ErrorIs(t, err, context.DeadlineExceeded)
	}
}

func TestCloudflareSenderPostResponses(t *testing.T) {
	t.Parallel()

	t.Run("empty batch", func(t *testing.T) {
		sender := &cloudflareSender{}
		require.NoError(t, sender.post(nil))
	})

	t.Run("request construction", func(t *testing.T) {
		sender := &cloudflareSender{streamURL: "://", httpClient: http.DefaultClient}
		require.ErrorContains(t, sender.post([]byte("record\n")), "build request")
	})

	t.Run("transport failure", func(t *testing.T) {
		sender := &cloudflareSender{
			streamURL: "https://stream.example",
			httpClient: &http.Client{Transport: roundTripperFunc(func(*http.Request) (*http.Response, error) {
				return nil, errors.New("network unavailable")
			})},
		}
		require.ErrorContains(t, sender.post([]byte("record\n")), "network unavailable")
	})

	t.Run("non-success status", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "temporarily unavailable", http.StatusServiceUnavailable)
		}))
		defer server.Close()
		sender := &cloudflareSender{streamURL: server.URL, httpClient: server.Client()}
		require.EqualError(t, sender.post([]byte("record\n")), "post batch: HTTP 503: temporarily unavailable")
	})
}

func TestCloudflareCoreWithFieldsAndLevelFiltering(t *testing.T) {
	t.Parallel()

	sender := &cloudflareSender{
		service:        "ff-indexer",
		environment:    "test",
		queue:          make(chan cloudflareQueueItem, 1),
		maxQueuedBytes: cloudflareMaxQueuedBytes,
	}
	core := newCloudflareCore(zapcore.InfoLevel, sender)
	assert.False(t, core.Enabled(zapcore.DebugLevel))
	assert.Nil(t, core.Check(zapcore.Entry{Level: zapcore.DebugLevel}, nil))
	require.NoError(t, core.Sync())

	logger := zap.New(core).With(zap.String("component", ComponentWorkerCore))
	logger.Info("processed", zap.Int64("job_id", 42))
	item := <-sender.queue

	records := decodeNDJSON(t, append(item.record, '\n'))
	require.Len(t, records, 1)
	var message map[string]any
	require.NoError(t, json.Unmarshal([]byte(records[0]["message"].(string)), &message))
	assert.Equal(t, ComponentWorkerCore, message["component"])
	assert.EqualValues(t, 42, message["job_id"])
}

func TestContextFieldsAndLoggingHelpers(t *testing.T) {
	previousLog, previousSender := log, remoteSender
	previousGlobal := zap.L()
	t.Cleanup(func() {
		log = previousLog
		remoteSender = previousSender
		zap.ReplaceGlobals(previousGlobal)
	})

	core, observed := observer.New(zapcore.DebugLevel)
	log = zap.New(core)
	remoteSender = nil

	ctx := WithFields(context.Background(), zap.String("request_id", "request-1"))
	ctx = ContextWithJob(
		WithComponent(ctx, ComponentWorkerCore),
		42,
		"metadata",
		"core",
	)
	ctx = WithFields(ctx, zap.String("token_cid", "token-1"))

	Info("info")
	InfoCtx(ctx, "info context")
	Error(errors.New("failure"))
	Error(nil)
	ErrorCtx(ctx, errors.New("context failure"))
	ErrorCtx(ctx, nil)
	Warn("warn")
	WarnCtx(ctx, "warn context")
	Debug("debug")
	DebugCtx(ctx, "debug context")
	FromContext(context.Background()).Info("empty context")

	assert.Same(t, log, Default())
	assert.Len(t, observed.All(), 11)
	contextEntry := observed.FilterMessage("info context").AllUntimed()[0]
	assert.Equal(t, map[string]any{
		"component":  ComponentWorkerCore,
		"job_id":     int64(42),
		"job_kind":   "metadata",
		"job_queue":  "core",
		"request_id": "request-1",
		"token_cid":  "token-1",
	}, contextEntry.ContextMap())
	Flush(time.Millisecond)
}

func TestInitializeStreamsAndFlushesRemoteLog(t *testing.T) {
	previousLog, previousSender := log, remoteSender
	previousGlobal := zap.L()
	t.Cleanup(func() {
		log = previousLog
		remoteSender = previousSender
		zap.ReplaceGlobals(previousGlobal)
	})

	requests := make(chan []byte, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		requests <- body
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	require.NoError(t, Initialize(Config{
		CloudflareStreamURL: server.URL,
		CloudflareAPIToken:  "token",
		Environment:         "test",
	}))
	Info("remote log", zap.String("field", "value"))
	Flush(time.Second)

	select {
	case body := <-requests:
		records := decodeNDJSON(t, body)
		require.Len(t, records, 1)
		assert.Equal(t, "test", records[0]["environment"])
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for remote log")
	}
}

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (f roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
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
