package logger

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	cloudflareBatchInterval  = time.Second
	cloudflareQueueCapacity  = 4096
	cloudflareMaxQueuedBytes = 64 * 1024 * 1024
	// Cloudflare accepts at most 5 MiB per Stream request. Leave headroom for
	// newline delimiters and any future HTTP-layer framing changes.
	cloudflareMaxBatchBytes = 4 * 1024 * 1024
)

type cloudflareRecord struct {
	Timestamp   string `json:"timestamp"`
	Level       string `json:"level"`
	Service     string `json:"service"`
	Environment string `json:"environment"`
	Message     string `json:"message"`
}

type cloudflareSenderConfig struct {
	streamURL   string
	apiToken    string
	service     string
	environment string
	httpClient  *http.Client
}

type cloudflareQueueItem struct {
	record []byte
	flush  chan error
}

// cloudflareSender owns the memory-only delivery queue and HTTP batching loop.
//
// Reason: remote I/O must not add Cloudflare latency to indexer work. Trade-offs:
// bounded memory means the remote copy can be dropped during a prolonged outage;
// stdout remains complete and enqueue/send failures are written to stderr.
// Constraints: each POST stays below the Stream's 5 MiB request limit and uses
// the trusted-service NDJSON contract from ff-logging.
type cloudflareSender struct {
	streamURL      string
	apiToken       string
	service        string
	environment    string
	httpClient     *http.Client
	queue          chan cloudflareQueueItem
	queuedBytes    atomic.Int64
	maxQueuedBytes int64
}

// newCloudflareSender starts one batching goroutine for a configured logger.
func newCloudflareSender(cfg cloudflareSenderConfig) *cloudflareSender {
	sender := &cloudflareSender{
		streamURL:      cfg.streamURL,
		apiToken:       cfg.apiToken,
		service:        cfg.service,
		environment:    cfg.environment,
		httpClient:     cfg.httpClient,
		queue:          make(chan cloudflareQueueItem, cloudflareQueueCapacity),
		maxQueuedBytes: cloudflareMaxQueuedBytes,
	}
	if sender.httpClient == nil {
		sender.httpClient = &http.Client{
			Timeout: 10 * time.Second,
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
				return http.ErrUseLastResponse
			},
		}
	}
	go sender.run()
	return sender
}

// enqueue accepts one already-schema-conformant record without blocking the caller.
func (s *cloudflareSender) enqueue(record []byte) error {
	recordBytes := int64(len(record) + 1)
	if recordBytes > cloudflareMaxBatchBytes {
		return fmt.Errorf("record is %d bytes; maximum is %d", recordBytes, cloudflareMaxBatchBytes)
	}
	for {
		queuedBytes := s.queuedBytes.Load()
		if queuedBytes+recordBytes > s.maxQueuedBytes {
			return fmt.Errorf("delivery queue byte limit exceeded: queued=%d record=%d maximum=%d",
				queuedBytes, recordBytes, s.maxQueuedBytes)
		}
		if s.queuedBytes.CompareAndSwap(queuedBytes, queuedBytes+recordBytes) {
			break
		}
	}
	item := cloudflareQueueItem{record: record}
	select {
	case s.queue <- item:
		return nil
	default:
		s.queuedBytes.Add(-recordBytes)
		return errors.New("delivery queue is full")
	}
}

// flush waits until the sender has attempted every record queued before this call.
func (s *cloudflareSender) flush(ctx context.Context) error {
	done := make(chan error, 1)
	select {
	case s.queue <- cloudflareQueueItem{flush: done}:
	case <-ctx.Done():
		return ctx.Err()
	}

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// run batches queued NDJSON records and serializes all HTTP delivery.
func (s *cloudflareSender) run() {
	ticker := time.NewTicker(cloudflareBatchInterval)
	defer ticker.Stop()

	batch := make([]byte, 0, 64*1024)
	for {
		select {
		case item := <-s.queue:
			if item.flush != nil {
				err := s.post(batch)
				batch = batch[:0]
				item.flush <- err
				continue
			}
			s.queuedBytes.Add(-int64(len(item.record) + 1))
			if len(batch)+len(item.record)+1 > cloudflareMaxBatchBytes {
				s.report(s.post(batch))
				batch = batch[:0]
			}
			batch = append(batch, item.record...)
			batch = append(batch, '\n')
		case <-ticker.C:
			s.report(s.post(batch))
			batch = batch[:0]
		}
	}
}

// post sends one NDJSON batch. An empty batch needs no HTTP request.
func (s *cloudflareSender) post(batch []byte) error {
	if len(batch) == 0 {
		return nil
	}
	req, err := http.NewRequest(http.MethodPost, s.streamURL, bytes.NewReader(batch))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+s.apiToken)
	req.Header.Set("Content-Type", "application/x-ndjson")

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("post batch: %w", err)
	}
	defer func() {
		if closeErr := resp.Body.Close(); closeErr != nil {
			s.report(fmt.Errorf("close response body: %w", closeErr))
		}
	}()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4*1024))
		return fmt.Errorf("post batch: HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4*1024))
	return nil
}

// report bypasses Zap to avoid recursively streaming delivery failures.
func (s *cloudflareSender) report(err error) {
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "cloudflare log stream: %v\n", err)
	}
}

type cloudflareCore struct {
	level   zapcore.LevelEnabler
	encoder zapcore.Encoder
	sender  *cloudflareSender
}

type cloudflareFatalHook struct {
	sender *cloudflareSender
}

// OnWrite drains the remote queue before preserving Zap's fatal exit behavior.
func (h cloudflareFatalHook) OnWrite(_ *zapcore.CheckedEntry, _ []zapcore.Field) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	err := h.sender.flush(ctx)
	cancel()
	if err != nil {
		h.sender.report(fmt.Errorf("flush fatal log: %w", err))
	}
	os.Exit(1)
}

// newCloudflareCore creates the remote half of the stdout/Cloudflare Zap tee.
func newCloudflareCore(level zapcore.LevelEnabler, sender *cloudflareSender) zapcore.Core {
	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.TimeKey = ""
	encoderConfig.LevelKey = ""
	encoderConfig.NameKey = "logger"
	encoderConfig.MessageKey = "message"
	return &cloudflareCore{
		level:   level,
		encoder: zapcore.NewJSONEncoder(encoderConfig),
		sender:  sender,
	}
}

func (c *cloudflareCore) Enabled(level zapcore.Level) bool {
	return c.level.Enabled(level)
}

func (c *cloudflareCore) With(fields []zapcore.Field) zapcore.Core {
	clone := &cloudflareCore{level: c.level, encoder: c.encoder.Clone(), sender: c.sender}
	for i := range fields {
		fields[i].AddTo(clone.encoder)
	}
	return clone
}

func (c *cloudflareCore) Check(entry zapcore.Entry, checked *zapcore.CheckedEntry) *zapcore.CheckedEntry {
	if c.Enabled(entry.Level) {
		return checked.AddCore(entry, c)
	}
	return checked
}

func (c *cloudflareCore) Write(entry zapcore.Entry, fields []zapcore.Field) error {
	message, err := c.encoder.EncodeEntry(entry, fields)
	if err != nil {
		return fmt.Errorf("encode structured message: %w", err)
	}
	defer message.Free()

	record, err := json.Marshal(cloudflareRecord{
		Timestamp:   entry.Time.UTC().Format(time.RFC3339Nano),
		Level:       entry.Level.String(),
		Service:     c.sender.service,
		Environment: c.sender.environment,
		Message:     strings.TrimSuffix(message.String(), "\n"),
	})
	if err != nil {
		return fmt.Errorf("encode Cloudflare record: %w", err)
	}
	return c.sender.enqueue(record)
}

func (c *cloudflareCore) Sync() error {
	return nil
}
