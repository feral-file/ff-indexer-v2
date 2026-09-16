package logger

import (
	"context"
	"fmt"
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	// log is the global zap logger instance
	log *zap.Logger
	// remoteSender owns the optional Cloudflare Stream delivery queue.
	remoteSender *cloudflareSender
)

// componentKey is the context key type for storing component names
type componentKey struct{}

// Component tags identify the ff-indexer process mode in structured logs (field "component").
// Keep these string values stable: operators and log filters depend on them.
const (
	ComponentHTTPServer        = "http-server"
	ComponentEthereumIngestion = "ethereum-ingestion"
	ComponentTezosIngestion    = "tezos-ingestion"
	ComponentWorkerCore        = "worker-core"
	ComponentWorkerMedia       = "worker-media"
	ComponentSweeper           = "sweeper"
)

// WithComponent returns a new context with the component name attached
func WithComponent(ctx context.Context, component string) context.Context {
	return context.WithValue(ctx, componentKey{}, component)
}

// Config holds logger configuration
type Config struct {
	Debug               bool
	CloudflareStreamURL string
	CloudflareAPIToken  string
	Environment         string
}

// consoleConfig returns Zap's normal encoder configuration with application logs
// explicitly routed to stdout. Logger-internal failures remain on stderr.
func consoleConfig(debug bool) zap.Config {
	var cfg zap.Config
	if debug {
		cfg = zap.NewDevelopmentConfig()
	} else {
		cfg = zap.NewProductionConfig()
	}
	cfg.OutputPaths = []string{"stdout"}
	cfg.ErrorOutputPaths = []string{"stderr"}
	// Preserve every local log line. Zap's production default samples repeated
	// messages, which would make stdout less complete than the Cloudflare copy.
	cfg.Sampling = nil
	cfg.Level = zap.NewAtomicLevelAt(zapcore.InfoLevel)
	cfg.EncoderConfig.EncodeTime = zapcore.RFC3339TimeEncoder
	if debug {
		cfg.Level = zap.NewAtomicLevelAt(zapcore.DebugLevel)
	}
	return cfg
}

// Initialize configures stdout logging and the optional Cloudflare Stream tee.
func Initialize(cfg Config) error {
	zapConfig := consoleConfig(cfg.Debug)
	baseLogger, err := zapConfig.Build()
	if err != nil {
		return err
	}
	log = baseLogger
	remoteSender = nil
	if cfg.CloudflareAPIToken != "" {
		remoteSender = newCloudflareSender(cloudflareSenderConfig{
			streamURL:   cfg.CloudflareStreamURL,
			apiToken:    cfg.CloudflareAPIToken,
			service:     "ff-indexer",
			environment: cfg.Environment,
		})
		remoteCore := newCloudflareCore(zapConfig.Level, remoteSender)
		log = baseLogger.WithOptions(
			zap.WrapCore(func(core zapcore.Core) zapcore.Core {
				return zapcore.NewTee(core, remoteCore)
			}),
			zap.WithFatalHook(cloudflareFatalHook{sender: remoteSender}),
		)
	}
	// Keep package-global Zap calls from bypassing stdout and the remote tee.
	zap.ReplaceGlobals(log)
	return nil
}

// Flush waits for Cloudflare records already accepted into memory to be sent.
func Flush(timeout time.Duration) {
	if remoteSender == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if err := remoteSender.flush(ctx); err != nil {
		remoteSender.report(fmt.Errorf("flush: %w", err))
	}
}

// FromContext returns a logger enriched with fields attached to ctx.
func FromContext(ctx context.Context) *zap.Logger {
	base := log
	if base == nil {
		// Tests and code paths before Initialize must not nil-deref.
		base = zap.NewNop()
	}
	if ctx == nil {
		return base
	}

	logger := base
	component, _ := ctx.Value(componentKey{}).(string)
	if component != "" {
		logger = logger.With(zap.String("component", component))
	}
	if fields, ok := ctx.Value(fieldsKey{}).([]zap.Field); ok {
		logger = logger.With(fields...)
	}

	return logger
}

// Default returns the global logger (without context scope)
func Default() *zap.Logger {
	return log
}

// Info logs an info message
func Info(msg string, fields ...zap.Field) {
	log.Info(msg, fields...)
}

// InfoCtx logs an info message with context
func InfoCtx(ctx context.Context, msg string, fields ...zap.Field) {
	FromContext(ctx).Info(msg, fields...)
}

// Error logs an error message
func Error(err error, fields ...zap.Field) {
	if err != nil {
		log.Error(err.Error(), fields...)
	} else {
		log.Error("error occurred", fields...)
	}
}

// ErrorCtx logs an error message with context
func ErrorCtx(ctx context.Context, err error, fields ...zap.Field) {
	if err != nil {
		FromContext(ctx).Error(err.Error(), fields...)
	} else {
		FromContext(ctx).Error("error occurred", fields...)
	}
}

// Fatal logs a fatal message and exits
func Fatal(msg string, fields ...zap.Field) {
	log.Fatal(msg, fields...)
}

// FatalCtx logs a fatal message with context and exits
func FatalCtx(ctx context.Context, msg string, fields ...zap.Field) {
	FromContext(ctx).Fatal(msg, fields...)
}

// Warn logs a warning message
func Warn(msg string, fields ...zap.Field) {
	log.Warn(msg, fields...)
}

// WarnCtx logs a warning message with context
func WarnCtx(ctx context.Context, msg string, fields ...zap.Field) {
	FromContext(ctx).Warn(msg, fields...)
}

// Debug logs a debug message
func Debug(msg string, fields ...zap.Field) {
	log.Debug(msg, fields...)
}

// DebugCtx logs a debug message with context
func DebugCtx(ctx context.Context, msg string, fields ...zap.Field) {
	FromContext(ctx).Debug(msg, fields...)
}
