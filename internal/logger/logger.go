package logger

import (
	"context"
	"time"

	"github.com/TheZeroSlave/zapsentry"
	"github.com/getsentry/sentry-go"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var (
	// log is the global zap logger instance
	log *zap.Logger
	// sentryClient is the global sentry client
	sentryClient *sentry.Client
)

// Config holds logger configuration
type Config struct {
	Debug           bool
	SentryDSN       string
	SentryClient    *sentry.Client
	BreadcrumbLevel zapcore.Level
	Tags            map[string]string
}

// Initialize initializes the logger with sentry integration
func Initialize(cfg Config) error {
	// Create zap config based on debug flag
	var zapConfig zap.Config
	if cfg.Debug {
		zapConfig = zap.NewDevelopmentConfig()
	} else {
		zapConfig = zap.NewProductionConfig()
	}
	zapConfig.Level = zap.NewAtomicLevelAt(zapcore.InfoLevel)
	if cfg.Debug {
		zapConfig.Level = zap.NewAtomicLevelAt(zapcore.DebugLevel)
	}

	// Build base logger
	baseLogger, err := zapConfig.Build()
	if err != nil {
		return err
	}

	// Setup sentry client if DSN is provided
	if cfg.SentryDSN != "" {
		if cfg.SentryClient == nil {
			sentryClient, err = sentry.NewClient(sentry.ClientOptions{
				Dsn:   cfg.SentryDSN,
				Debug: cfg.Debug,
			})
			if err != nil {
				return err
			}
		} else {
			sentryClient = cfg.SentryClient
		}

		// Configure zapsentry
		breadcrumbLevel := cfg.BreadcrumbLevel
		if breadcrumbLevel == zapcore.InvalidLevel {
			breadcrumbLevel = zapcore.InfoLevel // Default to Info level for breadcrumbs
		}

		zapsentryCfg := zapsentry.Configuration{
			Level:             zapcore.ErrorLevel, // Send errors to sentry
			EnableBreadcrumbs: true,
			BreadcrumbLevel:   breadcrumbLevel,
			Tags:              cfg.Tags,
		}

		// Create zapsentry core
		core, err := zapsentry.NewCore(zapsentryCfg, zapsentry.NewSentryClientFromClient(sentryClient))
		if err != nil {
			return err
		}

		// Attach sentry core to logger
		log = zapsentry.AttachCoreToLogger(core, baseLogger)
	} else {
		log = baseLogger
	}

	return nil
}

// Flush flushes any buffered sentry events
func Flush(timeout time.Duration) {
	if sentryClient != nil {
		sentryClient.Flush(timeout)
	}
}

// FromContext returns a logger with sentry scope from context
// This should be used when you have a context with sentry hub
func FromContext(ctx context.Context) *zap.Logger {
	if ctx == nil {
		return log
	}

	// Use zapsentry.Context to attach context to logger
	// This enables sentry scope tracking via context
	return log.With(zapsentry.Context(ctx))
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
