package temporal

import (
	"go.temporal.io/sdk/log"
	"go.uber.org/zap"
)

// ZapLoggerAdapter adapts zap.Logger to Temporal's log.Logger interface
type ZapLoggerAdapter struct {
	logger *zap.Logger
}

// NewZapLoggerAdapter creates a new zap logger adapter for Temporal
func NewZapLoggerAdapter(logger *zap.Logger) log.Logger {
	return &ZapLoggerAdapter{logger: logger}
}

// Debug logs a debug message
func (z *ZapLoggerAdapter) Debug(msg string, keyvals ...interface{}) {
	fields := convertKeyvalsToFields(keyvals...)
	z.logger.Debug(msg, fields...)
}

// Info logs an info message
func (z *ZapLoggerAdapter) Info(msg string, keyvals ...interface{}) {
	fields := convertKeyvalsToFields(keyvals...)
	z.logger.Info(msg, fields...)
}

// Warn logs a warning message
func (z *ZapLoggerAdapter) Warn(msg string, keyvals ...interface{}) {
	fields := convertKeyvalsToFields(keyvals...)
	z.logger.Warn(msg, fields...)
}

// Error logs an error message
func (z *ZapLoggerAdapter) Error(msg string, keyvals ...interface{}) {
	fields := convertKeyvalsToFields(keyvals...)
	z.logger.Error(msg, fields...)
}

// convertKeyvalsToFields converts key-value pairs to zap fields
// Temporal's log interface uses keyvals in format: key1, val1, key2, val2, ...
func convertKeyvalsToFields(keyvals ...interface{}) []zap.Field {
	if len(keyvals)%2 != 0 {
		// Odd number of keyvals, ignore last one
		keyvals = keyvals[:len(keyvals)-1]
	}

	fields := make([]zap.Field, 0, len(keyvals)/2)
	for i := 0; i < len(keyvals); i += 2 {
		key, ok := keyvals[i].(string)
		if !ok {
			continue
		}
		val := keyvals[i+1]
		fields = append(fields, zap.Any(key, val))
	}
	return fields
}
