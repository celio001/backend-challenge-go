package logger

import (
	"fmt"
	"os"
	"strings"

	"go.uber.org/zap"
)

type Logger struct {
	z *zap.Logger
}

type Entry struct {
	z *zap.Logger
}

var std *Logger

func Init(serviceName, serviceVersion, logEnv string) {
	cfg := configLogger(logEnv)

	l, err := cfg.Build()
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to initialize logger: %v\n", err)
		os.Exit(1)
	}

	l = l.With(
		zap.String("service.name", serviceName),
		zap.String("service.version", serviceVersion),
		zap.Any("labels", map[string]string{
			"environment": logEnv,
		}),
	)

	std = &Logger{z: l}
}

func SanitizeForLog(s string) string {
	return strings.Map(func(r rune) rune {
		if r == '\n' || r == '\r' {
			return ' '
		}
		return r
	}, s)
}

func Sync() {
	if std != nil {
		_ = std.z.Sync()
	}
}

// Package-level logging functions — accept raw zap.Field for flexibility.
func Debug(msg string, fields ...zap.Field) { std.z.Debug(msg, fields...) }
func Info(msg string, fields ...zap.Field)  { std.z.Info(msg, fields...) }
func Warn(msg string, fields ...zap.Field)  { std.z.Warn(msg, fields...) }
func Error(msg string, fields ...zap.Field) { std.z.Error(msg, fields...) }
func Fatal(msg string, fields ...zap.Field) { std.z.Fatal(msg, fields...) }

func With(fields ...zap.Field) *Logger {
	return &Logger{z: std.z.With(fields...)}
}

func (l *Logger) Debug(msg string, f ...zap.Field) { l.z.Debug(msg, f...) }
func (l *Logger) Info(msg string, f ...zap.Field)  { l.z.Info(msg, f...) }
func (l *Logger) Warn(msg string, f ...zap.Field)  { l.z.Warn(msg, f...) }
func (l *Logger) Error(msg string, f ...zap.Field) { l.z.Error(msg, f...) }
func (l *Logger) Fatal(msg string, f ...zap.Field) { l.z.Fatal(msg, f...) }

// WithRequestID starts an Entry with the ECS http.request.id field.
func WithRequestID(id string) *Entry {
	return &Entry{z: std.z.With(zap.String("http.request.id", id))}
}

// WithHTTPRequest starts an Entry with ECS HTTP request fields.
func WithHTTPRequest(method, path, clientIP string) *Entry {
	return &Entry{z: std.z.With(
		zap.String("http.request.method", method),
		zap.String("url.path", path),
		zap.String("client.ip", clientIP),
	)}
}

// WithError starts an Entry with ECS error fields.
func WithError(err error, errType, errCode string) *Entry {
	fields := []zap.Field{
		zap.String("error.type", errType),
		zap.String("error.message", err.Error()),
	}
	if errCode != "" {
		fields = append(fields, zap.String("error.code", errCode))
	}
	return &Entry{z: std.z.With(fields...)}
}

// WithEvent starts an Entry with ECS event fields.
func WithEvent(action, outcome string) *Entry {
	fields := []zap.Field{zap.String("event.action", action)}
	if outcome != "" {
		fields = append(fields, zap.String("event.outcome", outcome))
	}
	return &Entry{z: std.z.With(fields...)}
}

// --- Entry methods for fluent chaining ---

func (e *Entry) WithRequestID(id string) *Entry {
	return &Entry{z: e.z.With(zap.String("http.request.id", id))}
}

func (e *Entry) WithHTTPRequest(method, path, clientIP string) *Entry {
	return &Entry{z: e.z.With(
		zap.String("http.request.method", method),
		zap.String("url.path", path),
		zap.String("client.ip", clientIP),
	)}
}

func (e *Entry) WithError(err error, errType, errCode string) *Entry {
	fields := []zap.Field{
		zap.String("error.type", errType),
		zap.String("error.message", err.Error()),
	}
	if errCode != "" {
		fields = append(fields, zap.String("error.code", errCode))
	}
	return &Entry{z: e.z.With(fields...)}
}

func (e *Entry) WithEvent(action, outcome string) *Entry {
	fields := []zap.Field{zap.String("event.action", action)}
	if outcome != "" {
		fields = append(fields, zap.String("event.outcome", outcome))
	}
	return &Entry{z: e.z.With(fields...)}
}

// With adds an arbitrary key-value pair using ECS dot-notation key conventions.
func (e *Entry) With(key string, value interface{}) *Entry {
	return &Entry{z: e.z.With(zap.Any(key, value))}
}

func (e *Entry) Debug(msg string) { e.z.Debug(msg) }
func (e *Entry) Info(msg string)  { e.z.Info(msg) }
func (e *Entry) Warn(msg string)  { e.z.Warn(msg) }
func (e *Entry) Error(msg string) { e.z.Error(msg) }
func (e *Entry) Fatal(msg string) { e.z.Fatal(msg) }
