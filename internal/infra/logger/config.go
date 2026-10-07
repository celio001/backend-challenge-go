package logger

import (
	"time"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

// Timestamps as ISO 8601 UTC with millesecond precision
func ecsTimeEncoder(t time.Time, enc zapcore.PrimitiveArrayEncoder) {
	enc.AppendString(t.UTC().Format("2006-01-02T15:04:05.000Z07:00"))
}

func configLogger(logEnv string) zap.Config {
	level := zap.NewAtomicLevelAt(zap.InfoLevel)
	if logEnv != "production" && logEnv != "staging" {
		level = zap.NewAtomicLevelAt(zap.DebugLevel)
	}

	encoderConfig := zapcore.EncoderConfig{
		TimeKey:        "@timestamp",
		LevelKey:       "log.level",
		NameKey:        "log.logger",
		MessageKey:     "message",
		CallerKey:      zapcore.OmitKey,
		StacktraceKey:  zapcore.OmitKey,
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.CapitalLevelEncoder,
		EncodeTime:     ecsTimeEncoder,
		EncodeDuration: zapcore.MillisDurationEncoder,
	}

	return zap.Config{
		Level:            level,
		Development:      false,
		Encoding:         "json",
		EncoderConfig:    encoderConfig,
		OutputPaths:      []string{"stdout"},
		ErrorOutputPaths: []string{"stderr"},
	}
}
