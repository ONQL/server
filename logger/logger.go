package logger

import (
	"io"
	"log/slog"
	"os"
	"path/filepath"
)

var Log *slog.Logger

// Init initializes the logger with the specified level and optional file output
func Init(level string) {
	var logLevel slog.Level
	switch level {
	case "DEBUG":
		logLevel = slog.LevelDebug
	case "WARN":
		logLevel = slog.LevelWarn
	case "ERROR":
		logLevel = slog.LevelError
	default:
		logLevel = slog.LevelInfo
	}

	// Create multi-writer (console + file)
	writers := []io.Writer{os.Stdout}
	
	// Add file logging if LOG_FILE env var is set
	if logFile := os.Getenv("LOG_FILE"); logFile != "" {
		// Create logs directory if it doesn't exist
		logDir := filepath.Dir(logFile)
		if logDir != "." && logDir != "" {
			os.MkdirAll(logDir, 0755)
		}
		
		file, err := os.OpenFile(logFile, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
		if err == nil {
			writers = append(writers, file)
		}
	}

	opts := &slog.HandlerOptions{
		Level: logLevel,
		AddSource: logLevel == slog.LevelDebug, // Add source file/line in debug mode
	}
	
	handler := slog.NewTextHandler(io.MultiWriter(writers...), opts)
	Log = slog.New(handler)
}

// Helper functions for easier logging

func Debug(msg string, args ...any) {
	if Log != nil {
		Log.Debug(msg, args...)
	}
}

func Info(msg string, args ...any) {
	if Log != nil {
		Log.Info(msg, args...)
	}
}

func Warn(msg string, args ...any) {
	if Log != nil {
		Log.Warn(msg, args...)
	}
}

func Error(msg string, args ...any) {
	if Log != nil {
		Log.Error(msg, args...)
	}
}

// With creates a logger with additional context fields
func With(args ...any) *slog.Logger {
	if Log != nil {
		return Log.With(args...)
	}
	return slog.Default()
}
