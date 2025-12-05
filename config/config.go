package config

import (
	"os"
	"time"
)

type Config struct {
	DBPath        string
	FlushInterval time.Duration
	LogLevel      string
}

func Load() *Config {
	return &Config{
		DBPath:        getEnv("DB_PATH", "./store"),
		FlushInterval: getDurationEnv("FLUSH_INTERVAL", 500*time.Millisecond),
		LogLevel:      getEnv("LOG_LEVEL", "INFO"),
	}
}

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

func getDurationEnv(key string, fallback time.Duration) time.Duration {
	if value, ok := os.LookupEnv(key); ok {
		if d, err := time.ParseDuration(value); err == nil {
			return d
		}
	}
	return fallback
}
