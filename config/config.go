package config

import (
	"os"
	"strconv"
	"time"
)

type Config struct {
	DBPath        string
	FlushInterval time.Duration
	LogLevel      string
	Port          string
	CacheMaxBytes int64
	CacheTTL      time.Duration
}

func Load() *Config {
	cacheMaxMB := getIntEnv("CACHE_MAX_MB", 0)
	return &Config{
		DBPath:        getEnv("DB_PATH", "./store"),
		FlushInterval: getDurationEnv("FLUSH_INTERVAL", 500*time.Millisecond),
		LogLevel:      getEnv("LOG_LEVEL", "INFO"),
		Port:          getEnv("PORT", "5656"),
		CacheMaxBytes: int64(cacheMaxMB) * 1024 * 1024,
		CacheTTL:      getDurationEnv("CACHE_TTL", 60*time.Second),
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

func getIntEnv(key string, fallback int) int {
	if value, ok := os.LookupEnv(key); ok {
		if n, err := strconv.Atoi(value); err == nil {
			return n
		}
	}
	return fallback
}
