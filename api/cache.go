package api

import (
	"strings"
	"time"

	"onql/cache"
)

var queryCache *cache.Cache

// ConfigureQueryCache initializes the shared query cache.
func ConfigureQueryCache(maxBytes int64, ttl time.Duration) {
	if maxBytes <= 0 {
		queryCache = cache.New(0, ttl)
		return
	}
	queryCache = cache.New(maxBytes, ttl)
}

func cacheEnabled() bool {
	return queryCache != nil && queryCache.Enabled()
}

func cacheKey(req *DSLRequest) string {
	return strings.Join([]string{
		req.Protopass,
		req.Query,
		req.CtxKey,
		strings.Join(req.CtxValues, ","),
	}, "|")
}

func getCachedResponse(req *DSLRequest) (string, bool) {
	if !cacheEnabled() {
		return "", false
	}
	return queryCache.Get(cacheKey(req))
}

func storeCachedResponse(req *DSLRequest, payload string) {
	if !cacheEnabled() {
		return
	}
	queryCache.Set(cacheKey(req), payload)
}

func invalidateQueryCache() {
	if cacheEnabled() {
		queryCache.Clear()
	}
}
