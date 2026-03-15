package api

import (
	"encoding/json"
	"onql/dsl"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// GetConnectionCount is set by the server package to avoid import cycles.
var GetConnectionCount func() int

// ---------------------------------------------------------------------------
// Query history ring buffer
// ---------------------------------------------------------------------------

const maxHistory = 1000

type QueryRecord struct {
	Query     string `json:"query"`
	Target    string `json:"target"`
	Timestamp int64  `json:"timestamp"`
	DurationMs   int64  `json:"duration_ms"`
	MemAlloc     uint64 `json:"mem_alloc"`      // total bytes allocated during query (includes freed)
	MemHeapInUse uint64 `json:"mem_heap_inuse"` // heap in-use at end of query
	MemHeapPeak  uint64 `json:"mem_heap_peak"`  // highest heap snapshot sampled during query
	Goroutines   int    `json:"goroutines"`     // goroutine count at start
	RequestSize  int    `json:"request_size"`
	ResponseSize int    `json:"response_size"`
	Error        string `json:"error,omitempty"`
}

var (
	history   = make([]QueryRecord, 0, maxHistory)
	historyMu sync.RWMutex
)

// StartQueryTrace snapshots pre-execution state and returns a sampler + finish pair.
// Call sampler.Sample() periodically during execution (e.g. after heavy loads)
// to capture heap peaks. Call finish() once after execution completes.
func StartQueryTrace(target, query string, reqSize int) (*HeapSampler, func(resp string, errMsg string)) {
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	sampler := &HeapSampler{peak: before.HeapInuse}
	start := time.Now()
	goroutines := runtime.NumGoroutine()

	finish := func(resp string, errMsg string) {
		var after runtime.MemStats
		runtime.ReadMemStats(&after)
		// final sample
		sampler.sample(after.HeapInuse)

		rec := QueryRecord{
			Query:        query,
			Target:       target,
			Timestamp:    start.UnixMilli(),
			DurationMs:   time.Since(start).Milliseconds(),
			MemAlloc:     after.TotalAlloc - before.TotalAlloc,
			MemHeapInUse: after.HeapInuse,
			MemHeapPeak:  sampler.peak,
			Goroutines:   goroutines,
			RequestSize:  reqSize,
			ResponseSize: len(resp),
			Error:        errMsg,
		}

		historyMu.Lock()
		if len(history) >= maxHistory {
			copy(history, history[maxHistory/4:])
			history = history[:len(history)-maxHistory/4]
		}
		history = append(history, rec)
		historyMu.Unlock()
	}

	return sampler, finish
}

// HeapSampler tracks the highest observed HeapInuse during a query's lifetime.
type HeapSampler struct{ peak uint64 }

// Sample reads current heap and updates peak if higher.
func (s *HeapSampler) Sample() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	s.sample(m.HeapInuse)
}

func (s *HeapSampler) sample(heapInuse uint64) {
	if heapInuse > s.peak {
		s.peak = heapInuse
	}
}

func getHistory(limit int) []QueryRecord {
	historyMu.RLock()
	defer historyMu.RUnlock()
	n := len(history)
	if n == 0 {
		return []QueryRecord{}
	}
	start := 0
	if limit > 0 && limit < n {
		start = n - limit
	}
	out := make([]QueryRecord, n-start)
	copy(out, history[start:])
	return out
}

func clearHistory() {
	historyMu.Lock()
	history = history[:0]
	historyMu.Unlock()
}

func historySummary() map[string]any {
	historyMu.RLock()
	defer historyMu.RUnlock()
	n := len(history)
	if n == 0 {
		return map[string]any{"total_queries": 0}
	}
	var totalMs, maxMs int64
	var maxMem uint64
	var errors int
	var slowQ, heavyQ string
	targets := map[string]int{}

	for _, r := range history {
		totalMs += r.DurationMs
		targets[r.Target]++
		if r.Error != "" {
			errors++
		}
		if r.DurationMs > maxMs {
			maxMs = r.DurationMs
			slowQ = r.Query
		}
		if r.MemAlloc > maxMem {
			maxMem = r.MemAlloc
			heavyQ = r.Query
		}
	}
	return map[string]any{
		"total_queries":       n,
		"total_errors":        errors,
		"avg_duration_ms":     totalMs / int64(n),
		"max_duration_ms":     maxMs,
		"slowest_query":       slowQ,
		"max_mem_alloc":       maxMem,
		"heaviest_mem_query":  heavyQ,
		"by_target":           targets,
	}
}

// ---------------------------------------------------------------------------
// Stats handler
// ---------------------------------------------------------------------------

func handleStatsRequest(msg *Message) string {
	var req map[string]any
	_ = json.Unmarshal([]byte(msg.Payload), &req)
	action, _ := req["action"].(string)

	switch action {
	case "queries":
		limit := 100
		if v, ok := req["limit"]; ok {
			switch n := v.(type) {
			case float64:
				limit = int(n)
			case string:
				if p, err := strconv.Atoi(n); err == nil {
					limit = p
				}
			}
		}
		return marshal(map[string]any{"data": getHistory(limit), "error": ""})

	case "queries_summary":
		return marshal(map[string]any{"data": historySummary(), "error": ""})

	case "clear_queries":
		clearHistory()
		return marshal(map[string]any{"data": "cleared", "error": ""})

	default:
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		conns := 0
		if GetConnectionCount != nil {
			conns = GetConnectionCount()
		}
		return marshal(map[string]any{
			"connections": conns,
			"memory": map[string]any{
				"alloc":       m.Alloc,
				"total_alloc": m.TotalAlloc,
				"sys":         m.Sys,
				"heap_inuse":  m.HeapInuse,
				"num_gc":      m.NumGC,
			},
			"queries":    atomic.LoadInt64(&dsl.ActiveQueries),
			"goroutines": runtime.NumGoroutine(),
		})
	}
}

func marshal(v any) string {
	data, _ := json.Marshal(v)
	return string(data)
}
