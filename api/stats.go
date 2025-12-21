package api

import (
	"encoding/json"
	"onql/dsl"
	"runtime"
	"sync/atomic"
)

// StatsResponse represents the server statistics
type StatsResponse struct {
	Connections int         `json:"connections"`
	Memory      MemoryStats `json:"memory"`
	Queries     int64       `json:"queries"`
	GoRoutines  int         `json:"goroutines"`
}

type MemoryStats struct {
	Alloc      uint64 `json:"alloc"`
	TotalAlloc uint64 `json:"total_alloc"`
	Sys        uint64 `json:"sys"`
	NumGC      uint32 `json:"num_gc"`
}

func handleStatsRequest(msg *Message) string {
	// Gather memory stats
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	// Gather other stats
	// Note: We need to import onql/server which might cause import cycle if server imports api.
	// API imports Server? server imports api to call HandleRequest.
	// ERROR: Import Cycle! atomic -> server -> api -> server.
	// To fix this, GetConnectionCount should probably be in a separate package or passed in?
	// OR `api` should define an interface or variable that `server` updates.
	// But `server` calls `api.HandleRequest`.
	// For now, I will omit `server.GetConnectionCount` if it causes a cycle, OR refrain from importing `server`.
	// Let's check imports.
	// server/tcp_server.go imports "onql/api".
	// If I import "onql/server" here, it's a cycle.

	// FIX: Move GetConnectionCount logic? Or use a callback.
	// For this task, I will define a global variable in `api` package `ActiveConnections`
	// and have `server` update it? No, keeping state in `api` is okay.
	// Better: `api` can have `SetConnectionCounter(func() int)`.

	// Let's use the callback approach.

	connections := 0
	if GetConnectionCount != nil {
		connections = GetConnectionCount()
	}

	stats := StatsResponse{
		Connections: connections,
		Memory: MemoryStats{
			Alloc:      m.Alloc,
			TotalAlloc: m.TotalAlloc,
			Sys:        m.Sys,
			NumGC:      m.NumGC,
		},
		Queries:    atomic.LoadInt64(&dsl.ActiveQueries),
		GoRoutines: runtime.NumGoroutine(),
	}

	data, _ := json.Marshal(stats)
	return string(data)
}

// Global variable to break dependency cycle
var GetConnectionCount func() int
