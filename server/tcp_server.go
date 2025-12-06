package server

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"onql/api"
	"onql/config"
	"strings"
	"sync"

	"github.com/google/uuid"
)

const (
	endOfMessage = "\x04" // EOT - End of transmission
	msgDelimiter = "\x1E" // RS - Record separator (for fields within message)
)

// Response handler registry
type responseHandlers struct {
	handlers map[string]func(string)
	mu       sync.RWMutex
}

var handlers = &responseHandlers{
	handlers: make(map[string]func(string)),
}

// Setup starts the TCP server
func Setup(cfg *config.Config) {
	port := cfg.Port

	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		log.Fatal("Error starting TCP server:", err)
	}

	defer listener.Close()
	log.Println("🚀 Server started on port", port)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Println("Failed to accept connection:", err)
			continue
		}
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)
	connID := uuid.NewString()

	log.Printf("📡 New connection: %s", connID)

	// Response handler for this connection
	sendResponse := func(response string) {
		handlers.mu.Lock()
		defer handlers.mu.Unlock()

		response += endOfMessage
		if _, err := conn.Write([]byte(response)); err != nil {
			log.Println("Write failed:", err)
		}
	}

	handlers.mu.Lock()
	handlers.handlers[connID] = sendResponse
	handlers.mu.Unlock()

	defer func() {
		handlers.mu.Lock()
		delete(handlers.handlers, connID)
		handlers.mu.Unlock()
	}()

	for {
		message, err := reader.ReadString(endOfMessage[0])
		if err != nil {
			log.Printf("Connection closed: %s", connID)
			return
		}

		query := strings.TrimSuffix(message, endOfMessage)
		log.Printf("📨 Received: %s", query)

		// Handle request in parallel (each request in its own goroutine)
		go handleRequest(query, connID, sendResponse)
	}
}

func handleRequest(query, connID string, sendResponse func(string)) {
	// Parse message format: RID\x1Etarget\x1Edata
	parts := strings.Split(query, msgDelimiter)

	if len(parts) < 3 {
		log.Printf("Invalid message format: %s", query)
		sendResponse(fmt.Sprintf(`{"error":"invalid message format, expected: RID%starget%sdata"}`, msgDelimiter, msgDelimiter))
		return
	}

	rid := parts[0]
	target := parts[1]
	payload := parts[2]

	// Create API message
	msg := api.Message{
		ID:      connID,
		Target:  target,
		RID:     rid,
		Payload: payload,
		Type:    "request",
	}

	// Handle request through API
	response := api.HandleRequest(&msg)

	// Send response with RID for client to match
	responseMsg := fmt.Sprintf("%s%s%s%s%s", rid, msgDelimiter, target, msgDelimiter, response)
	sendResponse(responseMsg)
}
