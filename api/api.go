package api

import (
	"onql/database"
)

var db *database.DB

// SetDatabase sets the database instance for the API
func SetDatabase(database *database.DB) {
	db = database
}

// Message represents an API request/response
type Message struct {
	ID      string `json:"id"`
	Target  string `json:"target"`
	RID     string `json:"rid"`
	Payload string `json:"payload"`
	Type    string `json:"type"`
}

// HandleRequest routes API requests to appropriate handlers
func HandleRequest(msg *Message) string {
	switch msg.Target {
	case "database":
		return handleDatabaseRequest(msg)
	case "onql":
		return handleDSLRequest(msg)
	case "protocol":
		return handleProtocolRequest(msg)
	case "schema":
		return handleSchemaRequest(msg)
	case "insert":
		return HandleInsertRequest(msg)
	case "update":
		return HandleUpdateRequest(msg)
	case "delete":
		return HandleDeleteRequest(msg)
	case "stats":
		return handleStatsRequest(msg)
	default:
		return errorResponse("unknown target: " + msg.Target)
	}
}
