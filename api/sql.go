package api

import (
	"context"
	"encoding/json"
	"fmt"
	"onql/sql"
	"time"
)

// SQLRequest represents a SQL query request
type SQLRequest struct {
	Query string `json:"query"`
}

func handleSQLRequest(msg *Message) string {
	var req SQLRequest
	if err := json.Unmarshal([]byte(msg.Payload), &req); err != nil {
		return errorResponse(fmt.Sprintf("invalid payload: %v", err))
	}

	// Create context with timeout of 60 seconds
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	result, err := sql.Execute(ctx, req.Query)

	response := map[string]interface{}{
		"data":  result,
		"error": "",
	}
	if err != nil {
		response["error"] = err.Error()
		response["data"] = nil
	}

	data, _ := json.Marshal(response)
	return string(data)
}
