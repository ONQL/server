package api

import (
	"context"
	"encoding/json"
	"fmt"
	"onql/dsl"
	"time"
)

// DSLRequest represents a DSL query request
type DSLRequest struct {
	Protopass string   `json:"protopass"`
	Query     string   `json:"query"`
	CtxKey    string   `json:"ctxkey"`
	CtxValues []string `json:"ctxvalues"`
}

func handleDSLRequest(msg *Message) string {
	var req DSLRequest
	if err := json.Unmarshal([]byte(msg.Payload), &req); err != nil {
		return errorResponse(fmt.Sprintf("invalid payload: %v", err))
	}

	// Create context with timeout of 60 seconds
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	result, err := dsl.Execute(ctx, req.Protopass, req.Query, req.CtxKey, req.CtxValues)

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
