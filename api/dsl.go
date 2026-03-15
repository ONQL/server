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

	_, finish := StartQueryTrace("onql", req.Query, len(msg.Payload))

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	result, err := dsl.Execute(ctx, req.Protopass, req.Query, req.CtxKey, req.CtxValues)

	response := map[string]interface{}{
		"data":  result,
		"error": "",
	}
	errMsg := ""
	if err != nil {
		errMsg = err.Error()
		response["error"] = errMsg
		response["data"] = nil
	}

	data, _ := json.Marshal(response)
	resp := string(data)
	finish(resp, errMsg)
	return resp
}
