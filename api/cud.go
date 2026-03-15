package api

import (
	"context"
	"encoding/json"
	"fmt"
	"onql/dsl"
	"time"
)

type insertData struct {
	DB      string         `json:"db"`
	Table   string         `json:"table"`
	Records map[string]any `json:"records"`
}

type updateData struct {
	DB        string         `json:"db"`
	Table     string         `json:"table"`
	Records   map[string]any `json:"records"`
	Query     string         `json:"query"`
	Ids       []string       `json:"ids"`
	Protopass string         `json:"protopass"`
}

type deleteData struct {
	DB        string   `json:"db"`
	Table     string   `json:"table"`
	Query     string   `json:"query"`
	Ids       []string `json:"ids"`
	Protopass string   `json:"protopass"`
}

// HandleInsert handles insert operations with records
func HandleInsert(payload string) map[string]string {
	insData := insertData{}

	if err := json.Unmarshal([]byte(payload), &insData); err != nil {
		return map[string]string{"error": err.Error(), "data": ""}
	}

	id, err := db.Insert(insData.DB, insData.Table, insData.Records)
	if err != nil {
		return map[string]string{"error": err.Error(), "data": ""}
	}

	return map[string]string{"error": "", "data": id}
}

// HandleUpdate handles update operations with query or IDs
func HandleUpdate(payload string) map[string]string {
	updData := updateData{}

	if err := json.Unmarshal([]byte(payload), &updData); err != nil {
		return map[string]string{"error": err.Error(), "data": ""}
	}

	pks := []string{}

	if updData.Query != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		result, err := dsl.Execute(ctx, updData.Protopass, updData.Query, "", []string{})
		if err != nil {
			return map[string]string{"error": err.Error(), "data": ""}
		}

		if result == nil {
			pks = []string{}
		} else {
			var ok bool
			pks, ok = result.([]string)
			if !ok {
				return map[string]string{"error": "ids not returned by query", "data": ""}
			}
		}
	}

	if len(updData.Ids) != 0 {
		pks = updData.Ids
	}

	var payloadError string
	for _, pk := range pks {
		updData.Records["id"] = pk
		err := db.Update(updData.DB, updData.Table, pk, updData.Records)
		if err != nil {
			payloadError = err.Error()
			break
		}
	}

	if payloadError != "" {
		return map[string]string{"error": payloadError, "data": ""}
	}

	return map[string]string{"error": "", "data": "success"}
}

// HandleDelete handles delete operations with query or IDs
func HandleDelete(payload string) map[string]string {
	delData := deleteData{}

	if err := json.Unmarshal([]byte(payload), &delData); err != nil {
		return map[string]string{"error": err.Error(), "data": ""}
	}

	pks := []string{}

	if delData.Query != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		result, err := dsl.Execute(ctx, delData.Protopass, delData.Query, "", []string{})
		if err != nil {
			return map[string]string{"error": err.Error(), "data": ""}
		}

		if result == nil {
			pks = []string{}
		} else {
			var ok bool
			pks, ok = result.([]string)
			if !ok {
				return map[string]string{"error": "ids not returned by query", "data": ""}
			}
		}
	}

	if len(delData.Ids) != 0 {
		pks = delData.Ids
	}

	if len(pks) != 0 {
		for _, pk := range pks {
			err := db.Delete(delData.DB, delData.Table, pk)
			if err != nil {
				return map[string]string{"error": err.Error(), "data": ""}
			}
		}
	}

	return map[string]string{"error": "", "data": "success"}
}

// HandleInsertRequest handles insert API requests
func HandleInsertRequest(msg *Message) string {
	_, finish := StartQueryTrace("insert", fmt.Sprintf("%s.%s", extractField(msg.Payload, "db"), extractField(msg.Payload, "table")), len(msg.Payload))
	result := HandleInsert(msg.Payload)
	data, _ := json.Marshal(result)
	resp := string(data)
	finish(resp, result["error"])
	return resp
}

// HandleUpdateRequest handles update API requests
func HandleUpdateRequest(msg *Message) string {
	query := extractField(msg.Payload, "query")
	if query == "" {
		query = fmt.Sprintf("%s.%s", extractField(msg.Payload, "db"), extractField(msg.Payload, "table"))
	}
	_, finish := StartQueryTrace("update", query, len(msg.Payload))
	result := HandleUpdate(msg.Payload)
	data, _ := json.Marshal(result)
	resp := string(data)
	finish(resp, result["error"])
	return resp
}

// HandleDeleteRequest handles delete API requests
func HandleDeleteRequest(msg *Message) string {
	query := extractField(msg.Payload, "query")
	if query == "" {
		query = fmt.Sprintf("%s.%s", extractField(msg.Payload, "db"), extractField(msg.Payload, "table"))
	}
	_, finish := StartQueryTrace("delete", query, len(msg.Payload))
	result := HandleDelete(msg.Payload)
	data, _ := json.Marshal(result)
	resp := string(data)
	finish(resp, result["error"])
	return resp
}

func extractField(payload string, key string) string {
	var m map[string]any
	if err := json.Unmarshal([]byte(payload), &m); err != nil {
		return ""
	}
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}
