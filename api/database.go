package api

import (
	"encoding/json"
	"fmt"
	"onql/storemanager"
)

// DatabaseRequest represents a database function call
type DatabaseRequest struct {
	Function string            `json:"function"`
	Args     []json.RawMessage `json:"args"`
}

func handleDatabaseRequest(msg *Message) string {
	var req DatabaseRequest
	if err := json.Unmarshal([]byte(msg.Payload), &req); err != nil {
		return errorResponse(fmt.Sprintf("invalid payload: %v", err))
	}

	result, err := callDatabaseFunc(req.Function, req.Args)
	if err != nil {
		return errorResponse(err.Error())
	}

	data, _ := json.Marshal(result)
	return string(data)
}

func callDatabaseFunc(name string, args []json.RawMessage) (any, error) {
	switch name {
	case "GetDatabases":
		return db.FetchDatabases(), nil

	case "GetTables":
		if len(args) != 1 {
			return nil, fmt.Errorf("GetTables expects 1 arg")
		}
		var dbName string
		json.Unmarshal(args[0], &dbName)
		return db.FetchTables(dbName)

	case "CreateDatabase":
		if len(args) != 1 {
			return nil, fmt.Errorf("CreateDatabase expects 1 arg")
		}
		var name string
		json.Unmarshal(args[0], &name)
		if err := db.CreateDatabase(name); err != nil {
			return nil, err
		}
		return "success", nil

	case "DropDatabase":
		if len(args) != 1 {
			return nil, fmt.Errorf("DropDatabase expects 1 arg")
		}
		var name string
		json.Unmarshal(args[0], &name)
		if err := db.DropDatabase(name); err != nil {
			return nil, err
		}
		return "success", nil

	case "CreateTable":
		if len(args) != 2 {
			return nil, fmt.Errorf("CreateTable expects 2 args")
		}
		var dbName string
		var table storemanager.Table
		json.Unmarshal(args[0], &dbName)
		json.Unmarshal(args[1], &table)
		if err := db.CreateTable(dbName, table); err != nil {
			return nil, err
		}
		return "success", nil

	case "DropTable":
		if len(args) != 2 {
			return nil, fmt.Errorf("DropTable expects 2 args")
		}
		var dbName, tableName string
		json.Unmarshal(args[0], &dbName)
		json.Unmarshal(args[1], &tableName)
		if err := db.DropTable(dbName, tableName); err != nil {
			return nil, err
		}
		return "success", nil

	case "Insert":
		if len(args) != 3 {
			return nil, fmt.Errorf("Insert expects 3 args")
		}
		var dbName, tableName string
		var data map[string]interface{}
		json.Unmarshal(args[0], &dbName)
		json.Unmarshal(args[1], &tableName)
		json.Unmarshal(args[2], &data)
		return db.Insert(dbName, tableName, data)

	case "Get":
		if len(args) != 3 {
			return nil, fmt.Errorf("Get expects 3 args")
		}
		var dbName, tableName, pk string
		json.Unmarshal(args[0], &dbName)
		json.Unmarshal(args[1], &tableName)
		json.Unmarshal(args[2], &pk)
		return db.Get(dbName, tableName, pk)

	case "Update":
		if len(args) != 4 {
			return nil, fmt.Errorf("Update expects 4 args")
		}
		var dbName, tableName, pk string
		var data map[string]interface{}
		json.Unmarshal(args[0], &dbName)
		json.Unmarshal(args[1], &tableName)
		json.Unmarshal(args[2], &pk)
		json.Unmarshal(args[3], &data)
		if err := db.Update(dbName, tableName, pk, data); err != nil {
			return nil, err
		}
		return "success", nil

	case "Delete":
		if len(args) != 3 {
			return nil, fmt.Errorf("Delete expects 3 args")
		}
		var dbName, tableName, pk string
		json.Unmarshal(args[0], &dbName)
		json.Unmarshal(args[1], &tableName)
		json.Unmarshal(args[2], &pk)
		if err := db.Delete(dbName, tableName, pk); err != nil {
			return nil, err
		}
		return "success", nil

	default:
		return nil, fmt.Errorf("function %q not found", name)
	}
}
