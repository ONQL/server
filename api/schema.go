package api

import (
	"encoding/json"
	"fmt"
	"onql/storemanager"
	"strings"
)

func handleSchemaRequest(msg *Message) string {
	var command []interface{}
	if err := json.Unmarshal([]byte(msg.Payload), &command); err != nil {
		return errorResponse(fmt.Sprintf("invalid payload: %v", err))
	}

	if len(command) == 0 {
		return errorResponse("empty command")
	}

	cmd, ok := command[0].(string)
	if !ok {
		return errorResponse("invalid command type")
	}

	result, err := executeSchemaCommand(cmd, command[1:])
	if err != nil {
		return errorResponse(err.Error())
	}

	data, _ := json.Marshal(result)
	return string(data)
}

func executeSchemaCommand(cmd string, args []interface{}) (interface{}, error) {
	switch cmd {
	case "desc":
		return descSchema(args)
	case "databases":
		return listDatabases(args)
	case "tables":
		return listTables(args)
	case "create":
		return createSchema(args)
	case "set":
		return setSchema(args)
	case "drop":
		return dropSchema(args)
	case "alter":
		return alterSchema(args)
	case "rename":
		return renameSchema(args)
	default:
		return nil, fmt.Errorf("unknown command: %s", cmd)
	}
}

func descSchema(args []interface{}) (interface{}, error) {
	if len(args) == 0 {
		return db.FetchDatabases(), nil
	}

	dbName, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("invalid database name")
	}

	if len(args) == 1 {
		return db.FetchTables(dbName)
	}

	tableName, ok := args[1].(string)
	if !ok {
		return nil, fmt.Errorf("invalid table name")
	}

	return db.GetTableSchema(dbName, tableName)
}

func listDatabases(args []interface{}) (interface{}, error) {
	return db.FetchDatabases(), nil
}

func listTables(args []interface{}) (interface{}, error) {
	if len(args) < 1 {
		return nil, fmt.Errorf("tables command expects database name")
	}
	dbName, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("invalid database name")
	}
	return db.FetchTables(dbName)
}

func createSchema(args []interface{}) (interface{}, error) {
	if len(args) < 2 {
		return nil, fmt.Errorf("create expects type and name/def")
	}

	targetType, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("invalid create type")
	}

	if targetType == "db" || targetType == "database" {
		dbName, ok := args[1].(string)
		if !ok {
			return nil, fmt.Errorf("invalid database name")
		}
		if err := db.CreateDatabase(dbName); err != nil {
			return nil, err
		}
		return "success", nil
	}

	if targetType == "table" {
		if len(args) < 3 {
			return nil, fmt.Errorf("create table expects db and definition")
		}
		dbName, ok := args[1].(string)
		if !ok {
			return nil, fmt.Errorf("invalid database name")
		}

		// create table db <name> {def}
		if len(args) == 4 {
			tableName, ok := args[2].(string)
			if !ok {
				return nil, fmt.Errorf("invalid table name")
			}
			colsDef, ok := args[3].(map[string]interface{})
			if !ok {
				return nil, fmt.Errorf("invalid column definition")
			}
			table, err := parseTableDefinition(tableName, colsDef)
			if err != nil {
				return nil, err
			}
			if err := db.CreateTable(dbName, *table); err != nil {
				return nil, err
			}
			return "success", nil
		}
		return nil, fmt.Errorf("create table usage: create table <db> <table> <def>")
	}

	return nil, fmt.Errorf("unknown create target: %s", targetType)
}

func setSchema(args []interface{}) (interface{}, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("set expects schema definition")
	}

	schemaMap, ok := args[0].(map[string]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid schema format, expected JSON object")
	}

	if err := syncDatabases(schemaMap); err != nil {
		return nil, err
	}
	return "success", nil
}

func syncDatabases(targetSchema map[string]interface{}) error {
	currentDBs := db.FetchDatabases()
	existingDBs := make(map[string]bool)
	for _, name := range currentDBs {
		existingDBs[name] = true
	}

	for dbName, val := range targetSchema {
		tablesMap, ok := val.(map[string]interface{})
		if !ok {
			return fmt.Errorf("invalid format for database %s", dbName)
		}

		if !existingDBs[dbName] {
			if err := db.CreateDatabase(dbName); err != nil {
				return err
			}
		}

		if err := syncTables(dbName, tablesMap); err != nil {
			return err
		}

		delete(existingDBs, dbName)
	}

	// Database deletion removed to preserve existing databases not in schema
	// for dbName := range existingDBs {
	// 	if err := db.DropDatabase(dbName); err != nil {
	// 		return err
	// 	}
	// }

	return nil
}

func syncTables(dbName string, targetTables map[string]interface{}) error {
	currentTables, err := db.FetchTables(dbName)
	if err != nil {
		return err
	}
	existingTables := make(map[string]bool)
	for _, name := range currentTables {
		existingTables[name] = true
	}

	for tableName, val := range targetTables {
		colsDef, ok := val.(map[string]interface{})
		if !ok {
			return fmt.Errorf("invalid format for table %s.%s", dbName, tableName)
		}

		targetTable, err := parseTableDefinition(tableName, colsDef)
		if err != nil {
			return err
		}

		if !existingTables[tableName] {
			if err := db.CreateTable(dbName, *targetTable); err != nil {
				return err
			}
		} else {
			if err := syncColumns(dbName, tableName, targetTable); err != nil {
				return err
			}
		}
		delete(existingTables, tableName)
	}

	for tableName := range existingTables {
		if err := db.DropTable(dbName, tableName); err != nil {
			return err
		}
	}

	return nil
}

func syncColumns(dbName, tableName string, targetTable *storemanager.Table) error {
	oldTable, err := db.GetTableSchema(dbName, tableName)
	if err != nil {
		return err
	}

	for colName, newCol := range targetTable.Columns {
		oldCol, exists := oldTable.Columns[colName]
		if !exists {
			change := map[string]interface{}{
				"addColumn": map[string]interface{}{
					"name":      newCol.Name,
					"type":      string(newCol.Type),
					"formatter": newCol.Formatter,
					"validator": newCol.Validator,
					"indexed":   newCol.Indexed,
				},
			}
			if err := db.AlterTable(dbName, tableName, change); err != nil {
				return err
			}
		} else {
			if oldCol.Type != newCol.Type ||
				oldCol.Formatter != newCol.Formatter ||
				oldCol.Validator != newCol.Validator ||
				oldCol.Indexed != newCol.Indexed ||
				!isDefaultEqual(oldCol.DefaultValue, newCol.DefaultValue) {

				change := map[string]interface{}{
					"modifyColumn": map[string]interface{}{
						"name":      newCol.Name,
						"type":      string(newCol.Type),
						"formatter": newCol.Formatter,
						"validator": newCol.Validator,
						"indexed":   newCol.Indexed,
						"default":   newCol.DefaultValue,
					},
				}
				if err := db.AlterTable(dbName, tableName, change); err != nil {
					return err
				}
			}
		}
	}

	for colName := range oldTable.Columns {
		if _, exists := targetTable.Columns[colName]; !exists {
			if colName == oldTable.PK {
				continue
			}
			change := map[string]interface{}{
				"dropColumn": map[string]interface{}{
					"name": colName,
				},
			}
			if err := db.AlterTable(dbName, tableName, change); err != nil {
				return err
			}
		}
	}
	return nil
}

func parseTableDefinition(name string, colsDef map[string]interface{}) (*storemanager.Table, error) {
	table := &storemanager.Table{
		Name:    name,
		Columns: make(map[string]*storemanager.Column),
		PK:      "id",
	}

	for colName, def := range colsDef {
		props, ok := def.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("invalid column definition for %s", colName)
		}

		colType := getString(props, "type")
		isBlank := getString(props, "blank")
		formatter := getString(props, "formatter")
		validator := getString(props, "validator")

		if isBlank == "no" {
			if validator == "" {
				validator = "required"
			} else {
				if !containsRequired(validator) {
					validator = "required|" + validator
				}
			}
		}

		defaultValue := props["default"]

		col := &storemanager.Column{
			Name:         colName,
			Type:         storemanager.DataType(colType),
			Validator:    validator,
			Formatter:    formatter,
			DefaultValue: defaultValue,
			ID:           "", // Will be generated by CreateTable
		}
		table.Columns[colName] = col
	}

	return table, nil
}

func renameSchema(args []interface{}) (interface{}, error) {
	if len(args) < 3 {
		return nil, fmt.Errorf("rename expects at least 3 args")
	}

	if len(args) == 3 {
		oldName, _ := args[1].(string)
		newName, _ := args[2].(string)
		if err := db.RenameDatabase(oldName, newName); err != nil {
			return nil, err
		}
		return "success", nil
	}

	if len(args) == 4 {
		dbName, _ := args[1].(string)
		oldName, _ := args[2].(string)
		newName, _ := args[3].(string)
		if err := db.RenameTable(dbName, oldName, newName); err != nil {
			return nil, err
		}
		return "success", nil
	}

	return nil, fmt.Errorf("invalid rename arguments")
}

func dropSchema(args []interface{}) (interface{}, error) {
	if len(args) == 0 {
		return nil, fmt.Errorf("drop expects at least 1 arg")
	}

	dbName, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("invalid database name")
	}

	if len(args) == 1 {
		if err := db.DropDatabase(dbName); err != nil {
			return nil, err
		}
		return "success", nil
	}

	tableName, ok := args[1].(string)
	if !ok {
		return nil, fmt.Errorf("invalid table name")
	}

	if err := db.DropTable(dbName, tableName); err != nil {
		return nil, err
	}
	return "success", nil
}

func alterSchema(args []interface{}) (interface{}, error) {
	if len(args) != 3 {
		return nil, fmt.Errorf("alter expects 3 args (db, table, changes)")
	}

	dbName, _ := args[0].(string)
	tableName, _ := args[1].(string)
	changes, _ := args[2].(map[string]interface{})

	if err := db.AlterTable(dbName, tableName, changes); err != nil {
		return nil, err
	}
	return "success", nil
}

func getString(m map[string]interface{}, key string) string {
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func isDefaultEqual(v1, v2 interface{}) bool {
	if v1 == nil && v2 == nil {
		return true
	}
	if v1 == nil || v2 == nil {
		return false
	}
	// Convert both to string for comparison to handle type differences (e.g. json number vs int)
	s1 := fmt.Sprintf("%v", v1)
	s2 := fmt.Sprintf("%v", v2)
	return s1 == s2
}

func containsRequired(validator string) bool {
	return strings.Contains(validator, "required")
}
