package evaluator

import (
	"context"
	"fmt"
	"onql/database"
	"onql/sql/parser"
	"onql/storemanager"
)

type Evaluator struct {
}

func Execute(ctx context.Context, stmt parser.Statement) (any, error) {
	switch s := stmt.(type) {
	case *parser.SelectStmt:
		return evalSelect(s)
	case *parser.InsertStmt:
		return evalInsert(s)
	case *parser.UpdateStmt:
		return evalUpdate(s)
	case *parser.DeleteStmt:
		return evalDelete(s)
	case *parser.CreateStmt:
		return evalCreate(s)
	case *parser.DropStmt:
		return evalDrop(s)
	case *parser.AlterStmt:
		return evalAlter(s)
	case *parser.RenameStmt:
		return evalRename(s)
	default:
		return nil, fmt.Errorf("unknown statement type")
	}
}

func evalSelect(stmt *parser.SelectStmt) (any, error) {
	// Strategy: Stop-gap In-Memory Join
	// 1. Fetch data from base table
	// 1. Fetch data from base table
	basePks, err := database.GetAllPks(stmt.DB, stmt.Table)
	if err != nil {
		return nil, err
	}

	if len(basePks) == 0 {
		// Debugging: Check if DB/Table exist
		dbs := database.FetchDatabases()
		dbExists := false
		for _, d := range dbs {
			if d == stmt.DB {
				dbExists = true
				break
			}
		}
		if !dbExists {
			return nil, fmt.Errorf("database '%s' does not exist. Available: %v", stmt.DB, dbs)
		}

		tables, err := database.FetchTables(stmt.DB)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch tables for db '%s': %v", stmt.DB, err)
		}
		tableExists := false
		for _, t := range tables {
			if t == stmt.Table {
				tableExists = true
				break
			}
		}
		if !tableExists {
			return nil, fmt.Errorf("table '%s' does not exist in db '%s'. Available: %v", stmt.Table, stmt.DB, tables)
		}

		// If both exist, then table is just empty
		return []map[string]interface{}{}, nil
	}

	baseData, err := database.GetWithPKs(stmt.DB, stmt.Table, basePks)
	if err != nil {
		return nil, err
	}

	// Convert to qualified map: "table.col" -> val
	// And "col" -> val (ambiguous? last wins? usually distinct)
	// Let's us "db.table.col" fully qualified as canonical key, and "col" as shortcut if unique

	currentRows := make([]map[string]interface{}, len(baseData))
	for i, row := range baseData {
		newRow := make(map[string]interface{})
		for k, v := range row {
			newRow[stmt.Table+"."+k] = v
			newRow[k] = v // explicit shortcut
		}
		currentRows[i] = newRow
	}

	// 2. Process Joins
	for _, join := range stmt.Joins {
		// Fetch Right Table
		rightPks, err := database.GetAllPks(join.DB, join.Table)
		if err != nil {
			return nil, err
		}
		rightData, err := database.GetWithPKs(join.DB, join.Table, rightPks)
		if err != nil {
			return nil, err
		}

		// Prepare Right Rows (qualified)
		rightRows := make([]map[string]interface{}, len(rightData))
		for i, row := range rightData {
			newRow := make(map[string]interface{})
			for k, v := range row {
				newRow[join.Table+"."+k] = v
				newRow[k] = v
			}
			rightRows[i] = newRow
		}

		// Perform Join
		joinedResult := []map[string]interface{}{}

		// Helper to check match
		matches := func(left, right map[string]interface{}) bool {
			for lKey, rKey := range join.On {
				// Keys in ON are usually "t1.col", "t2.col"

				// Normalize keys: if user supplied "col", try to find it.
				// If "table.col", it should match our keys directly if table matches short key or full key.

				// Left Lookup
				lVal, lOk := left[lKey]

				// Right Lookup
				rVal, rOk := right[rKey]

				if lOk && rOk && fmt.Sprintf("%v", lVal) == fmt.Sprintf("%v", rVal) {
					return true
				}
				return false
			}
			return true
		}

		matchedRightIndices := make(map[int]bool)

		for _, lRow := range currentRows {
			matched := false
			for rI, rRow := range rightRows {
				if matches(lRow, rRow) {
					matched = true
					matchedRightIndices[rI] = true

					// Merge
					merged := make(map[string]interface{})
					for k, v := range lRow {
						merged[k] = v
					}
					for k, v := range rRow {
						merged[k] = v
					}
					joinedResult = append(joinedResult, merged)
				}
			}

			if !matched && (join.Type == "LEFT" || join.Type == "FULL") {
				// Add Left with Nulls (implicit)
				merged := make(map[string]interface{})
				for k, v := range lRow {
					merged[k] = v
				}
				joinedResult = append(joinedResult, merged)
			}
		}

		if join.Type == "RIGHT" || join.Type == "FULL" {
			for rI, rRow := range rightRows {
				if !matchedRightIndices[rI] {
					// Add Right with Nulls
					merged := make(map[string]interface{})
					for k, v := range rRow {
						merged[k] = v
					}
					joinedResult = append(joinedResult, merged)
				}
			}
		}

		currentRows = joinedResult
	}

	// 3. Apply WHERE
	filteredRows := []map[string]interface{}{}
	for _, row := range currentRows {
		match := true
		for col, val := range stmt.Where {
			rowVal, ok := row[col]
			if !ok {
				match = false
				break
			}
			if fmt.Sprintf("%v", rowVal) != fmt.Sprintf("%v", val) {
				match = false
				break
			}
		}
		if match {
			filteredRows = append(filteredRows, row)
		}
	}

	// 4. Projection
	if len(stmt.Columns) == 1 && stmt.Columns[0] == "*" {
		return filteredRows, nil
	}

	finalRows := make([]map[string]any, len(filteredRows))
	for i, row := range filteredRows {
		newRow := make(map[string]any)
		for _, col := range stmt.Columns {
			if val, ok := row[col]; ok {
				newRow[col] = val
			}
		}
		finalRows[i] = newRow
	}

	return finalRows, nil
}

// Rest of functions (Insert, Update, Delete, etc.) identical to previous
func evalInsert(stmt *parser.InsertStmt) (any, error) {
	data := make(map[string]interface{})
	if len(stmt.Columns) != len(stmt.Values) {
		return nil, fmt.Errorf("columns count %d does not match values count %d", len(stmt.Columns), len(stmt.Values))
	}
	for i, col := range stmt.Columns {
		data[col] = stmt.Values[i]
	}

	return database.Insert(stmt.DB, stmt.Table, data)
}

func evalUpdate(stmt *parser.UpdateStmt) (any, error) {
	var pks []string
	if len(stmt.Where) > 0 {
		first := true
		for col, val := range stmt.Where {
			valStr := fmt.Sprintf("%v", val)
			currentPks, err := database.GetPksFromIndex(stmt.DB, stmt.Table, col+":"+valStr)
			if err != nil {
				return nil, err
			}
			if first {
				pks = currentPks
				first = false
			} else {
				pks = intersect(pks, currentPks)
			}
		}
	} else {
		return nil, fmt.Errorf("UPDATE without WHERE not supported (safety)")
	}
	for _, pk := range pks {
		if err := database.Update(stmt.DB, stmt.Table, pk, stmt.Set); err != nil {
			return nil, err
		}
	}
	return "success", nil
}

func evalDelete(stmt *parser.DeleteStmt) (any, error) {
	var pks []string
	if len(stmt.Where) > 0 {
		first := true
		for col, val := range stmt.Where {
			valStr := fmt.Sprintf("%v", val)
			currentPks, err := database.GetPksFromIndex(stmt.DB, stmt.Table, col+":"+valStr)
			if err != nil {
				return nil, err
			}
			if first {
				pks = currentPks
				first = false
			} else {
				pks = intersect(pks, currentPks)
			}
		}
	} else {
		return nil, fmt.Errorf("DELETE without WHERE not supported (safety)")
	}
	for _, pk := range pks {
		if err := database.Delete(stmt.DB, stmt.Table, pk); err != nil {
			return nil, err
		}
	}
	return "success", nil
}

func evalCreate(stmt *parser.CreateStmt) (any, error) {
	if stmt.Type == "DATABASE" {
		if err := database.CreateDatabase(stmt.DB); err != nil {
			return nil, err
		}
		return "success", nil
	} else if stmt.Type == "TABLE" {
		table := storemanager.Table{
			Name:    stmt.Table,
			Columns: make(map[string]*storemanager.Column),
			PK:      "id",
		}
		for name, def := range stmt.Columns {
			defMap := def.(map[string]interface{})
			typ := defMap["type"].(string)
			col := &storemanager.Column{Name: name, Type: storemanager.DataType(typ), Validator: "required"}
			table.Columns[name] = col
		}
		if _, ok := table.Columns["id"]; !ok {
			table.Columns["id"] = &storemanager.Column{Name: "id", Type: storemanager.DataType("string"), DefaultValue: "$UUID", Validator: "required"}
		}
		if err := database.CreateTable(stmt.DB, table); err != nil {
			return nil, err
		}
		return "success", nil
	}
	return nil, fmt.Errorf("unknown create type")
}

func evalDrop(stmt *parser.DropStmt) (any, error) {
	if stmt.Type == "DATABASE" {
		if err := database.DropDatabase(stmt.DB); err != nil {
			return nil, err
		}
		return "success", nil
	} else if stmt.Type == "TABLE" {
		if err := database.DropTable(stmt.DB, stmt.Table); err != nil {
			return nil, err
		}
		return "success", nil
	}
	return nil, fmt.Errorf("unknown drop type")
}

func evalAlter(stmt *parser.AlterStmt) (any, error) {
	change := make(map[string]interface{})
	if stmt.Action == "ADD" {
		def := stmt.ColDef
		typ := def["type"].(string)
		change["addColumn"] = map[string]interface{}{"name": stmt.ColName, "type": typ, "validator": "required", "indexed": true}
	} else if stmt.Action == "DROP" {
		change["dropColumn"] = map[string]interface{}{"name": stmt.ColName}
	} else if stmt.Action == "MODIFY" {
		def := stmt.ColDef
		typ := def["type"].(string)
		change["modifyColumn"] = map[string]interface{}{"name": stmt.ColName, "type": typ}
	}
	if err := database.AlterTable(stmt.DB, stmt.Table, change); err != nil {
		return nil, err
	}
	return "success", nil
}

func evalRename(stmt *parser.RenameStmt) (any, error) {
	if stmt.Type == "DATABASE" {
		if err := database.RenameDatabase(stmt.OldName, stmt.NewName); err != nil {
			return nil, err
		}
		return "success", nil
	} else if stmt.Type == "TABLE" {
		if err := database.RenameTable(stmt.DB, stmt.OldName, stmt.NewName); err != nil {
			return nil, err
		}
		return "success", nil
	}
	return nil, fmt.Errorf("unknown rename types")
}

func intersect(a, b []string) []string {
	seen := make(map[string]struct{}, len(a))
	for _, x := range a {
		seen[x] = struct{}{}
	}
	out := make([]string, 0)
	for _, x := range b {
		if _, ok := seen[x]; ok {
			out = append(out, x)
		}
	}
	return out
}
