package storemanager

import (
	"onql/config"
	"testing"
	"time"
)

func TestAlterTable(t *testing.T) {
	engine := NewMockEngine()
	cfg := &config.Config{FlushInterval: 100 * time.Millisecond}
	sm := New(engine, cfg)
	defer sm.Close()

	// Setup: Create DB and Table
	dbName := "alterdb"
	sm.CreateDatabase(dbName)
	tableName := "users"
	table := Table{
		Name: tableName,
		PK:   "id",
		Columns: map[string]*Column{
			"id":  {Name: "id", Type: TypeString, Indexed: true},
			"age": {Name: "age", Type: TypeNumber, Indexed: true},
		},
	}
	sm.CreateTable(dbName, table)

	// Insert data to test indices
	sm.Insert(dbName, tableName, Row{Data: map[string]interface{}{"id": "1", "age": 30}})

	// 1. Add Column
	err := sm.AlterTable(dbName, tableName, map[string]interface{}{
		"addColumn": map[string]interface{}{
			"name":    "email",
			"type":    "string",
			"indexed": true,
		},
	})
	if err != nil {
		t.Fatalf("Add Column failed: %v", err)
	}

	// Verify
	dbID, tbl, _ := sm.GetTableSchema(dbName, tableName)
	if _, ok := tbl.Columns["email"]; !ok {
		t.Errorf("Column email not added")
	}
	if tbl.Columns["email"].ID == "" {
		t.Errorf("Column email has no ID")
	}

	// 2. Rename Column
	err = sm.AlterTable(dbName, tableName, map[string]interface{}{
		"renameColumn": map[string]interface{}{
			"oldName": "age",
			"newName": "years",
		},
	})
	if err != nil {
		t.Fatalf("Rename Column failed: %v", err)
	}

	// Verify
	if _, ok := tbl.Columns["age"]; ok {
		t.Errorf("Old column age still exists")
	}
	if _, ok := tbl.Columns["years"]; !ok {
		t.Errorf("New column years not found")
	}

	// Verify Index Access (should still work via ID)
	// We need to know the ID of "years" (which was "age")
	colID := tbl.Columns["years"].ID
	// Index Key: IDX:dbID:tblID:colID:30:1
	// Check if key exists in engine (flush first?)
	sm.Flush()

	idxKey := IndexKey(dbID, tbl.ID, colID, "30", "1")
	if _, err := engine.Get(idxKey); err != nil {
		t.Errorf("Index for renamed column not found: %v", err)
	}

	// 3. Modify Column
	err = sm.AlterTable(dbName, tableName, map[string]interface{}{
		"modifyColumn": map[string]interface{}{
			"name": "years",
			"type": "string", // Change to string
		},
	})
	if err != nil {
		t.Fatalf("Modify Column failed: %v", err)
	}
	if tbl.Columns["years"].Type != TypeString {
		t.Errorf("Column type not updated")
	}

	// 4. Drop Column
	err = sm.AlterTable(dbName, tableName, map[string]interface{}{
		"dropColumn": map[string]interface{}{
			"name": "years",
		},
	})
	if err != nil {
		t.Fatalf("Drop Column failed: %v", err)
	}

	// Verify
	if _, ok := tbl.Columns["years"]; ok {
		t.Errorf("Column years not dropped")
	}

	// Verify Index Removed
	if _, err := engine.Get(idxKey); err == nil {
		t.Errorf("Index for dropped column still exists")
	}
}
