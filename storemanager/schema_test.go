package storemanager

import (
	"fmt"
	"onql/config"
	"strings"
	"testing"
	"time"
)

// MockEngine implements Engine interface for testing
type MockEngine struct {
	data map[string][]byte
}

func NewMockEngine() *MockEngine {
	return &MockEngine{
		data: make(map[string][]byte),
	}
}

func (m *MockEngine) Set(key, value []byte) error {
	m.data[string(key)] = value
	return nil
}

func (m *MockEngine) Get(key []byte) ([]byte, error) {
	val, ok := m.data[string(key)]
	if !ok {
		return nil, fmt.Errorf("not found")
	}
	return val, nil
}

func (m *MockEngine) Delete(key []byte) error {
	delete(m.data, string(key))
	return nil
}

func (m *MockEngine) BatchSet(keys, values [][]byte) error {
	for i, k := range keys {
		m.data[string(k)] = values[i]
	}
	return nil
}

func (m *MockEngine) IteratePrefix(prefix []byte, fn func(k, v []byte) error) error {
	for k, v := range m.data {
		if strings.HasPrefix(k, string(prefix)) {
			if err := fn([]byte(k), v); err != nil {
				return err
			}
		}
	}
	return nil
}

func TestSchemaRefactoring(t *testing.T) {
	engine := NewMockEngine()
	cfg := &config.Config{FlushInterval: 100 * time.Millisecond}
	sm := New(engine, cfg)
	defer sm.Close()

	// 1. Create Database
	dbName := "testdb"
	err := sm.CreateDatabase(dbName)
	if err != nil {
		t.Fatalf("CreateDatabase failed: %v", err)
	}

	// Verify DB ID generated
	db, ok := sm.schema.Databases[dbName]
	if !ok {
		t.Fatalf("Database not found in memory")
	}
	if db.ID == "" {
		t.Fatalf("Database ID not generated")
	}
	t.Logf("Database ID: %s", db.ID)

	// Verify Map
	mapVal, err := engine.Get(MapDBKey(dbName))
	if err != nil {
		t.Fatalf("MapDBKey not found: %v", err)
	}
	if string(mapVal) != db.ID {
		t.Errorf("MapDBKey value mismatch. Got %s, want %s", string(mapVal), db.ID)
	}

	// 2. Create Table
	tableName := "users"
	table := Table{
		Name: tableName,
		PK:   "id",
		Columns: map[string]*Column{
			"id":   {Name: "id", Type: TypeString, Indexed: true},
			"name": {Name: "name", Type: TypeString, Indexed: true},
		},
	}
	err = sm.CreateTable(dbName, table)
	if err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// Verify Table ID generated
	tbl, ok := db.Tables[tableName]
	if !ok {
		t.Fatalf("Table not found in memory")
	}
	if tbl.ID == "" {
		t.Fatalf("Table ID not generated")
	}
	t.Logf("Table ID: %s", tbl.ID)

	// Verify Map
	mapTblVal, err := engine.Get(MapTableKey(db.ID, tableName))
	if err != nil {
		t.Fatalf("MapTableKey not found: %v", err)
	}
	if string(mapTblVal) != tbl.ID {
		t.Errorf("MapTableKey value mismatch. Got %s, want %s", string(mapTblVal), tbl.ID)
	}

	// 3. Insert Data
	row := Row{
		Data: map[string]interface{}{
			"id":   "u1",
			"name": "Alice",
		},
	}
	err = sm.Insert(dbName, tableName, row)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// Verify Data Key uses IDs
	// We need to flush buffer to check engine, or check buffer directly.
	// Let's check buffer first.
	dataKey := string(DataKey(db.ID, tbl.ID, "u1"))
	if _, exists, _ := sm.buffer.Get(dataKey); !exists {
		t.Errorf("Data not found in buffer with key %s", dataKey)
	}

	// 4. Rename Table
	newTableName := "customers"
	err = sm.RenameTable(dbName, tableName, newTableName)
	if err != nil {
		t.Fatalf("RenameTable failed: %v", err)
	}

	// Verify Memory
	if _, ok := db.Tables[tableName]; ok {
		t.Errorf("Old table name still exists in memory")
	}
	if _, ok := db.Tables[newTableName]; !ok {
		t.Errorf("New table name not found in memory")
	}

	// Verify Data Access via new name
	retrievedRow, err := sm.Get(dbName, newTableName, "u1")
	if err != nil {
		t.Fatalf("Get failed with new table name: %v", err)
	}
	if retrievedRow.Data["name"] != "Alice" {
		t.Errorf("Data mismatch. Got %v, want Alice", retrievedRow.Data["name"])
	}

	// 5. Protocol Context
	protoPass := "secret"
	protocol := QueryProtocol{
		dbName: &ProtocolModule{
			Database: dbName,
			Entities: map[string]*Entity{
				"User": {
					Table: newTableName,
					Fields: map[string]string{
						"ID":   "id",
						"Name": "name",
					},
					Context: map[string]string{
						"role": "admin",
					},
				},
			},
		},
	}
	err = sm.SetProtocol(protoPass, protocol)
	if err != nil {
		t.Fatalf("SetProtocol failed: %v", err)
	}

	// Verify Context Retrieval
	ctxVal, err := sm.GetProtoContext(protoPass, "User", "role")
	if err != nil {
		t.Fatalf("GetProtoContext failed: %v", err)
	}
	if ctxVal != "admin" {
		t.Errorf("Context mismatch. Got %s, want admin", ctxVal)
	}

	// 6. Batch Operations
	// Insert another row
	row2 := Row{
		Data: map[string]interface{}{
			"id":   "u2",
			"name": "Bob",
		},
	}
	err = sm.Insert(dbName, newTableName, row2)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// GetAllPks
	pks, err := sm.GetAllPks(dbName, newTableName)
	if err != nil {
		t.Fatalf("GetAllPks failed: %v", err)
	}
	if len(pks) != 2 {
		t.Errorf("GetAllPks count mismatch. Got %d, want 2", len(pks))
	}
	// Check content (order not guaranteed)
	pkMap := make(map[string]bool)
	for _, pk := range pks {
		pkMap[pk] = true
	}
	if !pkMap["u1"] || !pkMap["u2"] {
		t.Errorf("GetAllPks missing expected keys. Got %v", pks)
	}

	// GetDataByPKs
	rows, err := sm.GetDataByPKs(dbName, newTableName, []string{"u1", "u2"})
	if err != nil {
		t.Fatalf("GetDataByPKs failed: %v", err)
	}
	if len(rows) != 2 {
		t.Errorf("GetDataByPKs count mismatch. Got %d, want 2", len(rows))
	}

	// GetPkByIndex (Slice return)
	// u1 -> Alice, u2 -> Bob
	foundPKs, err := sm.GetPkByIndex(dbName, newTableName, "name", "Alice")
	if err != nil {
		t.Fatalf("GetPkByIndex failed: %v", err)
	}
	if len(foundPKs) != 1 || foundPKs[0] != "u1" {
		t.Errorf("GetPkByIndex mismatch. Got %v, want [u1]", foundPKs)
	}
}
