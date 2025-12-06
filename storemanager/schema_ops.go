/*
Business Source License 1.1

Parameters
Licensor:             Autobit Software Services Private Limited
Licensed Work:        ONQL (Database Engine)
The Licensed Work is (c) 2025 Autobit Software Services Private Limited.
Change Date:          2028-01-01
Change License:       GNU General Public License, version 3 or later

Terms
The Business Source License (this “License”) grants you the right to copy,
modify, and redistribute the Licensed Work, provided that you do not use the
Licensed Work for a Commercial Use.

“Commercial Use” means offering the Licensed Work to third parties as a
paid service, product, or part of a service or product for which you or a
third party receives payment or other consideration.

You may make use of the Licensed Work for internal use, research, evaluation,
education, and non-commercial purposes, and you may contribute modifications
back to the Licensor under the same License.

Before the Change Date, use of the Licensed Work in violation of this License
automatically terminates your rights.  After the Change Date, the Licensed Work
will be governed by the Change License.

The Licensor may make an Additional Use Grant allowing specific commercial
uses by prior written permission.

THE LICENSED WORK IS PROVIDED “AS IS” AND WITHOUT WARRANTY OF ANY KIND,
EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO WARRANTIES OF
MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE, OR NON-INFRINGEMENT.

This License does not grant trademark rights.  The ONQL name and logo are
trademarks of Autobit Software Services Private Limited and may not be used
without written permission.

For more details see: https://mariadb.com/bsl11/
*/

package storemanager

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"onql/common"
	"strings"
	"time"
)

// generateID creates a unique 32-character hexadecimal ID.
// It uses crypto/rand for secure random number generation.
// If random generation fails, it falls back to a timestamp-based ID.
func generateID() string {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		// Fallback or panic, but for now just return a timestamp based fallback if rand fails (unlikely)
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}

// CreateDatabase creates a new database with the given name.
// It generates a unique ID for the database and stores the mapping and metadata.
// Returns an error if a database with the same name already exists.
func (sm *StoreManager) CreateDatabase(name string) error {
	sm.schema.Mu.Lock()
	defer sm.schema.Mu.Unlock()

	if _, exists := sm.schema.Databases[name]; exists {
		return common.ErrDatabaseExists
	}

	id := generateID()
	db := &Database{
		ID:     id,
		Name:   name,
		Tables: make(map[string]*Table),
	}
	sm.schema.Databases[name] = db

	// Persist
	// 1. Map Name -> ID
	if err := sm.engine.Set(MapDBKey(name), []byte(id)); err != nil {
		return err
	}
	// 2. Store DB Metadata
	data, err := json.Marshal(db)
	if err != nil {
		return err
	}
	if err := sm.engine.Set(MetaDBKey(id), data); err != nil {
		return err
	}
	go sm.UpdateDefaultProtocol()
	return nil
}

// CreateTable creates a new table within a specified database.
// It generates unique IDs for the table and its columns.
// Returns an error if the database does not exist or if the table already exists.
func (sm *StoreManager) CreateTable(dbName string, table Table) error {
	sm.schema.Mu.Lock()
	defer sm.schema.Mu.Unlock()

	db, exists := sm.schema.Databases[dbName]
	if !exists {
		return fmt.Errorf("database %s does not exist", dbName)
	}

	if _, exists := db.Tables[table.Name]; exists {
		return fmt.Errorf("table %s already exists", table.Name)
	}

	// Validate PK
	if _, ok := table.Columns[table.PK]; !ok {
		return fmt.Errorf("primary key column %s not defined", table.PK)
	}

	// Generate IDs
	table.ID = generateID()
	for _, col := range table.Columns {
		col.ID = generateID()
		col.Indexed = true // Enforce indexing
	}

	db.Tables[table.Name] = &table

	// Persist
	// 1. Map Table Name -> ID
	if err := sm.engine.Set(MapTableKey(db.ID, table.Name), []byte(table.ID)); err != nil {
		return err
	}
	// 2. Store Table Metadata
	data, err := json.Marshal(table)
	if err != nil {
		return err
	}
	if err := sm.engine.Set(MetaTableKey(db.ID, table.ID), data); err != nil {
		return err
	}
	go sm.UpdateDefaultProtocol()
	return nil
}

// LoadSchema loads the entire database schema from the storage engine into memory.
// It iterates over metadata keys to reconstruct the Database and Table objects.
// This is typically called on startup.
func (sm *StoreManager) LoadSchema() error {
	sm.schema.Mu.Lock()
	defer sm.schema.Mu.Unlock()

	// 1. Load Databases
	// Iterate META:DB: to get all DB structs (which contain ID and Name)
	// We don't strictly need to iterate MAP:DB: if META:DB: contains the name.
	// But we need to populate sm.schema.Databases[name]

	prefix := []byte("META:DB:")
	err := sm.engine.IteratePrefix(prefix, func(k, v []byte) error {
		var db Database
		if err := json.Unmarshal(v, &db); err != nil {
			return err
		}
		if db.Tables == nil {
			db.Tables = make(map[string]*Table)
		}
		sm.schema.Databases[db.Name] = &db
		return nil
	})
	if err != nil {
		return err
	}

	// 2. Load Tables
	// We need to iterate META:TBL: which is META:TBL:<dbID>:<tableID>
	// But we need to know which DB it belongs to.
	// The key structure is META:TBL:<dbID>:<tableID>
	// We can iterate all META:TBL: and parse the key.

	prefix = []byte("META:TBL:")
	err = sm.engine.IteratePrefix(prefix, func(k, v []byte) error {
		// Key is META:TBL:dbID:tableID
		parts := strings.Split(string(k), ":")
		if len(parts) < 4 {
			return nil
		}
		dbID := parts[2]

		var table Table
		if err := json.Unmarshal(v, &table); err != nil {
			return err
		}

		// Re-parse rules
		for _, col := range table.Columns {
			if col.Formatter != "" {
				col.FormatterRules = strings.Split(col.Formatter, "|")
			}
			if col.Validator != "" {
				col.ValidatorRules = strings.Split(col.Validator, "|")
			}
		}

		// Find DB by ID
		var foundDB *Database
		for _, db := range sm.schema.Databases {
			if db.ID == dbID {
				foundDB = db
				break
			}
		}

		if foundDB != nil {
			foundDB.Tables[table.Name] = &table
		}
		return nil
	})
	return err
}

// FetchDatabases returns a list of all database names.
func (sm *StoreManager) FetchDatabases() []string {
	sm.schema.Mu.RLock()
	defer sm.schema.Mu.RUnlock()
	var dbs []string
	for name := range sm.schema.Databases {
		dbs = append(dbs, name)
	}
	return dbs
}

// FetchTables returns a list of all table names in the specified database.
func (sm *StoreManager) FetchTables(dbName string) ([]string, error) {
	sm.schema.Mu.RLock()
	defer sm.schema.Mu.RUnlock()
	db, ok := sm.schema.Databases[dbName]
	if !ok {
		return nil, fmt.Errorf("database %s not found", dbName)
	}
	var tables []string
	for name := range db.Tables {
		tables = append(tables, name)
	}
	return tables, nil
}

// RenameDatabase renames an existing database.
// It updates the name mapping and the database metadata.
// Data keys are not affected as they use the immutable Database ID.
func (sm *StoreManager) RenameDatabase(oldName, newName string) error {
	sm.schema.Mu.Lock()
	defer sm.schema.Mu.Unlock()

	if _, exists := sm.schema.Databases[newName]; exists {
		return common.ErrDatabaseExists
	}
	db, exists := sm.schema.Databases[oldName]
	if !exists {
		return common.ErrNotFound
	}

	// With IDs, we only need to update the Name mapping and the Name field in DB struct.
	// No need to rewrite data keys!

	// 1. Update Map Name -> ID
	// Delete old map
	if err := sm.engine.Delete(MapDBKey(oldName)); err != nil {
		return err
	}
	// Set new map
	if err := sm.engine.Set(MapDBKey(newName), []byte(db.ID)); err != nil {
		return err
	}

	// 2. Update DB Metadata
	db.Name = newName
	data, err := json.Marshal(db)
	if err != nil {
		return err
	}
	if err := sm.engine.Set(MetaDBKey(db.ID), data); err != nil {
		return err
	}

	// 3. Update in-memory schema
	sm.schema.Databases[newName] = db
	delete(sm.schema.Databases, oldName)

	go sm.UpdateDefaultProtocol()
	return nil
}

// DropDatabase deletes a database and all its contents.
// This includes deleting all tables, data, and indices associated with the database.
// WARNING: This operation is irreversible.
func (sm *StoreManager) DropDatabase(name string) error {
	sm.schema.Mu.Lock()
	defer sm.schema.Mu.Unlock()

	db, exists := sm.schema.Databases[name]
	if !exists {
		return common.ErrNotFound
	}

	// 1. Delete all tables
	// We need to delete data keys too.
	// Iterate prefix DATA:dbID: and IDX:dbID: and META:TBL:dbID:
	// This is still heavy but necessary for cleanup.

	// Delete Map
	if err := sm.engine.Delete(MapDBKey(name)); err != nil {
		return err
	}
	// Delete Meta
	if err := sm.engine.Delete(MetaDBKey(db.ID)); err != nil {
		return err
	}

	delete(sm.schema.Databases, name)
	go sm.UpdateDefaultProtocol()
	return nil
}

// RenameTable renames a table within a database.
// It updates the name mapping and the table metadata.
// Data keys are not affected as they use the immutable Table ID.
func (sm *StoreManager) RenameTable(dbName, oldName, newName string) error {
	sm.schema.Mu.Lock()
	defer sm.schema.Mu.Unlock()

	db, ok := sm.schema.Databases[dbName]
	if !ok {
		return common.ErrNotFound
	}
	table, ok := db.Tables[oldName]
	if !ok {
		return common.ErrNotFound
	}
	if _, exists := db.Tables[newName]; exists {
		return common.ErrTableExists
	}

	// Update memory
	table.Name = newName
	db.Tables[newName] = table
	delete(db.Tables, oldName)

	// Persist
	// 1. Update Map Table Name -> ID
	if err := sm.engine.Delete(MapTableKey(db.ID, oldName)); err != nil {
		return err
	}
	if err := sm.engine.Set(MapTableKey(db.ID, newName), []byte(table.ID)); err != nil {
		return err
	}

	// 2. Update Table Metadata
	data, err := json.Marshal(table)
	if err != nil {
		return err
	}
	if err := sm.engine.Set(MetaTableKey(db.ID, table.ID), data); err != nil {
		return err
	}
	go sm.UpdateDefaultProtocol()
	return nil
}

// DropTable drops a table from a database.
// It removes the table metadata and mapping.
// Note: This currently does not delete the actual data rows or indices for efficiency,
// but a background cleanup process should ideally handle that.
func (sm *StoreManager) DropTable(dbName, tableName string) error {
	sm.schema.Mu.Lock()
	defer sm.schema.Mu.Unlock()

	db, ok := sm.schema.Databases[dbName]
	if !ok {
		return common.ErrNotFound
	}
	table, ok := db.Tables[tableName]
	if !ok {
		return common.ErrNotFound
	}

	delete(db.Tables, tableName)

	// Delete Map
	if err := sm.engine.Delete(MapTableKey(db.ID, tableName)); err != nil {
		return err
	}
	// Delete Meta
	if err := sm.engine.Delete(MetaTableKey(db.ID, table.ID)); err != nil {
		return err
	}
	go sm.UpdateDefaultProtocol()
	return nil
}

// AlterTable modifies the structure of an existing table.
// Supported operations: addColumn, dropColumn, modifyColumn, renameColumn.
// It handles ID generation for new columns and index cleanup for dropped columns.
func (sm *StoreManager) AlterTable(dbName, tableName string, changes map[string]interface{}) error {
	// Acquire write lock for operations that modify structure
	// (renameColumn, dropColumn need migration lock; addColumn/modifyColumn are safer)
	if _, hasRename := changes["renameColumn"]; hasRename {
		sm.migrationLock.Lock()
		defer sm.migrationLock.Unlock()
	}

	sm.schema.Mu.Lock()
	defer sm.schema.Mu.Unlock()

	db, ok := sm.schema.Databases[dbName]
	if !ok {
		return common.ErrNotFound
	}
	table, ok := db.Tables[tableName]
	if !ok {
		return common.ErrNotFound
	}

	// Supported operations:
	// - addColumn: { name, type, formatter, validator, indexed }
	// - dropColumn: { name }
	// - modifyColumn: { name, type, formatter, validator, indexed }
	// - renameColumn: { oldName, newName }

	// Add Column
	if addCol, ok := changes["addColumn"]; ok {
		colMap := addCol.(map[string]interface{})
		colName := colMap["name"].(string)

		if _, exists := table.Columns[colName]; exists {
			return fmt.Errorf("column %s already exists", colName)
		}

		colTypeStr, _ := colMap["type"].(string)
		col := &Column{
			Name:      colName,
			Type:      DataType(colTypeStr),
			Formatter: getString(colMap, "formatter"),
			Validator: getString(colMap, "validator"),
			Indexed:   true, // Enforce indexing
			ID:        generateID(),
		}
		// Parse rules
		if col.Formatter != "" {
			col.FormatterRules = strings.Split(col.Formatter, "|")
		}
		if col.Validator != "" {
			col.ValidatorRules = strings.Split(col.Validator, "|")
		}

		table.Columns[colName] = col
	}

	// Drop Column
	if dropCol, ok := changes["dropColumn"]; ok {
		colMap := dropCol.(map[string]interface{})
		colName := colMap["name"].(string)

		if colName == table.PK {
			return fmt.Errorf("cannot drop primary key column")
		}

		col, exists := table.Columns[colName]
		if !exists {
			return fmt.Errorf("column %s does not exist", colName)
		}

		// Remove indices
		if col.Indexed {
			// IDX:dbID:tableID:colID:
			prefix := fmt.Sprintf("IDX:%s:%s:%s:", db.ID, table.ID, col.ID)
			keysToDelete := make([][]byte, 0)
			sm.engine.IteratePrefix([]byte(prefix), func(k, v []byte) error {
				keysToDelete = append(keysToDelete, k)
				return nil
			})
			for _, k := range keysToDelete {
				sm.engine.Delete(k)
			}
		}

		delete(table.Columns, colName)
	}

	// Modify Column
	if modCol, ok := changes["modifyColumn"]; ok {
		colMap := modCol.(map[string]interface{})
		colName := colMap["name"].(string)

		existingCol, exists := table.Columns[colName]
		if !exists {
			return fmt.Errorf("column %s does not exist", colName)
		}

		// Update properties if provided
		if typeStr, ok := colMap["type"].(string); ok {
			existingCol.Type = DataType(typeStr)
		}
		if formatter, ok := colMap["formatter"].(string); ok {
			existingCol.Formatter = formatter
			existingCol.FormatterRules = strings.Split(formatter, "|")
		}
		if validator, ok := colMap["validator"].(string); ok {
			existingCol.Validator = validator
			existingCol.ValidatorRules = strings.Split(validator, "|")
		}
		// Enforce indexing always
		existingCol.Indexed = true
	}

	// Rename Column
	if renCol, ok := changes["renameColumn"]; ok {
		colMap := renCol.(map[string]interface{})
		oldName := colMap["oldName"].(string)
		newName := colMap["newName"].(string)

		col, exists := table.Columns[oldName]
		if !exists {
			return fmt.Errorf("column %s does not exist", oldName)
		}
		if _, exists := table.Columns[newName]; exists {
			return fmt.Errorf("column %s already exists", newName)
		}

		// Update column name
		col.Name = newName
		table.Columns[newName] = col
		delete(table.Columns, oldName)

		// Update PK if renamed
		if table.PK == oldName {
			table.PK = newName
		}

		// No need to migrate indices because they use Column ID, which hasn't changed!
	}

	// Persist
	data, err := json.Marshal(table)
	if err != nil {
		return err
	}
	if err := sm.engine.Set(MetaTableKey(db.ID, table.ID), data); err != nil {
		return err
	}
	go sm.UpdateDefaultProtocol()
	return nil
}

func getString(m map[string]interface{}, key string) string {
	if v, ok := m[key]; ok {
		if s, ok := v.(string); ok {
			return s
		}
	}
	return ""
}

func getBool(m map[string]interface{}, key string) bool {
	if v, ok := m[key]; ok {
		if b, ok := v.(bool); ok {
			return b
		}
	}
	return false
}
