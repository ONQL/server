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

package database

import (
	"fmt"
	"onql/storemanager"
	"strings"
)

// SetProtocol stores a protocol definition.
// It delegates to the underlying StoreManager.
func (db *DB) SetProtocol(password string, protocol storemanager.QueryProtocol) error {
	return db.sm.SetProtocol(password, protocol)
}

// GetProtocol retrieves a protocol by its password.
// It delegates to the underlying StoreManager.
func (db *DB) GetProtocol(password string) (*storemanager.QueryProtocol, error) {
	return db.sm.GetProtocol(password)
}

// GetAllProtocols retrieves a list of all registered protocol passwords.
// It delegates to the underlying StoreManager.
func (db *DB) GetAllProtocols() ([]string, error) {
	return db.sm.GetAllProtocols()
}

// DeleteProtocol removes a protocol definition.
// It delegates to the underlying StoreManager.
func (db *DB) DeleteProtocol(password string) error {
	return db.sm.DeleteProtocol(password)
}

// GetProtoContext retrieves the context query for a specific entity within a protocol.
// It delegates to the underlying StoreManager.
func (db *DB) GetProtoContext(password, entity, contextKey string) (string, error) {
	return db.sm.GetProtoContext(password, entity, contextKey)
}

// ResolveEntityToTable converts an entity name to the actual database and table names.
// It delegates to the underlying StoreManager.
func (db *DB) ResolveEntityToTable(password, entity string) (string, string, error) {
	return db.sm.ResolveEntityToTable(password, entity)
}

// ResolveField converts an alias field name to the actual database column name.
// It delegates to the underlying StoreManager.
func (db *DB) ResolveField(password, entity, aliasField string) (string, error) {
	return db.sm.ResolveField(password, entity, aliasField)
}

// ===== DSL Helper Functions =====
// These functions are used by the DSL evaluator to validate queries against the protocol.
// They rely on a global DB instance being set.

// IsDatabase checks if a database exists in the protocol definition.
func IsDatabase(protoPass, dbName string) bool {
	protocol, err := globalDB.GetProtocol(protoPass)
	if err != nil {
		return false
	}
	_, exists := (*protocol)[dbName]
	return exists
}

// IsTable checks if a table (entity) exists in the protocol definition.
func IsTable(protoPass, dbName, tableName string) bool {
	protocol, err := globalDB.GetProtocol(protoPass)
	if err != nil {
		return false
	}
	module, exists := (*protocol)[dbName]
	if !exists {
		return false
	}
	_, exists = module.Entities[tableName]
	return exists
}

// IsColumn checks if a column (field) exists in an entity definition.
func IsColumn(protoPass, dbName, tableName, columnName string) bool {
	protocol, err := globalDB.GetProtocol(protoPass)
	if err != nil {
		return false
	}
	module, exists := (*protocol)[dbName]
	if !exists {
		return false
	}
	entity, exists := module.Entities[tableName]
	if !exists {
		return false
	}
	_, exists = entity.Fields[columnName]
	return exists
}

// IsRelatedTableByRelationName checks if a relation exists by its name.
func IsRelatedTableByRelationName(protoPass, dbName, tableName, relationName string) bool {
	protocol, err := globalDB.GetProtocol(protoPass)
	if err != nil {
		return false
	}
	module, exists := (*protocol)[dbName]
	if !exists {
		return false
	}
	entity, exists := module.Entities[tableName]
	if !exists {
		return false
	}
	_, exists = entity.Relations[relationName]
	return exists
}

// GetRelationByRelationName retrieves a relation definition by its name.
func GetRelationByRelationName(protoPass, dbName, tableName, relationName string) (*storemanager.Relation, error) {
	protocol, err := globalDB.GetProtocol(protoPass)
	if err != nil {
		return nil, err
	}
	module, exists := (*protocol)[dbName]
	if !exists {
		return nil, fmt.Errorf("database %s not found in protocol", dbName)
	}
	entity, exists := module.Entities[tableName]
	if !exists {
		return nil, fmt.Errorf("entity %s not found", tableName)
	}
	relation, exists := entity.Relations[relationName]
	if !exists {
		return nil, fmt.Errorf("relation %s not found", relationName)
	}
	return relation, nil
}

// GetDbNameFromProtoName returns the actual database name from the protocol.
func GetDbNameFromProtoName(protoPass, dbName string) (string, error) {
	protocol, err := globalDB.GetProtocol(protoPass)
	if err != nil {
		return "", err
	}
	module, exists := (*protocol)[dbName]
	if !exists {
		return "", fmt.Errorf("database %s not found in protocol", dbName)
	}
	return module.Database, nil
}

// GetTableNameFromProtoName returns the actual table name from an entity alias.
func GetTableNameFromProtoName(protoPass, dbName, tableName string) (string, error) {
	protocol, err := globalDB.GetProtocol(protoPass)
	if err != nil {
		return "", err
	}
	module, exists := (*protocol)[dbName]
	if !exists {
		return "", fmt.Errorf("database %s not found in protocol", dbName)
	}
	entity, exists := module.Entities[tableName]
	if !exists {
		return "", fmt.Errorf("entity %s not found", tableName)
	}
	return entity.Table, nil
}

// GetColSchemaFromProtoName returns schema metadata for a specific column alias.
func GetColSchemaFromProtoName(protoPass, dbName, tableName, columnName string) (map[string]string, error) {
	protocol, err := globalDB.GetProtocol(protoPass)
	if err != nil {
		return nil, err
	}
	module, exists := (*protocol)[dbName]
	if !exists {
		return nil, fmt.Errorf("database %s not found in protocol", dbName)
	}
	entity, exists := module.Entities[tableName]
	if !exists {
		return nil, fmt.Errorf("entity %s not found", tableName)
	}
	actualFieldName, exists := entity.Fields[columnName]
	if !exists {
		return nil, fmt.Errorf("field %s not found in entity %s", columnName, tableName)
	}

	// Return metadata about the column
	return map[string]string{
		"name":  actualFieldName,
		"alias": columnName,
		"table": entity.Table,
		"type":  "string", // TODO: Get actual type from schema
	}, nil
}

// SetProtocolBySchema generates a protocol from a schema map and sets it.
// It maintains a default name protocol for every database.
func (db *DB) SetProtocolBySchema(password string, schema map[string]map[string]map[string]map[string]string) error {
	// Convert schema to Query protocol
	protocol := make(storemanager.QueryProtocol)
	for dbName, tables := range schema {
		protocol[dbName] = &storemanager.ProtocolModule{Database: dbName}
		protocol[dbName].Entities = make(map[string]*storemanager.Entity)
		for table, columns := range tables {
			entity := storemanager.Entity{}
			entity.Table = table
			entity.Fields = make(map[string]string)
			for column := range columns {
				entity.Fields[column] = column
			}
			protocol[dbName].Entities[table] = &entity
		}
	}
	return db.SetProtocol(password, protocol)
}

// SetProtocolBySchema generates a protocol from a schema map and sets it using the global DB.
func SetProtocolBySchema(password string, schema map[string]map[string]map[string]map[string]string) error {
	if globalDB == nil {
		return fmt.Errorf("global DB not initialized")
	}
	return globalDB.SetProtocolBySchema(password, schema)
}

// Global database instance for DSL functions
var globalDB *DB

// SetGlobalDB sets the global database instance for DSL helper functions.
// This is called during database initialization.
func SetGlobalDB(db *DB) {
	globalDB = db
}

// GetProtoContext retrieves the context query for a specific entity within a protocol using the global DB.
func GetProtoContext(password, entity, contextKey string) (string, error) {
	if globalDB == nil {
		return "", fmt.Errorf("global DB not initialized")
	}
	return globalDB.GetProtoContext(password, entity, contextKey)
}

// GetAllPks retrieves all primary keys for a given table using the global DB.
func GetAllPks(dbName, tableName string) ([]string, error) {
	if globalDB == nil {
		return nil, fmt.Errorf("global DB not initialized")
	}
	return globalDB.GetAllPks(dbName, tableName)
}

// GetDataByPKs retrieves multiple rows by their primary keys using the global DB.
func GetDataByPKs(dbName, tableName string, pks []string) ([]map[string]interface{}, error) {
	if globalDB == nil {
		return nil, fmt.Errorf("global DB not initialized")
	}
	return globalDB.GetDataByPKs(dbName, tableName, pks)
}

// GetPkByIndex retrieves the primary keys of rows using an indexed column value using the global DB.
func GetPkByIndex(dbName, tableName, colName, value string) ([]string, error) {
	if globalDB == nil {
		return nil, fmt.Errorf("global DB not initialized")
	}
	return globalDB.GetPkByIndex(dbName, tableName, colName, value)
}

// GetWithPKs is an alias for GetDataByPKs to match DSL expectations.
func GetWithPKs(dbName, tableName string, pks []string) ([]map[string]interface{}, error) {
	return GetDataByPKs(dbName, tableName, pks)
}

// GetPksFromIndex is an alias for GetPkByIndex to match DSL expectations.
func GetPksFromIndex(dbName, tableName, indexKey string) ([]string, error) {
	// indexKey is expected to be "col:val"
	parts := strings.SplitN(indexKey, ":", 2)
	if len(parts) != 2 {
		return nil, fmt.Errorf("invalid index key format: %s", indexKey)
	}
	return GetPkByIndex(dbName, tableName, parts[0], parts[1])
}
