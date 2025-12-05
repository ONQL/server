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

// CreateDatabase creates a new database.
// It delegates to the underlying StoreManager.
func (db *DB) CreateDatabase(name string) error {
	return db.sm.CreateDatabase(name)
}

// CreateTable creates a new table in the specified database.
// It validates the column definitions (types, formatters, validators) before delegating to the StoreManager.
func (db *DB) CreateTable(dbName string, table storemanager.Table) error {
	// Validate Column Definitions
	for _, col := range table.Columns {
		// Validate Type
		switch col.Type {
		case storemanager.TypeString, storemanager.TypeNumber, storemanager.TypeTimestamp, storemanager.TypeJSON:
			// ok
		default:
			return fmt.Errorf("invalid type %s for column %s", col.Type, col.Name)
		}

		// Validate Formatter/Validator strings syntax
		if col.Formatter != "" {
			col.FormatterRules = strings.Split(col.Formatter, "|")
		}
		if col.Validator != "" {
			col.ValidatorRules = strings.Split(col.Validator, "|")
		}
	}

	return db.sm.CreateTable(dbName, table)
}

// FetchDatabases returns a list of all database names.
// It delegates to the underlying StoreManager.
func (db *DB) FetchDatabases() []string {
	return db.sm.FetchDatabases()
}

// FetchTables returns a list of all table names in a database.
// It delegates to the underlying StoreManager.
func (db *DB) FetchTables(dbName string) ([]string, error) {
	return db.sm.FetchTables(dbName)
}

// RenameDatabase renames an existing database.
// It delegates to the underlying StoreManager.
func (db *DB) RenameDatabase(oldName, newName string) error {
	return db.sm.RenameDatabase(oldName, newName)
}

// DropDatabase deletes a database and all its contents.
// It delegates to the underlying StoreManager.
func (db *DB) DropDatabase(name string) error {
	return db.sm.DropDatabase(name)
}

// RenameTable renames a table within a database.
// It delegates to the underlying StoreManager.
func (db *DB) RenameTable(dbName, oldName, newName string) error {
	return db.sm.RenameTable(dbName, oldName, newName)
}

// DropTable drops a table from a database.
// It delegates to the underlying StoreManager.
func (db *DB) DropTable(dbName, tableName string) error {
	return db.sm.DropTable(dbName, tableName)
}

// AlterTable modifies the structure of an existing table.
// It delegates to the underlying StoreManager.
func (db *DB) AlterTable(dbName, tableName string, changes map[string]interface{}) error {
	return db.sm.AlterTable(dbName, tableName, changes)
}

// GetTableSchema retrieves the schema definition for a table.
// It delegates to the underlying StoreManager.
func (db *DB) GetTableSchema(dbName, tableName string) (*storemanager.Table, error) {
	_, table, err := db.sm.GetTableSchema(dbName, tableName)
	return table, err
}
