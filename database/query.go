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
	"strconv"

	"github.com/google/uuid"
)

// Insert adds a new row to a table.
// It performs the following steps:
// 1. Retrieves the table schema.
// 2. Validates and formats the input data against the table columns.
// 3. Constructs a Row object and delegates the insertion to the StoreManager.
// Returns the primary key value of the inserted row and any error encountered.
func (db *DB) Insert(dbName, tableName string, data map[string]interface{}) (string, error) {
	// 1. Get Schema
	_, table, err := db.sm.GetTableSchema(dbName, tableName)
	if err != nil {
		return "", err
	}

	// 2. Validate and Format
	processedData := make(map[string]interface{})

	for colName, colDef := range table.Columns {
		val, exists := data[colName]

		// Apply Default Value if missing
		if !exists && colDef.DefaultValue != nil {
			if defStr, ok := colDef.DefaultValue.(string); ok {
				if defStr == "$AUTO" {
					// Generate Sequence
					seqVal, err := db.sm.NextSequence(dbName, tableName, colName)
					if err != nil {
						return "", fmt.Errorf("failed to generate sequence for %s: %v", colName, err)
					}
					val = seqVal
					exists = true
				} else if defStr == "$UUID" {
					// Generate UUID
					val = uuid.New().String()
					exists = true
				} else if defStr == "$EMPTY" {
					// Set to empty string
					val = ""
					exists = true
				} else {
					val = colDef.DefaultValue
					exists = true
				}
			} else {
				val = colDef.DefaultValue
				exists = true
			}
		}

		// Automatic Type Conversion (String -> Number/Timestamp)
		if exists {
			if strVal, ok := val.(string); ok {
				switch colDef.Type {
				case storemanager.TypeNumber, storemanager.TypeTimestamp:
					// Try parsing as float64
					if f, err := strconv.ParseFloat(strVal, 64); err == nil {
						val = f
					}
				}
			}
		}

		// Apply Validator
		if len(colDef.ValidatorRules) > 0 {
			// If not exists, pass nil/empty to validator?
			// Validator handles "required".
			if !exists {
				if err := Validate(nil, colDef.ValidatorRules); err != nil {
					return "", fmt.Errorf("column %s: %v", colName, err)
				}
			} else {
				// Runtime check: if default was applied AND it was $EMPTY (resulting in empty string),
				// AND the column is supposed to be required, we should allow it.
				// However, standard logic says if required, "" is invalid.
				// If we want $EMPTY to mean "allow empty string even if required",
				// we must strip "required" from rules for this validation call.

				rules := colDef.ValidatorRules
				if strVal, ok := val.(string); ok && strVal == "" {
					// Check if this came from $EMPTY default
					isDefaultEmpty := false
					if defStr, ok := colDef.DefaultValue.(string); ok && defStr == "$EMPTY" {
						isDefaultEmpty = true
					}

					if isDefaultEmpty {
						// Filter out "required"
						newRules := []string{}
						for _, r := range rules {
							if r != "required" {
								newRules = append(newRules, r)
							}
						}
						rules = newRules
					}
				}

				if err := Validate(val, rules); err != nil {
					return "", fmt.Errorf("column %s: %v", colName, err)
				}
				// Type check
				if err := ValidateType(val, string(colDef.Type)); err != nil {
					return "", fmt.Errorf("column %s: %v", colName, err)
				}
			}
		}

		// Apply Formatter
		if exists {
			if len(colDef.FormatterRules) > 0 {
				formattedVal, err := Format(val, colDef.FormatterRules)
				if err != nil {
					return "", fmt.Errorf("column %s format error: %v", colName, err)
				}
				val = formattedVal
			}
			processedData[colName] = val
		}
	}

	// 3. Insert
	row := storemanager.Row{Data: processedData}
	err = db.sm.Insert(dbName, tableName, row)
	if err != nil {
		return "", err
	}

	// 4. Return the primary key value
	pkVal, ok := processedData[table.PK]
	if !ok {
		return "", fmt.Errorf("primary key %s not found in processed data", table.PK)
	}
	return fmt.Sprintf("%v", pkVal), nil
}

// Update modifies an existing row in a table.
// It performs the following steps:
// 1. Retrieves the table schema.
// 2. Validates and formats the input data (partial updates allowed).
// 3. Fetches the existing row.
// 4. Merges the new data with the existing row.
// 5. Delegates the update to the StoreManager.
func (db *DB) Update(dbName, tableName, pk string, data map[string]interface{}) error {
	// 1. Get Schema
	_, table, err := db.sm.GetTableSchema(dbName, tableName)
	if err != nil {
		return err
	}

	// 2. Validate and Format
	// For update, we might only have partial data.
	// But validators might enforce "required".
	// Usually Update validates only what's present, unless it's a full replace.
	// Let's assume partial update is allowed, but we still validate present fields.

	processedData := make(map[string]interface{})

	// We might need to fetch old row to merge if we want to validate "required" on the final state?
	// Or just validate the fields being updated.
	// Let's validate fields being updated.

	for key, val := range data {
		colDef, ok := table.Columns[key]
		if !ok {
			// Unknown column, ignore or error?
			continue
		}

		// Automatic Type Conversion (String -> Number/Timestamp)
		if strVal, ok := val.(string); ok {
			switch colDef.Type {
			case storemanager.TypeNumber, storemanager.TypeTimestamp:
				// Try parsing as float64
				if f, err := strconv.ParseFloat(strVal, 64); err == nil {
					val = f
				}
			}
		}

		if len(colDef.ValidatorRules) > 0 {
			if err := Validate(val, colDef.ValidatorRules); err != nil {
				return fmt.Errorf("column %s: %v", key, err)
			}
			if err := ValidateType(val, string(colDef.Type)); err != nil {
				return fmt.Errorf("column %s: %v", key, err)
			}
		}

		if len(colDef.FormatterRules) > 0 {
			formattedVal, err := Format(val, colDef.FormatterRules)
			if err != nil {
				return fmt.Errorf("column %s format error: %v", key, err)
			}
			val = formattedVal
		}
		processedData[key] = val
	}

	// Merge with existing data?
	// StoreManager.Update replaces the row.
	// So we MUST fetch the old row, merge, and then save.
	// StoreManager.Update implementation:
	// "Get old row... Serialize new data... Update Buffer"
	// It seems my StoreManager.Update REPLACES the content.
	// So I need to fetch, merge, then call sm.Update.

	oldRow, err := db.sm.Get(dbName, tableName, pk)
	if err != nil {
		return err
	}

	for k, v := range processedData {
		oldRow.Data[k] = v
	}

	return db.sm.Update(dbName, tableName, pk, *oldRow)
}

// Delete removes a row from a table by its primary key.
// It delegates to the underlying StoreManager.
func (db *DB) Delete(dbName, tableName, pk string) error {
	return db.sm.Delete(dbName, tableName, pk)
}

// Get retrieves a single row from a table by its primary key.
// It delegates to the underlying StoreManager and returns the data map.
func (db *DB) Get(dbName, tableName, pk string) (map[string]interface{}, error) {
	row, err := db.sm.Get(dbName, tableName, pk)
	if err != nil {
		return nil, err
	}
	return row.Data, nil
}

// GetPkByIndex retrieves the primary keys of rows using an indexed column value.
// It delegates to the underlying StoreManager.
func (db *DB) GetPkByIndex(dbName, tableName, colName, value string) ([]string, error) {
	return db.sm.GetPkByIndex(dbName, tableName, colName, value)
}

// GetAllPks retrieves all primary keys for a given table.
// It delegates to the underlying StoreManager.
func (db *DB) GetAllPks(dbName, tableName string) ([]string, error) {
	return db.sm.GetAllPks(dbName, tableName)
}

// GetDataByPKs retrieves multiple rows by their primary keys.
// It delegates to the underlying StoreManager.
func (db *DB) GetDataByPKs(dbName, tableName string, pks []string) ([]map[string]interface{}, error) {
	return db.sm.GetDataByPKs(dbName, tableName, pks)
}
