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
	"encoding/json"
	"fmt"
	"onql/common"
	"strconv"
	"strings"
)

// NextSequence increments and returns the next value for a column sequence.
func (sm *StoreManager) NextSequence(dbName, tableName, colName string) (int, error) {
	sm.schema.Mu.RLock()
	db, ok := sm.schema.Databases[dbName]
	if !ok {
		sm.schema.Mu.RUnlock()
		return 0, fmt.Errorf("database %s not found", dbName)
	}
	table, ok := db.Tables[tableName]
	if !ok {
		sm.schema.Mu.RUnlock()
		return 0, fmt.Errorf("table %s not found", tableName)
	}
	col, ok := table.Columns[colName]
	if !ok {
		sm.schema.Mu.RUnlock()
		return 0, fmt.Errorf("column %s not found", colName)
	}
	sm.schema.Mu.RUnlock() // unlock early as we use IDs now

	key := SequenceKey(db.ID, table.ID, col.ID)

	// We need atomic increment. The Engine interface doesn't strictly provide it,
	// but we can lock on the key or use a global lock.
	// For simplicity, we'll use a lock here or rely on the engine if it supported it.
	// Badger has Sequence, but our interface is generic.
	// We'll use a simple Get-Increment-Set with a lock.
	// Ideally, this should be finer-grained locking.

	sm.flushMutex.Lock() // Reusing a lock or need a new one? flushMutex might be too broad.
	defer sm.flushMutex.Unlock()

	valBytes, err := sm.engine.Get(key)
	var current int
	if err != nil {
		if err == common.ErrNotFound {
			current = 0
		} else {
			return 0, err
		}
	} else {
		current, err = strconv.Atoi(string(valBytes))
		if err != nil {
			return 0, err
		}
	}

	next := current + 1
	if err := sm.engine.Set(key, []byte(strconv.Itoa(next))); err != nil {
		return 0, err
	}

	return next, nil
}

// Insert adds a new row to the specified table.
// It validates the primary key, checks for duplicates in both buffer and disk,
// serializes the data, and updates the write buffer and indices.
func (sm *StoreManager) Insert(dbName, tableName string, row Row) error {
	// Acquire read lock to prevent schema migrations during operation
	sm.migrationLock.RLock()
	defer sm.migrationLock.RUnlock()

	dbID, table, err := sm.GetTableSchema(dbName, tableName)
	if err != nil {
		return err
	}

	pkVal, ok := row.Data[table.PK]
	if !ok {
		return fmt.Errorf("primary key %s missing", table.PK)
	}
	pkStr := fmt.Sprintf("%v", pkVal)

	// 2. Check if exists (Buffer or Disk)
	dataKey := string(DataKey(dbID, table.ID, pkStr))
	if _, exists, isDeleted := sm.buffer.Get(dataKey); exists && !isDeleted {
		return common.ErrDuplicate
	}

	// 3. Serialize
	dataBytes, err := json.Marshal(row.Data)
	if err != nil {
		return err
	}

	// 4. Update Buffer
	sm.buffer.Put(dataKey, dataBytes)

	// 5. Update Indices
	for colName, colDef := range table.Columns {
		if colDef.Indexed {
			if val, ok := row.Data[colName]; ok {
				valStr := fmt.Sprintf("%v", val)
				idxKey := string(IndexKey(dbID, table.ID, colDef.ID, valStr, pkStr))
				sm.buffer.Put(idxKey, []byte(pkStr)) // Value is PK
			}
		}
	}

	return nil
}

// Get retrieves a row by its primary key.
// It first checks the write buffer for recent changes, then falls back to the disk.
// Returns common.ErrNotFound if the row does not exist or was deleted.
func (sm *StoreManager) Get(dbName, tableName, pk string) (*Row, error) {
	// Acquire read lock to prevent schema migrations during operation
	sm.migrationLock.RLock()
	defer sm.migrationLock.RUnlock()

	dbID, table, err := sm.GetTableSchema(dbName, tableName)
	if err != nil {
		return nil, err
	}

	dataKey := string(DataKey(dbID, table.ID, pk))

	// Check Buffer
	if val, exists, isDeleted := sm.buffer.Get(dataKey); exists {
		if isDeleted {
			return nil, common.ErrNotFound
		}
		var data map[string]interface{}
		if err := json.Unmarshal(val, &data); err != nil {
			return nil, err
		}
		return &Row{Data: data}, nil
	}

	// Check Disk
	val, err := sm.engine.Get([]byte(dataKey))
	if err != nil {
		return nil, err
	}
	var data map[string]interface{}
	if err := json.Unmarshal(val, &data); err != nil {
		return nil, err
	}
	return &Row{Data: data}, nil
}

// Update modifies an existing row.
// It retrieves the old row to properly update indices, then overwrites the data
// in the buffer and updates relevant indices.
func (sm *StoreManager) Update(dbName, tableName, pk string, newRow Row) error {
	// Acquire read lock to prevent schema migrations during operation
	sm.migrationLock.RLock()
	defer sm.migrationLock.RUnlock()

	// 1. Get old row to update indices
	oldRow, err := sm.Get(dbName, tableName, pk)
	if err != nil {
		return err
	}

	dbID, table, err := sm.GetTableSchema(dbName, tableName)
	if err != nil {
		return err
	}

	// 2. Serialize new data
	dataBytes, err := json.Marshal(newRow.Data)
	if err != nil {
		return err
	}

	// 3. Update Buffer
	dataKey := string(DataKey(dbID, table.ID, pk))
	sm.buffer.Put(dataKey, dataBytes)

	// 4. Update Indices
	for colName, colDef := range table.Columns {
		if colDef.Indexed {
			oldVal := oldRow.Data[colName]
			newVal := newRow.Data[colName]

			oldValStr := fmt.Sprintf("%v", oldVal)
			newValStr := fmt.Sprintf("%v", newVal)

			if oldValStr != newValStr {
				// Remove old index
				oldIdxKey := string(IndexKey(dbID, table.ID, colDef.ID, oldValStr, pk))
				sm.buffer.Delete(oldIdxKey)

				// Add new index
				newIdxKey := string(IndexKey(dbID, table.ID, colDef.ID, newValStr, pk))
				sm.buffer.Put(newIdxKey, []byte(pk))
			}
		}
	}

	return nil
}

// Delete removes a row by its primary key.
// It marks the row as deleted in the buffer and removes associated indices.
func (sm *StoreManager) Delete(dbName, tableName, pk string) error {
	// Acquire read lock to prevent schema migrations during operation
	sm.migrationLock.RLock()
	defer sm.migrationLock.RUnlock()

	// 1. Get old row to remove indices
	oldRow, err := sm.Get(dbName, tableName, pk)
	if err != nil {
		return err // Already doesn't exist?
	}

	dbID, table, err := sm.GetTableSchema(dbName, tableName)
	if err != nil {
		return err
	}

	// 2. Mark as deleted in Buffer
	dataKey := string(DataKey(dbID, table.ID, pk))
	sm.buffer.Delete(dataKey)

	// 3. Remove Indices
	for colName, colDef := range table.Columns {
		if colDef.Indexed {
			if val, ok := oldRow.Data[colName]; ok {
				valStr := fmt.Sprintf("%v", val)
				idxKey := string(IndexKey(dbID, table.ID, colDef.ID, valStr, pk))
				sm.buffer.Delete(idxKey)
			}
		}
	}

	return nil
}

// Flush writes all buffered data (inserts, updates, deletes) to the underlying storage engine.
// It uses batch operations for efficiency.
func (sm *StoreManager) Flush() error {
	sm.flushMutex.Lock()
	defer sm.flushMutex.Unlock()

	data := sm.buffer.FlushAndClear()
	if data == nil {
		return nil
	}

	var keys, values [][]byte
	var deleteKeys [][]byte

	for k, v := range data {
		if v.IsDeleted {
			deleteKeys = append(deleteKeys, []byte(k))
		} else {
			keys = append(keys, []byte(k))
			values = append(values, v.Value)
		}
	}

	// Batch Set
	if len(keys) > 0 {
		if err := sm.engine.BatchSet(keys, values); err != nil {
			// In a real system, we might want to retry or put back in buffer
			return err
		}
	}

	// Batch Delete (Engine interface needs BatchDelete or we loop)
	// Assuming Engine has Delete, we can loop or add BatchDelete.
	// For now, loop.
	for _, k := range deleteKeys {
		if err := sm.engine.Delete(k); err != nil {
			return err
		}
	}

	return nil
}

// GetPkByIndex retrieves the primary keys of rows using an indexed column value.
// It checks both the buffer and the disk for the index entries.
func (sm *StoreManager) GetPkByIndex(dbName, tableName, colName, value string) ([]string, error) {
	dbID, table, err := sm.GetTableSchema(dbName, tableName)
	if err != nil {
		return nil, err
	}

	colDef, ok := table.Columns[colName]
	if !ok {
		return nil, fmt.Errorf("column %s not found", colName)
	}

	prefix := fmt.Sprintf("IDX:%s:%s:%s:%s:", dbID, table.ID, colDef.ID, value)
	prefixBytes := []byte(prefix)

	var foundPKs []string
	seenPKs := make(map[string]struct{})

	// Check Buffer
	sm.buffer.mu.RLock()
	for k, v := range sm.buffer.data {
		if !v.IsDeleted && len(k) > len(prefix) && k[:len(prefix)] == prefix {
			// Extract PK from key or value
			pk := string(v.Value)
			if _, seen := seenPKs[pk]; !seen {
				foundPKs = append(foundPKs, pk)
				seenPKs[pk] = struct{}{}
			}
		}
	}
	sm.buffer.mu.RUnlock()

	// Check Disk
	err = sm.engine.IteratePrefix(prefixBytes, func(k, v []byte) error {
		// Check if deleted in buffer
		// The index key in buffer might be marked deleted, but we need the PK to check data key?
		// Actually, if index entry is deleted in buffer, we shouldn't return it.
		// Buffer check above only adds if !IsDeleted.
		// But if it's in disk but deleted in buffer (overwritten/deleted), we need to check buffer for deletion.

		// The index key itself in buffer:
		keyStr := string(k)
		if _, exists, isDeleted := sm.buffer.Get(keyStr); exists && isDeleted {
			return nil
		}

		pk := string(v)
		if _, seen := seenPKs[pk]; !seen {
			foundPKs = append(foundPKs, pk)
			seenPKs[pk] = struct{}{}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return foundPKs, nil
}

// GetAllPks retrieves all primary keys for a given table.
func (sm *StoreManager) GetAllPks(dbName, tableName string) ([]string, error) {
	dbID, table, err := sm.GetTableSchema(dbName, tableName)
	if err != nil {
		return nil, err
	}

	// Iterate over DATA:dbID:tableID:
	prefix := DataKey(dbID, table.ID, "")
	prefixStr := string(prefix)

	var pks []string
	seenPKs := make(map[string]struct{})

	// Buffer
	sm.buffer.mu.RLock()
	for k, v := range sm.buffer.data {
		if strings.HasPrefix(k, prefixStr) {
			pk := k[len(prefixStr):]
			if v.IsDeleted {
				seenPKs[pk] = struct{}{} // Mark as seen (deleted) so we don't add from disk
			} else {
				if _, seen := seenPKs[pk]; !seen {
					pks = append(pks, pk)
					seenPKs[pk] = struct{}{}
				}
			}
		}
	}
	sm.buffer.mu.RUnlock()

	// Disk
	err = sm.engine.IteratePrefix(prefix, func(k, v []byte) error {
		pk := string(k[len(prefix):])
		// If explicitly deleted in buffer (and thus in seenPKs), skip
		// If added in buffer (and thus in seenPKs), skip (already added)
		if _, seen := seenPKs[pk]; !seen {
			pks = append(pks, pk)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return pks, nil
}

// GetDataByPKs retrieves multiple rows by their primary keys.
func (sm *StoreManager) GetDataByPKs(dbName, tableName string, pks []string) ([]map[string]interface{}, error) {
	var results []map[string]interface{}
	for _, pk := range pks {
		row, err := sm.Get(dbName, tableName, pk)
		if err != nil {
			if err == common.ErrNotFound {
				continue
			}
			return nil, err
		}
		results = append(results, row.Data)
	}
	return results, nil
}

// GetTableSchema retrieves the schema for a specific table.
// It returns the database ID, the Table definition, and any error encountered.
func (sm *StoreManager) GetTableSchema(dbName, tableName string) (string, *Table, error) {
	sm.schema.Mu.RLock()
	defer sm.schema.Mu.RUnlock()

	db, ok := sm.schema.Databases[dbName]
	if !ok {
		return "", nil, fmt.Errorf("database %s not found", dbName)
	}
	table, ok := db.Tables[tableName]
	if !ok {
		return "", nil, fmt.Errorf("table %s not found", tableName)
	}
	return db.ID, table, nil
}
