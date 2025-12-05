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
	"strings"
)

// SetProtocol stores a protocol definition.
// It validates the protocol against the existing schema before storing it.
// The protocol is cached in memory and persisted to disk.
func (sm *StoreManager) SetProtocol(password string, protocol QueryProtocol) error {
	sm.schema.Mu.Lock()
	defer sm.schema.Mu.Unlock()

	// Validate protocol against schema
	if err := sm.validateProtocol(&protocol); err != nil {
		return fmt.Errorf("protocol validation failed: %v", err)
	}

	// Store in memory cache
	sm.schema.Protocols[password] = &protocol

	// Persist to disk
	key := ProtocolKey(password)
	data, err := json.Marshal(protocol)
	if err != nil {
		return err
	}

	return sm.engine.Set(key, data)
}

// GetProtocol retrieves a protocol by its password (identifier).
// It checks the in-memory cache first.
func (sm *StoreManager) GetProtocol(password string) (*QueryProtocol, error) {
	sm.schema.Mu.RLock()
	defer sm.schema.Mu.RUnlock()

	// Check cache first
	if proto, exists := sm.schema.Protocols[password]; exists {
		return proto, nil
	}

	return nil, common.ErrNotFound
}

// GetAllProtocols returns a list of all registered protocol passwords.
func (sm *StoreManager) GetAllProtocols() ([]string, error) {
	sm.schema.Mu.RLock()
	defer sm.schema.Mu.RUnlock()

	passwords := make([]string, 0, len(sm.schema.Protocols))
	for password := range sm.schema.Protocols {
		passwords = append(passwords, password)
	}

	return passwords, nil
}

// DeleteProtocol removes a protocol definition from both cache and disk.
func (sm *StoreManager) DeleteProtocol(password string) error {
	sm.schema.Mu.Lock()
	defer sm.schema.Mu.Unlock()

	// Remove from cache
	delete(sm.schema.Protocols, password)

	// Remove from disk
	key := ProtocolKey(password)
	return sm.engine.Delete(key)
}

// LoadProtocols loads all protocols from disk into the in-memory cache.
// This is typically called on startup.
func (sm *StoreManager) LoadProtocols() error {
	prefix := []byte("PROTO:")

	return sm.engine.IteratePrefix(prefix, func(k, v []byte) error {
		// Extract password from key (PROTO:password)
		parts := strings.Split(string(k), ":")
		if len(parts) < 2 {
			return nil
		}
		password := parts[1]

		var protocol QueryProtocol
		if err := json.Unmarshal(v, &protocol); err != nil {
			return err
		}

		sm.schema.Protocols[password] = &protocol
		return nil
	})
}

// validateProtocol checks if a protocol references valid database and tables.
// It ensures that all entities, fields, and relations defined in the protocol
// actually exist in the underlying database schema.
func (sm *StoreManager) validateProtocol(protocol *QueryProtocol) error {
	for _, module := range *protocol {
		// Check if database exists
		db, exists := sm.schema.Databases[module.Database]
		if !exists {
			return fmt.Errorf("database '%s' does not exist", module.Database)
		}

		// Validate each entity
		for entityName, entity := range module.Entities {
			// Check if table exists
			table, exists := db.Tables[entity.Table]
			if !exists {
				return fmt.Errorf("entity '%s': table '%s' does not exist", entityName, entity.Table)
			}

			// Validate fields
			for alias, actualField := range entity.Fields {
				if _, exists := table.Columns[actualField]; !exists {
					return fmt.Errorf("entity '%s': field '%s' (alias '%s') does not exist in table '%s'",
						entityName, actualField, alias, entity.Table)
				}
			}

			// Validate relations
			for relName, relation := range entity.Relations {
				// Check if related entity exists in protocol (in the same module/DB?)
				// Assuming relations are within the same protocol context (potentially cross-DB if we search all?)
				// For now, let's assume relations are within the same module or we search the whole protocol.
				// Let's search the whole protocol.
				found := false
				for _, m := range *protocol {
					if _, exists := m.Entities[relation.Entity]; exists {
						found = true
						break
					}
				}
				if !found {
					return fmt.Errorf("entity '%s': relation '%s' references non-existent entity '%s'",
						entityName, relName, relation.Entity)
				}

				// Validate FK fields exist
				fkParts := strings.Split(relation.FKField, ":")
				if len(fkParts) < 2 {
					return fmt.Errorf("entity '%s': relation '%s' has invalid FK field format",
						entityName, relName)
				}

				// For MTM, validate through table
				if relation.Type == "mtm" && relation.Through != "" {
					// Through table should be in the same DB? Or specified?
					// Assuming same DB for now.
					if _, exists := db.Tables[relation.Through]; !exists {
						return fmt.Errorf("entity '%s': relation '%s' through table '%s' does not exist",
							entityName, relName, relation.Through)
					}
				}
			}
		}
	}

	return nil
}

// GetProtoContext retrieves a context query for a specific entity within a protocol.
// Context queries are used to apply default filters or conditions.
func (sm *StoreManager) GetProtoContext(password, entityAlias, contextKey string) (string, error) {
	protocol, err := sm.GetProtocol(password)
	if err != nil {
		return "", err
	}

	// Search all modules for the entity
	for _, module := range *protocol {
		if entity, exists := module.Entities[entityAlias]; exists {
			if entity.Context != nil {
				if query, exists := entity.Context[contextKey]; exists {
					return query, nil
				}
			}
			return "", nil // Entity found, but no context key
		}
	}

	return "", fmt.Errorf("entity '%s' not found", entityAlias)
}

// ResolveEntityToTable converts an entity alias to the actual database and table names.
func (sm *StoreManager) ResolveEntityToTable(password, entityAlias string) (string, string, error) {
	protocol, err := sm.GetProtocol(password)
	if err != nil {
		return "", "", err
	}

	for _, module := range *protocol {
		if entity, exists := module.Entities[entityAlias]; exists {
			return module.Database, entity.Table, nil
		}
	}

	return "", "", fmt.Errorf("entity '%s' not found in protocol", entityAlias)
}

// ResolveField converts an alias field name to the actual database column name.
func (sm *StoreManager) ResolveField(password, entityAlias, aliasField string) (string, error) {
	protocol, err := sm.GetProtocol(password)
	if err != nil {
		return "", err
	}

	for _, module := range *protocol {
		if entity, exists := module.Entities[entityAlias]; exists {
			actualField, exists := entity.Fields[aliasField]
			if !exists {
				return "", fmt.Errorf("field '%s' not found in entity '%s'", aliasField, entityAlias)
			}
			return actualField, nil
		}
	}

	return "", fmt.Errorf("entity '%s' not found", entityAlias)
}

// ProtocolKey generates the storage key for a protocol.
// Format: PROTO:<password>
func ProtocolKey(password string) []byte {
	return []byte(fmt.Sprintf("PROTO:%s", password))
}

// UpdateDefaultProtocol regenerates the "default" protocol from the current schema
// and persists it. This should be called after any schema change.
func (sm *StoreManager) UpdateDefaultProtocol() error {
	sm.schema.Mu.RLock()
	// We need to construct the protocol while holding the read lock on schema
	protocol := make(QueryProtocol)
	for dbName, db := range sm.schema.Databases {
		module := &ProtocolModule{
			Database: dbName,
			Entities: make(map[string]*Entity),
		}
		for tableName, table := range db.Tables {
			entity := &Entity{
				Table:  tableName,
				Fields: make(map[string]string),
			}
			for colName := range table.Columns {
				entity.Fields[colName] = colName
			}
			module.Entities[tableName] = entity
		}
		protocol[dbName] = module
	}
	sm.schema.Mu.RUnlock()

	// Now set the protocol (this will acquire Write lock on schema)
	return sm.SetProtocol("default", protocol)
}
