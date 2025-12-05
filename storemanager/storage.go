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
	"fmt"
	"strings"
)

// Key generation helpers
// These functions define the key structure used in the underlying key-value store.
// Consistent key generation is crucial for data retrieval and integrity.

// MetaDBKey generates the key for storing database metadata.
// Format: META:DB:<dbID>
func MetaDBKey(dbID string) []byte {
	return []byte(fmt.Sprintf("META:DB:%s", dbID))
}

// MetaTableKey generates the key for storing table metadata.
// Format: META:TBL:<dbID>:<tableID>
func MetaTableKey(dbID, tableID string) []byte {
	return []byte(fmt.Sprintf("META:TBL:%s:%s", dbID, tableID))
}

// MapDBKey generates the key for mapping a database name to its ID.
// Format: MAP:DB:<dbName>
func MapDBKey(dbName string) []byte {
	return []byte(fmt.Sprintf("MAP:DB:%s", dbName))
}

// MapTableKey generates the key for mapping a table name to its ID within a database.
// Format: MAP:TBL:<dbID>:<tableName>
func MapTableKey(dbID, tableName string) []byte {
	return []byte(fmt.Sprintf("MAP:TBL:%s:%s", dbID, tableName))
}

// DataKey generates the key for storing a row of data.
// Format: DATA:<dbID>:<tableID>:<pk>
func DataKey(dbID, tableID, pk string) []byte {
	return []byte(fmt.Sprintf("DATA:%s:%s:%s", dbID, tableID, pk))
}

// IndexKey generates the key for an index entry.
// Format: IDX:<dbID>:<tableID>:<colID>:<value>:<pk>
func IndexKey(dbID, tableID, colID, value, pk string) []byte {
	// Value might contain colons, so we might need to escape or just use it as is if we are careful.
	// For simplicity, assuming value doesn't break the structure or we use a separator that is rare.
	// A better approach is to use a binary format or length-prefixed, but string is easier for debugging.
	return []byte(fmt.Sprintf("IDX:%s:%s:%s:%s:%s", dbID, tableID, colID, value, pk))
}

// ParseIndexKey extracts info from an index key.
// It returns the components of the key: dbID, tableID, colID, value, and pk.
func ParseIndexKey(key []byte) (dbID, tableID, colID, val, pk string) {
	parts := strings.Split(string(key), ":")
	if len(parts) < 6 {
		return
	}
	// IDX:dbID:tableID:colID:val:pk
	return parts[1], parts[2], parts[3], parts[4], parts[5]
}
