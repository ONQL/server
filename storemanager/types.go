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
	"onql/config"
	"sync"
)

// DataType represents the type of a column in the database.
// It is used for type validation and storage.
type DataType string

const (
	TypeString    DataType = "string"
	TypeNumber    DataType = "number"
	TypeTimestamp DataType = "timestamp"
	TypeJSON      DataType = "json"
)

// Schema represents the entire database schema and loaded protocols.
// It holds in-memory representations of all databases and their tables.
type Schema struct {
	Databases map[string]*Database
	Protocols map[string]*QueryProtocol // In-memory protocol cache
	Mu        sync.RWMutex
}

// Database represents a logical grouping of tables.
// It has a unique ID and a Name.
type Database struct {
	ID     string
	Name   string
	Tables map[string]*Table
}

// Table represents a collection of rows and columns.
// It has a unique ID, a Name, and a Primary Key definition.
type Table struct {
	ID      string
	Name    string
	Columns map[string]*Column
	PK      string // Primary Key column name
}

// Column represents a single field in a table.
// It defines the data type, validation rules, and formatting rules.
type Column struct {
	ID           string
	Name         string
	Type         DataType
	Formatter    string // e.g., "trim|decimal:2"
	Validator    string // e.g., "required|min:5"
	DefaultValue interface{}
	Indexed      bool

	// Parsed rules (internal use)
	FormatterRules []string `json:"-"`
	ValidatorRules []string `json:"-"`
}

// Row represents a single record in a table.
// It stores data as a map of column names to values.
type Row struct {
	Data map[string]interface{}
}

// ===== Protocol Types =====

// QueryProtocol defines the mapping between database aliases and entities.
// It is a map of database names to ProtocolModules.
type QueryProtocol map[string]*ProtocolModule

// ProtocolModule represents a single database configuration within a protocol.
type ProtocolModule struct {
	Database string             `json:"database"`
	Entities map[string]*Entity `json:"entities"`
}

// Entity represents a table or logical entity within a protocol.
// It maps DSL fields to database columns and defines relationships.
type Entity struct {
	Table     string               `json:"table"`
	Fields    map[string]string    `json:"fields"`
	Relations map[string]*Relation `json:"relations,omitempty"`
	Context   map[string]string    `json:"context,omitempty"`
}

// Relation defines a relationship between two entities.
// It supports One-to-One, One-to-Many, Many-to-One, and Many-to-Many.
type Relation struct {
	ProtoTable string `json:"prototable"`
	Type       string `json:"type"` // oto, otm, mto, mtm
	Entity     string `json:"entity"`
	FKField    string `json:"fkfield"`
	Through    string `json:"through,omitempty"` // For mtm
}

// ===== StoreManager =====

// StoreManager is the central component for managing data storage and retrieval.
// It coordinates between the high-level schema/operations and the low-level storage engine.
type StoreManager struct {
	engine        Engine
	schema        *Schema
	buffer        *Buffer
	flushMutex    sync.Mutex
	migrationLock sync.RWMutex // Prevents data operations during schema migrations
	config        *config.Config
	done          chan struct{}
	wg            sync.WaitGroup
}

// Engine interface abstracts the underlying key-value storage.
// This allows swapping the storage backend (e.g., BadgerDB, RocksDB, or Mock) without changing StoreManager.
type Engine interface {
	Set(key, value []byte) error
	Get(key []byte) ([]byte, error)
	Delete(key []byte) error
	BatchSet(keys, values [][]byte) error
	IteratePrefix(prefix []byte, fn func(k, v []byte) error) error
}
