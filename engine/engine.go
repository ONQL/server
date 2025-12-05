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

package engine

import (
	"time"

	badger "github.com/dgraph-io/badger/v3"
)

// DB represents a wrapper around the BadgerDB instance.
// It provides methods for basic key-value operations and batch processing.
type DB struct {
	badgerDB *badger.DB
}

// New initializes and returns a new DB instance.
// It opens a BadgerDB at the specified path with default options,
// but disables the default logger for cleaner output.
func New(path string) (*DB, error) {
	opts := badger.DefaultOptions(path)
	opts.Logger = nil // Disable default logger for cleaner output
	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	return &DB{badgerDB: db}, nil
}

// Close closes the underlying BadgerDB instance.
// It should be called when the DB is no longer needed to ensure data integrity.
func (db *DB) Close() error {
	return db.badgerDB.Close()
}

// Set stores a key-value pair in the database.
// It uses a read-write transaction to perform the update.
func (db *DB) Set(key, value []byte) error {
	return db.badgerDB.Update(func(txn *badger.Txn) error {
		return txn.Set(key, value)
	})
}

// Get retrieves the value associated with the given key.
// It uses a read-only transaction. Returns an error if the key is not found.
func (db *DB) Get(key []byte) ([]byte, error) {
	var valCopy []byte
	err := db.badgerDB.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		valCopy, err = item.ValueCopy(nil)
		return err
	})
	return valCopy, err
}

// Delete removes the key-value pair associated with the given key.
// It uses a read-write transaction.
func (db *DB) Delete(key []byte) error {
	return db.badgerDB.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})
}

// BatchSet efficiently sets multiple key-value pairs in a single transaction.
// This is preferred over calling Set multiple times for bulk updates.
func (db *DB) BatchSet(keys, values [][]byte) error {
	return db.badgerDB.Update(func(txn *badger.Txn) error {
		for i, key := range keys {
			if err := txn.Set(key, values[i]); err != nil {
				return err
			}
		}
		return nil
	})
}

// IteratePrefix iterates over all keys with a specific prefix.
// The provided function fn is called for each matching key-value pair.
// Iteration stops if fn returns an error.
func (db *DB) IteratePrefix(prefix []byte, fn func(k, v []byte) error) error {
	return db.badgerDB.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()
		for it.Seek(prefix); it.ValidForPrefix(prefix); it.Next() {
			item := it.Item()
			k := item.Key()
			err := item.Value(func(v []byte) error {
				return fn(k, v)
			})
			if err != nil {
				return err
			}
		}
		return nil
	})
}

// RunGC runs value log garbage collection periodically.
// It runs every 5 minutes and attempts to reclaim space if the value log
// has at least 0.7 discard ratio.
func (db *DB) RunGC() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
	again:
		err := db.badgerDB.RunValueLogGC(0.7)
		if err == nil {
			goto again
		}
	}
}
