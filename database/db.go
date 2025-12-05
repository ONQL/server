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
	"onql/config"
	"onql/engine"
	"onql/logger"
	"onql/storemanager"
)

// DB represents the high-level database instance.
// It wraps the StoreManager and the underlying storage Engine.
type DB struct {
	sm     *storemanager.StoreManager
	engine *engine.DB
}

// New creates and initializes a new DB instance.
// It initializes the logger, the storage engine, and the store manager.
// It also sets the global DB instance for DSL helper functions.
func New(cfg *config.Config) (*DB, error) {
	logger.Init(cfg.LogLevel)

	eng, err := engine.New(cfg.DBPath)
	if err != nil {
		return nil, err
	}
	sm := storemanager.New(eng, cfg)
	db := &DB{sm: sm, engine: eng}

	// Set global DB for DSL helper functions
	SetGlobalDB(db)

	return db, nil
}

// Close shuts down the database.
// It closes the store manager (flushing any buffers) and the underlying storage engine.
func (db *DB) Close() {
	db.sm.Close()
	db.engine.Close()
}
