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
	"onql/logger"
	"time"
)

// New creates a new StoreManager instance.
// It initializes the schema, buffer, and starts the background flush routine.
// It also loads the existing schema and protocols from the engine.
func New(eng Engine, cfg *config.Config) *StoreManager {
	sm := &StoreManager{
		engine: eng,
		schema: &Schema{
			Databases: make(map[string]*Database),
			Protocols: make(map[string]*QueryProtocol), // Initialize protocol cache
		},
		buffer: NewBuffer(),
		config: cfg,
		done:   make(chan struct{}),
	}

	// Load schema and protocols from disk
	if err := sm.LoadSchema(); err != nil {
		logger.Error("Failed to load schema: %v", err)
	}
	if err := sm.LoadProtocols(); err != nil {
		logger.Error("Failed to load protocols: %v", err)
	}

	// Start background flush
	sm.wg.Add(1)
	go sm.autoFlush()

	return sm
}

// Close gracefully shuts down the StoreManager.
// It stops the background flusher and waits for any pending operations to complete.
func (sm *StoreManager) Close() {
	close(sm.done)
	sm.wg.Wait()
}

// GetEngine returns the underlying storage engine.
// This allows direct access to the engine for advanced operations or testing.
func (sm *StoreManager) GetEngine() Engine {
	return sm.engine
}

// autoFlush runs a background loop that periodically flushes the write buffer to disk.
// It runs at the interval specified in the configuration.
// It also performs a final flush when the StoreManager is closed.
func (sm *StoreManager) autoFlush() {
	defer sm.wg.Done()
	ticker := time.NewTicker(sm.config.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			if err := sm.Flush(); err != nil {
				logger.Error("Flush failed: %v", err)
			}
		case <-sm.done:
			logger.Info("Flusher stopping, final flush...")
			if err := sm.Flush(); err != nil {
				logger.Error("Final flush failed: %v", err)
			}
			return
		}
	}
}
