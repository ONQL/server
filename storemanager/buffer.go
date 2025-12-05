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
	"sync"
)

// BufferEntry represents a single operation (write or delete) in the write buffer.
type BufferEntry struct {
	Value     []byte
	IsDeleted bool
}

// Buffer manages in-memory data before flushing to the underlying storage engine.
// It acts as a write-back cache to improve write performance.
type Buffer struct {
	data map[string]BufferEntry
	mu   sync.RWMutex
}

// NewBuffer creates and initializes a new Buffer instance.
func NewBuffer() *Buffer {
	return &Buffer{
		data: make(map[string]BufferEntry),
	}
}

// Put adds or updates a value in the buffer.
// It marks the entry as not deleted.
func (b *Buffer) Put(key string, value []byte) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.data[key] = BufferEntry{Value: value, IsDeleted: false}
}

// Delete marks a key for deletion in the buffer.
// The actual deletion from storage happens during flush.
func (b *Buffer) Delete(key string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.data[key] = BufferEntry{IsDeleted: true}
}

// Get retrieves a value from the buffer if it exists.
// It returns the value, a boolean indicating if it was found in the buffer,
// and a boolean indicating if it was marked as deleted in the buffer.
func (b *Buffer) Get(key string) ([]byte, bool, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	entry, exists := b.data[key]
	if !exists {
		return nil, false, false
	}
	if entry.IsDeleted {
		return nil, true, true // Found but deleted
	}
	return entry.Value, true, false // Found and valid
}

// FlushAndClear returns the current buffered data and resets the buffer.
// This operation is thread-safe and atomic with respect to other buffer operations.
func (b *Buffer) FlushAndClear() map[string]BufferEntry {
	b.mu.Lock()
	defer b.mu.Unlock()

	if len(b.data) == 0 {
		return nil
	}

	oldData := b.data
	b.data = make(map[string]BufferEntry)
	return oldData
}
