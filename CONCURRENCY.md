# Concurrency Safety & Locking Strategy

## Overview

The RDBMS uses a multi-level locking strategy to prevent data corruption and ensure consistency during concurrent operations.

## Lock Hierarchy

### 1. Schema Lock (`schema.Mu` - RWMutex)
**Purpose**: Protects schema metadata (databases, tables, columns)

**Read Lock** (allows concurrent reads):
- `GetTableSchema()`
- `FetchDatabases()`
- `FetchTables()`

**Write Lock** (exclusive):
- `CreateDatabase()`
- `CreateTable()`
- `AlterTable()`
- `DropDatabase()`
- `DropTable()`

### 2. Migration Lock (`migrationLock` - RWMutex)
**Purpose**: Prevents data operations during schema migrations

**Read Lock** (data operations):
- `Insert()` - Acquires RLock
- `Get()` - Acquires RLock
- `Update()` - Acquires RLock
- `Delete()` - Acquires RLock

**Write Lock** (schema migrations):
- `RenameDatabase()` - Acquires Lock (blocks ALL operations)
- `RenameTable()` - Acquires Lock (blocks ALL operations)
- `AlterTable()` with `renameColumn` - Acquires Lock

### 3. Flush Lock (`flushMutex` - Mutex)
**Purpose**: Ensures only one flush operation at a time

**Lock**:
- `Flush()` - Prevents concurrent flushes

### 4. Buffer Lock (`buffer.mu` - RWMutex)
**Purpose**: Protects in-memory buffer data structure

**Read Lock**:
- `Get()` - Reading buffer entries
- `GetPkByIndex()` - Iterating buffer

**Write Lock**:
- `Put()` - Adding/updating entries
- `Delete()` - Marking entries as deleted
- `FlushAndClear()` - Clearing buffer

## Concurrency Scenarios

### Scenario 1: Normal Operations (No Migration)
```
Thread 1: Insert() → RLock(migrationLock) → ... → RUnlock
Thread 2: Get() → RLock(migrationLock) → ... → RUnlock
Thread 3: Update() → RLock(migrationLock) → ... → RUnlock
```
✅ **All operations proceed concurrently**

### Scenario 2: During Database Rename
```
Thread 1: RenameDatabase() → Lock(migrationLock) → migrating...
Thread 2: Insert() → RLock(migrationLock) → BLOCKED
Thread 3: Get() → RLock(migrationLock) → BLOCKED
```
✅ **All data operations blocked until migration completes**

### Scenario 3: During AlterTable (Add Column)
```
Thread 1: AlterTable(addColumn) → Lock(schema.Mu) → ...
Thread 2: Insert() → RLock(migrationLock) → Lock(schema.Mu for GetTableSchema) → BLOCKED
```
✅ **Insert waits for schema update, then proceeds**

### Scenario 4: During AlterTable (Rename Column)
```
Thread 1: AlterTable(renameColumn) → Lock(migrationLock) → ...
Thread 2: Insert() → RLock(migrationLock) → BLOCKED
```
✅ **Insert blocked during column rename (prevents using old column name)**

## Why This Prevents Corruption

### Problem Without Migration Lock
```
Thread 1: RenameTable("users", "accounts") 
  → Migrating DATA:db:users:123 → DATA:db:accounts:123
Thread 2: Insert("users", row) 
  → Writes DATA:db:users:456 (to old table name!)
```
❌ **Result**: Data written to old table name during migration = LOST DATA

### Solution With Migration Lock
```
Thread 1: RenameTable("users", "accounts")
  → Lock(migrationLock) ← EXCLUSIVE ACCESS
  → Migrates all keys atomically
  → Unlock(migrationLock)
Thread 2: Insert("users", row)
  → RLock(migrationLock) ← WAITS until migration done
  → GetTableSchema() ← Gets NEW schema (table doesn't exist)
  → Returns error
```
✅ **Result**: No data corruption, operations fail safely

## Performance Considerations

### Fast Operations (No Migration Lock)
- `CreateDatabase()` - Only schema lock
- `CreateTable()` - Only schema lock
- `DropDatabase()` - Only schema lock
- `DropTable()` - Only schema lock
- `AlterTable(addColumn)` - Only schema lock
- `AlterTable(modifyColumn)` - Only schema lock
- `AlterTable(dropColumn)` - Only schema lock

### Slow Operations (Migration Lock - BLOCKS EVERYTHING)
- `RenameDatabase()` - Rewrites ALL keys
- `RenameTable()` - Rewrites table keys
- `AlterTable(renameColumn)` - Blocks operations

**Recommendation**: Perform rename operations during maintenance windows.

## Lock Ordering (Prevents Deadlocks)

Always acquire locks in this order:
1. `migrationLock` (if needed)
2. `schema.Mu`
3. `buffer.mu`
4. `flushMutex`

Never acquire in reverse order to prevent deadlocks.

## Best Practices

1. **Use Read Operations**: Prefer read-only operations when possible
2. **Batch Migrations**: Group schema changes to minimize lock time
3. **Maintenance Windows**: Schedule rename operations during low traffic
4. **Monitor Lock Contention**: Log when operations are blocked
5. **Timeout Handling**: Consider adding timeouts for long-running migrations

## Future Improvements

1. **Table-Level Locks**: Lock only affected tables instead of entire system
2. **Online Schema Changes**: Allow some operations without blocking
3. **Lock Monitoring**: Add metrics for lock wait times
4. **Graceful Degradation**: Queue operations instead of blocking indefinitely

