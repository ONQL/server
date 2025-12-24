package database

import "onql/storemanager"

// Insert wrapper using globalDB
func Insert(dbName, tableName string, data map[string]interface{}) (string, error) {
	if globalDB == nil {
		// return "", fmt.Errorf("global DB not initialized")
		// avoiding fmt separate import if not needed, but query.go uses it.
		// Assuming panic or error is fine.
		panic("global DB not initialized")
	}
	return globalDB.Insert(dbName, tableName, data)
}

// Update wrapper using globalDB
func Update(dbName, tableName, pk string, data map[string]interface{}) error {
	if globalDB == nil {
		panic("global DB not initialized")
	}
	return globalDB.Update(dbName, tableName, pk, data)
}

// Delete wrapper using globalDB
func Delete(dbName, tableName, pk string) error {
	if globalDB == nil {
		panic("global DB not initialized")
	}
	return globalDB.Delete(dbName, tableName, pk)
}

// CreateDatabase wrapper using globalDB
func CreateDatabase(dbName string) error {
	if globalDB == nil {
		panic("global DB not initialized")
	}
	return globalDB.sm.CreateDatabase(dbName)
}

// CreateTable wrapper using globalDB
func CreateTable(dbName string, table storemanager.Table) error {
	if globalDB == nil {
		panic("global DB not initialized")
	}
	return globalDB.sm.CreateTable(dbName, table)
}

// DropDatabase wrapper using globalDB
func DropDatabase(dbName string) error {
	if globalDB == nil {
		panic("global DB not initialized")
	}
	return globalDB.sm.DropDatabase(dbName)
}

// DropTable wrapper using globalDB
func DropTable(dbName, tableName string) error {
	if globalDB == nil {
		panic("global DB not initialized")
	}
	return globalDB.sm.DropTable(dbName, tableName)
}

// RenameDatabase wrapper using globalDB
func RenameDatabase(oldName, newName string) error {
	if globalDB == nil {
		panic("global DB not initialized")
	}
	return globalDB.sm.RenameDatabase(oldName, newName)
}

// RenameTable wrapper using globalDB
func RenameTable(dbName, oldName, newName string) error {
	if globalDB == nil {
		panic("global DB not initialized")
	}
	return globalDB.sm.RenameTable(dbName, oldName, newName)
}

// AlterTable wrapper using globalDB
func AlterTable(dbName, tableName string, changes map[string]interface{}) error {
	if globalDB == nil {
		panic("global DB not initialized")
	}
	return globalDB.sm.AlterTable(dbName, tableName, changes)
}

// FetchDatabases wrapper using globalDB
func FetchDatabases() []string {
	if globalDB == nil {
		panic("global DB not initialized")
	}
	return globalDB.sm.FetchDatabases()
}

// FetchTables wrapper using globalDB
func FetchTables(dbName string) ([]string, error) {
	if globalDB == nil {
		panic("global DB not initialized")
	}
	return globalDB.sm.FetchTables(dbName)
}
