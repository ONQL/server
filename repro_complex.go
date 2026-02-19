package main

import (
	"context"
	"fmt"
	"onql/config"
	"onql/database"
	"onql/dsl"
	"onql/storemanager"
	"os"
	"time"
)

func main() {
	// 1. Initialize DB
	cfg := &config.Config{
		DBPath:        "./testdb_data",
		LogLevel:      "INFO",
		FlushInterval: 1000 * time.Millisecond,
	}
	db, err := database.New(cfg)
	if err != nil {
		fmt.Printf("Error initializing DB: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	dbName := "testdb"
	tableName := "users"

	// 2. Create Database & Table
	// Ensure DB exists in schema (New doesn't auto-create it in schema, just high-level object)
	// Actually database.CreateDatabase(dbName) might be needed.
	err = database.CreateDatabase(dbName)
	if err != nil {
		// Ignore if exists? Or print error.
		// fmt.Printf("Error creating database: %v\n", err)
	}

	err = database.CreateTable(dbName, storemanager.Table{
		ID:   "users",
		Name: "users",
		Columns: map[string]*storemanager.Column{
			"id":   {ID: "id", Name: "id", Type: "string"},
			"name": {ID: "name", Name: "name", Type: "string"},
			"age":  {ID: "age", Name: "age", Type: "int", Indexed: true}, // Index on Age for sorting
		},
		PK: "id",
	})
	if err != nil {
		fmt.Printf("Error creating table: %v\n", err)
		// continue anyway if exists
	}

	// 3. Insert Data
	// Insert multiple Charlies with different ages to test sorting and filtering
	users := []map[string]interface{}{
		{"id": "1", "name": "Alice", "age": 30},
		{"id": "2", "name": "Bob", "age": 25},
		{"id": "3", "name": "Charlie", "age": 35}, // Oldest Charlie (Limit 1)
		{"id": "4", "name": "Dave", "age": 40},
		{"id": "5", "name": "Charlie", "age": 22}, // Youngest Charlie
		{"id": "6", "name": "Charlie", "age": 28}, // Middle Charlie (Limit 2)
	}

	for _, u := range users {
		_, err := database.Insert(dbName, tableName, u)
		if err != nil {
			fmt.Printf("Error inserting: %v\n", err)
		}
	}

	// 3.5 Setup Protocol (Required for Parser validation)
	protocol := storemanager.QueryProtocol{
		dbName: &storemanager.ProtocolModule{
			Database: dbName,
			Entities: map[string]*storemanager.Entity{
				"users": {
					Table: tableName,
					Fields: map[string]string{
						"id":   "id",
						"name": "name",
						"age":  "age",
					},
				},
			},
		},
	}
	db.SetProtocol("pass", protocol)

	// 4. Run Query: Filter=Charlie, Sort=Age (Desc), Limit=2
	// Expect: Charlie(35), Charlie(28). (Charlie(22) is skipped due to limit)
	query := `testdb.users[name="Charlie"]._desc(age)[0:2]`
	fmt.Printf("\nRunning Query: %s\n", query)

	result, err := dsl.Execute(context.Background(), "pass", query, "", nil)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("Result:", result)
}
