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
	// Setup generic config
	cfg := &config.Config{
		DBPath:        "./test_db_sort",
		LogLevel:      "info",
		FlushInterval: 5 * time.Second,
	}

	// Initialize DB
	db, err := database.New(cfg)
	if err != nil {
		fmt.Printf("Failed to init DB: %v\n", err)
		os.Exit(1)
	}
	defer db.Close()

	// 1. Setup Data
	dbName := "testdb"
	tableName := "users"

	// Create DB
	_ = database.CreateDatabase(dbName)

	// Create Table
	err = database.CreateTable(dbName, storemanager.Table{
		ID:   tableName,
		Name: tableName,
		PK:   "id",
		Columns: map[string]*storemanager.Column{
			"id":   {ID: "id", Name: "id", Type: storemanager.TypeString, Indexed: true},
			"name": {ID: "name", Name: "name", Type: storemanager.TypeString, Indexed: true},
			"age":  {ID: "age", Name: "age", Type: storemanager.TypeNumber, Indexed: true},
		},
	})

	// Insert Data (Unsorted ID, Unsorted Age)
	users := []map[string]interface{}{
		{"id": "1", "name": "Alice", "age": 30},
		{"id": "2", "name": "Bob", "age": 25},
		{"id": "3", "name": "Charlie", "age": 35},
		{"id": "4", "name": "Dave", "age": 40},
		{"id": "5", "name": "Eve", "age": 22},
	}
	for _, u := range users {
		database.Insert(dbName, tableName, u)
	}

	// Setup Protocol
	protocol := storemanager.QueryProtocol{
		dbName: &storemanager.ProtocolModule{
			Database: dbName,
			Entities: map[string]*storemanager.Entity{
				tableName: {
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
	db.SetProtocol("default", protocol)

	// 2. Query: Sort by Age (Desc) + Slice [0:2]
	// Expect: Dave(40), Charlie(35)
	query := `testdb.users._desc(age)[0:2]`
	fmt.Println("Running Query:", query)

	ctx := context.Background()
	res, err := dsl.Execute(ctx, "default", query, "", nil)
	if err != nil {
		fmt.Printf("Execution Error: %v\n", err)
	} else {
		fmt.Printf("Result: %v\n", res)
	}
}
