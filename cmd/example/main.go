package main

import (
	"fmt"
	"log"
	"onql/config"
	"onql/database"
	"onql/dsl"
	"onql/storemanager"
	"os"
	"time"
)

func main() {
	// Cleanup previous run
	os.RemoveAll("./store_example")

	// 1. Load Config
	cfg := config.Load()
	cfg.DBPath = "./store_example" // Use separate store for example

	// 2. Initialize DB
	db, err := database.New(cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	fmt.Println("Database initialized.")

	// 3. Create Database
	dbName := "shop"
	err = db.CreateDatabase(dbName)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Database '%s' created.\n", dbName)

	// 4. Create Table
	tableName := "products"
	table := storemanager.Table{
		Name: tableName,
		PK:   "id",
		Columns: map[string]*storemanager.Column{
			"id":    {Name: "id", Type: storemanager.TypeString, Indexed: true},
			"name":  {Name: "name", Type: storemanager.TypeString, Indexed: true},
			"price": {Name: "price", Type: storemanager.TypeNumber},
		},
	}
	err = db.CreateTable(dbName, table)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("Table '%s' created.\n", tableName)

	// 5. Insert Data
	product1 := map[string]interface{}{
		"id":    "p1",
		"name":  "Laptop",
		"price": 1200.50,
	}
	_, err = db.Insert(dbName, tableName, product1)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Product 1 inserted.")

	product2 := map[string]interface{}{
		"id":    "p2",
		"name":  "Mouse",
		"price": 25.00,
	}
	_, err = db.Insert(dbName, tableName, product2)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Product 2 inserted.")

	// 6. Run DSL Query
	// Note: The "default" protocol is automatically updated by our hooks.
	// We can use "default" as the protocol password.

	// Query: Get all products
	query := `shop.products[price=1200.50]`

	// Wait for async protocol update
	time.Sleep(100 * time.Millisecond)

	fmt.Printf("\nExecuting DSL Query: %s\n", query)

	// Execute
	result, err := dsl.Execute("default", query, "", nil)
	if err != nil {
		log.Fatalf("DSL Execution failed: %v", err)
	}

	fmt.Println("\nQuery Result:")
	fmt.Printf("%+v\n", result)
}
