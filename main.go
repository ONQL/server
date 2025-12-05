package main

import (
	"fmt"
	"log"
	"onql/config"
	"onql/database"
	"onql/storemanager"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	// Cleanup previous run
	os.RemoveAll("./store")

	// 1. Load Config
	cfg := config.Load()
	// cfg.DBPath is now "./store" by default

	// 2. Initialize DB
	db, err := database.New(cfg)
	if err != nil {
		log.Fatal(err)
	}

	// Graceful Shutdown Handling
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-c
		fmt.Println("\nShutting down...")
		db.Close()
		os.Exit(0)
	}()
	// Defer close in case of normal exit
	defer db.Close()

	fmt.Println("Database initialized.")

	// 2. Create Database
	err = db.CreateDatabase("testdb")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Database 'testdb' created.")

	// 3. Create Table
	usersTable := storemanager.Table{
		Name: "users",
		PK:   "id",
		Columns: map[string]*storemanager.Column{
			"id":      {Name: "id", Type: storemanager.TypeString, Indexed: true},
			"name":    {Name: "name", Type: storemanager.TypeString, Formatter: "trim|upper", Validator: "required", Indexed: true},
			"age":     {Name: "age", Type: storemanager.TypeNumber, Validator: "min:18", Indexed: true},
			"email":   {Name: "email", Type: storemanager.TypeString, Validator: "required", Indexed: true},
			"balance": {Name: "balance", Type: storemanager.TypeNumber, Formatter: "decimal:2"},
		},
	}
	err = db.CreateTable("testdb", usersTable)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Table 'users' created.")

	// 4. Insert User
	user1 := map[string]interface{}{
		"id":      "1",
		"name":    "  john doe  ",
		"age":     25,
		"email":   "john@example.com",
		"balance": 100.555,
	}
	err = db.Insert("testdb", "users", user1)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("User 1 inserted.")

	// 5. Get User (RAM)
	u1, err := db.Get("testdb", "users", "1")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("User 1 (RAM): %+v\n", u1)

	// Verify formatting
	if u1["name"] != "JOHN DOE" {
		log.Fatalf("Name formatting failed: expected 'JOHN DOE', got '%v'", u1["name"])
	}
	if u1["balance"] != 100.56 {
		log.Fatalf("Balance formatting failed: expected 100.56, got '%v'", u1["balance"])
	}

	// 6. Wait for Flush
	fmt.Println("Waiting for flush...")
	time.Sleep(1 * time.Second)

	// 7. Get User (Disk - simulated by checking if it's still there,
	// ideally we'd restart the DB to prove persistence, but for now we trust the flush logic)
	u1Disk, err := db.Get("testdb", "users", "1")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("User 1 (Disk): %+v\n", u1Disk)

	// 8. Query by Index
	pks, err := db.GetPkByIndex("testdb", "users", "name", "JOHN DOE")
	if err != nil {
		log.Fatal("Index lookup failed:", err)
	}
	fmt.Printf("Index lookup 'JOHN DOE' -> PKs: %v\n", pks)
	if len(pks) == 0 || pks[0] != "1" {
		log.Fatalf("Index lookup failed: expected ['1'], got '%v'", pks)
	}

	// 9. Update User
	updateData := map[string]interface{}{
		"age": 30,
	}
	err = db.Update("testdb", "users", "1", updateData)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("User 1 updated (age -> 30).")

	u1Updated, err := db.Get("testdb", "users", "1")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Printf("User 1 (Updated): %+v\n", u1Updated)
	if u1Updated["age"] != 30 { // JSON unmarshal might give float64
		// Check type
		val := u1Updated["age"]
		if v, ok := val.(float64); ok && v == 30 {
			// ok
		} else if v, ok := val.(int); ok && v == 30 {
			// ok
		} else {
			log.Fatalf("Update failed: expected 30, got %v", val)
		}
	}

	// 10. Delete User
	err = db.Delete("testdb", "users", "1")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("User 1 deleted.")

	_, err = db.Get("testdb", "users", "1")
	if err == nil {
		log.Fatal("User 1 should be deleted")
	}
	fmt.Println("User 1 deletion verified.")

	// --- Extension Demo ---
	// fmt.Println("\n--- Extension Demo ---")

	// Mock SDK
	// sdk := &ConsoleSDK{}
	// handler := schema.NewRequestHandler(db, sdk)

	// 1. Create DB via Extension
	// cmd := map[string]interface{}{
	// 	"payload": `["create", "db", "ext_db"]`,
	// }
	// if err := handler.HandleRequest(cmd); err != nil {
	// 	log.Fatal(err)
	// }

	// 2. Set Schema (Complex Sync)
	// Define schema: ext_db -> products -> name, price
	// setPayload := `["set", {
	// 	"ext_db": {
	// 		"products": {
	// 			"name": {"type": "string", "validator": "required"},
	// 			"price": {"type": "number", "formatter": "decimal:2"}
	// 		}
	// 	}
	// }]`
	// cmdSet := map[string]interface{}{
	// 	"payload": setPayload,
	// }
	// if err := handler.HandleRequest(cmdSet); err != nil {
	// 	log.Fatal(err)
	// }

	// 3. Desc Schema
	// cmdDesc := map[string]interface{}{
	// 	"payload": `["desc", "ext_db", "products"]`,
	// }
	// if err := handler.HandleRequest(cmdDesc); err != nil {
	// 	log.Fatal(err)
	// }

	// fmt.Println("Verification passed!")
}

type ConsoleSDK struct{}

func (s *ConsoleSDK) Response(msg map[string]interface{}, res interface{}) error {
	fmt.Printf("SDK Response: %+v\n", res)
	return nil
}
