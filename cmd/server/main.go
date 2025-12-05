package main

import (
	"fmt"
	"log"
	"onql/api"
	"onql/config"
	"onql/database"
	"onql/server"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	// 1. Load Config
	cfg := config.Load()

	// 2. Initialize DB
	// Note: We do not clear the store directory here to ensure persistence.
	db, err := database.New(cfg)
	if err != nil {
		log.Fatal("Failed to initialize database:", err)
	}

	// 3. Set Global DB for API
	api.SetDatabase(db)

	// 4. Setup Graceful Shutdown
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)

	go func() {
		<-c
		fmt.Println("\nShutting down ONQL Server...")
		db.Close()
		os.Exit(0)
	}()

	// 5. Start TCP Server (Blocks)
	fmt.Println("Starting ONQL Server...")
	server.Setup(cfg)
}
