package main

import (
	"connection-manager/internal/manager"
	"connection-manager/internal/transport"
	"connection-manager/internal/worker"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
)

func main() {

	cm := manager.ClientManager{
		Clients:    make(map[string]*manager.Client),
		IsShutting: false,
	}

	workerPool := worker.NewWorkerPool(5)

	workerPool.Start()

	// Capture OS signals for graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)

	// Launch the background assassin to wait for the signal
	go func() {
		<-quit
		fmt.Println("\nReceived shutdown signal. Stopping worker pool securely...")
		cm.Drain()
		workerPool.Stop()
		fmt.Println("Worker pool stopped. Exiting.")
		os.Exit(0)
	}()

	transport := transport.WebSocketServer{
		Manager:    &cm,
		WorkerPool: workerPool,
	}

	http.HandleFunc("/ws", transport.ServeWS)
	fmt.Println("Starting strict echo server on :8080...")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
