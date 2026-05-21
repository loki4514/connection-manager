package main

import (
	"fmt"
	"log"
	"sync"

	"github.com/gorilla/websocket"
)

func simulateClient(id int, wg *sync.WaitGroup) {
	defer wg.Done()

	u := "ws://localhost:8080/ws"

	conn, _, err := websocket.DefaultDialer.Dial(u, nil)
	if err != nil {
		log.Printf("Client %d failed to connect: %v", id, err)
		return
	}

	// reader goroutine
	go func() {
		for {
			_, _, err := conn.ReadMessage()
			if err != nil {
				log.Printf("Client %d disconnected", id)
				return
			}
		}
	}()

	// flood
	for i := 1; i <= 500; i++ {
		msg := fmt.Sprintf(
			"Hello from client %d [%d]",
			id,
			i,
		)

		err := conn.WriteMessage(
			websocket.TextMessage,
			[]byte(msg),
		)

		if err != nil {
			log.Printf("write failed: %v", err)
			return
		}
	}

	log.Printf("Client %d finished flooding", id)

}

// Load test
func main() {
	var wg sync.WaitGroup
	numClients := 1

	fmt.Printf("Starting load test with %d clients...\n", numClients)

	for i := 1; i <= numClients; i++ {
		// 2. MUST add exactly 1 to the wait group per loop
		wg.Add(1)

		// 3. MUST pass the waitgroup by POINTER (&wg), otherwise each goroutine gets a copy!
		go simulateClient(i, &wg)
	}

	wg.Wait()
	fmt.Println("Load test complete.")
}
