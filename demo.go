package main

import (
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	// Allow any origin for testing
	CheckOrigin: func(r *http.Request) bool { return true },
}

func handleWebSocket(w http.ResponseWriter, r *http.Request) {
	// 1. Upgrade the connection! (Your code here)
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println(err)
		return
	}

	// 2. Defer closing the connection
	defer conn.Close()

	// 3. Create an unbuffered channel

	type messageChannelStruct struct {
		messageType int
		message     []byte
	}

	websocket_channel := make(chan messageChannelStruct)

	err = conn.SetReadDeadline(time.Now().Add(time.Second * 10))
	if err != nil {
		log.Println("Error occured while setting read deadline", err)
		return
	}

	// 4. create a read pump
	go func() {
		for {
			messageType, p, err := conn.ReadMessage()
			if err != nil {
				log.Println("Error occured while reading message", err)
				// close the channel when the read pump exits to signal the main loop
				close(websocket_channel)
				return
			}

			err = conn.SetReadDeadline(time.Now().Add(time.Second * 10))
			if err != nil {
				log.Println("Error occured while setting read deadline", err)
				return
			}
			websocket_channel <- messageChannelStruct{
				messageType: messageType,
				message:     p,
			}
		}
	}()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case msgFromChannel, ok := <-websocket_channel:
			if !ok {
				fmt.Println("Channel closed, exiting write pump.")
				return
			}

			conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
			err := conn.WriteMessage(msgFromChannel.messageType, msgFromChannel.message)
			if err != nil {
				log.Println("Write error:", err)
				return
			}
		case heartBeat := <-ticker.C:
			fmt.Println("Tick at", heartBeat)
			conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
			err := conn.WriteMessage(websocket.TextMessage, []byte("SERVER_HEARTBEAT"))
			if err != nil {
				log.Println("Heartbeat write error:", err)
				return
			}
		}
	}

}

func main() {
	http.HandleFunc("/ws", handleWebSocket)
	fmt.Println("Starting strict echo server on :8080...")
	log.Fatal(http.ListenAndServe(":8080", nil))
}
