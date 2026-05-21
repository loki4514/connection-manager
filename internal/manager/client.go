package manager

import (
	"connection-manager/internal/worker"
	"context"
	"errors"
	"log"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

type Client struct {
	ClientId       string
	MessageChannel chan string
	Conn           *websocket.Conn
}

type ClientManager struct {
	Clients    map[string]*Client
	IsShutting bool
	sync.RWMutex
}

const MAX_CLIENT_MESSAGE_SIZE = 100
const MAX_READ_CONNECTION_TIMOUT_SEC = 10

func NewClient(clientId string, conn *websocket.Conn) *Client {
	return &Client{
		ClientId:       clientId,
		MessageChannel: make(chan string, MAX_CLIENT_MESSAGE_SIZE),
		Conn:           conn,
	}
}

func (cm *ClientManager) GetClient(clientId string) (*Client, bool) {
	cm.RLock()
	defer cm.RUnlock()
	client, exists := cm.Clients[clientId]
	return client, exists
}

func (cm *ClientManager) Register(clientId string, conn *websocket.Conn) error {
	connectionCloseError := errors.New("SERVER_CLOSE")

	cm.Lock()
	defer cm.Unlock()

	if cm.IsShutting {
		return errors.New("denying new connection acceptance, due to server shut down: " + connectionCloseError.Error())
	}

	// Access cm.Clients directly — we already hold the write lock
	if _, exists := cm.Clients[clientId]; exists {
		log.Printf("Client already exists %s", clientId)
		return nil
	}

	cm.Clients[clientId] = NewClient(clientId, conn)
	log.Println("Client successfully registered", clientId)

	return nil
}

func (cm *ClientManager) UnRegister(clientId string, conn *websocket.Conn) {
	cm.Lock()
	defer cm.Unlock()

	client, exists := cm.Clients[clientId]
	if !exists {
		log.Printf("Client does not exist %s", clientId)
		return
	}

	// Optional: verify the connection matches
	if client.Conn != conn {
		log.Printf("Connection mismatch for client %s", clientId)
		return
	}

	close(client.MessageChannel)
	delete(cm.Clients, clientId)

	log.Printf("Client successfully unregistered %s", clientId)
}

func (c *Client) ReadPump(ctx context.Context, cm *ClientManager, workerPool *worker.Pool) {
	defer c.Conn.Close()

	err := c.Conn.SetReadDeadline(time.Now().Add(time.Second * MAX_READ_CONNECTION_TIMOUT_SEC))
	if err != nil {
		log.Println("Error occurred while setting read deadline", err)
		return
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		_, p, err := c.Conn.ReadMessage()
		if err != nil {
			log.Println("Error occurred while reading message", err)
			return
		}

		err = c.Conn.SetReadDeadline(time.Now().Add(time.Second * MAX_READ_CONNECTION_TIMOUT_SEC))
		if err != nil {
			log.Println("Error occurred while setting read deadline", err)
			return
		}

		cm.ListenMessage(c.ClientId, string(p), workerPool)
	}
}

func (cm *ClientManager) SendMessage(clientId string, message string) {
	client, exists := cm.GetClient(clientId)
	if !exists {
		log.Printf("SendMessage: client %s not found", clientId)
		return
	}

	select {
	case client.MessageChannel <- message:
	default:
		log.Printf("Message queue full for client %s, dropping message", clientId)
	}
}

func (cm *ClientManager) Drain() {
	cm.Lock()
	defer cm.Unlock()

	cm.IsShutting = true
	for _, client := range cm.Clients {
		select {
		case client.MessageChannel <- "Server shutting down":
		default:
		}
	}
}

var highPriorityMessages = map[string]bool{
	"ping":      true,
	"heartbeat": true,
	"close":     true,
}

func priority(message string) worker.Priority {
	if highPriorityMessages[message] {
		return worker.High
	}
	return worker.Low
}

func (cm *ClientManager) ListenMessage(clientId string, message string, workerPool *worker.Pool) {
	client, exists := cm.GetClient(clientId)
	if !exists {
		return
	}

	workerPool.SubmitJob(worker.Job{
		ClientID:  clientId,
		Message:   message,
		ReplyChan: client.MessageChannel,
	}, priority(message))
}

func (cm *ClientManager) HeartBeat(clientId string, message string) {
	cm.SendMessage(clientId, message)
}

// WritePump continually pulls from the client's internal bounded channel
// and writes the data across the physical network.
func (c *Client) WritePump(ctx context.Context) {
	defer c.Conn.Close()

	for {
		select {
		case <-ctx.Done():
			return
		case message, ok := <-c.MessageChannel:
			if !ok {
				return
			}
			c.Conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
			err := c.Conn.WriteMessage(websocket.TextMessage, []byte(message))
			if err != nil {
				log.Println("Write error or client disconnected:", err)
				return
			}
		}
	}
}
