package transport

import (
	"connection-manager/internal/manager"
	"connection-manager/internal/worker"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestServeWS_Success(t *testing.T) {
	// 1. Setup ClientManager and WorkerPool
	cm := &manager.ClientManager{Clients: make(map[string]*manager.Client)}
	pool := worker.NewWorkerPool(2)
	pool.Start()
	defer pool.Stop()

	// 2. Setup WebSocketServer
	wsServer := &WebSocketServer{
		Manager:    cm,
		WorkerPool: pool,
	}

	// 3. Setup httptest server
	server := httptest.NewServer(http.HandlerFunc(wsServer.ServeWS))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")

	// 4. Dial server
	clientConn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("Failed to dial WebSocket server: %v", err)
	}
	defer clientConn.Close()

	// Allow some time for registration
	time.Sleep(50 * time.Millisecond)

	// Verify client is registered in manager
	cm.Lock()
	clientsCount := len(cm.Clients)
	var registeredClientID string
	for id := range cm.Clients {
		registeredClientID = id
	}
	cm.Unlock()

	if clientsCount != 1 {
		t.Fatalf("Expected exactly 1 registered client, got %d", clientsCount)
	}

	// 5. Send a message to server
	testMessage := "hello-world"
	err = clientConn.WriteMessage(websocket.TextMessage, []byte(testMessage))
	if err != nil {
		t.Fatalf("Failed to send message: %v", err)
	}

	// 6. Read processed response from server
	_, responseBytes, err := clientConn.ReadMessage()
	if err != nil {
		t.Fatalf("Failed to read reply: %v", err)
	}

	expectedReply := "Processed Message: " + testMessage
	if string(responseBytes) != expectedReply {
		t.Errorf("Expected reply %q, got %q", expectedReply, string(responseBytes))
	}

	// 7. Close client connection and verify unregistration
	clientConn.Close()

	// Wait for UnRegister defer to execute
	time.Sleep(100 * time.Millisecond)

	cm.Lock()
	_, exists := cm.Clients[registeredClientID]
	cm.Unlock()

	if exists {
		t.Errorf("Expected client %s to be unregistered after connection closed", registeredClientID)
	}
}

func TestServeWS_ShuttingDown(t *testing.T) {
	// 1. Setup ClientManager with IsShutting = true
	cm := &manager.ClientManager{
		Clients:    make(map[string]*manager.Client),
		IsShutting: true,
	}
	pool := worker.NewWorkerPool(1)
	pool.Start()
	defer pool.Stop()

	wsServer := &WebSocketServer{
		Manager:    cm,
		WorkerPool: pool,
	}

	server := httptest.NewServer(http.HandlerFunc(wsServer.ServeWS))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")

	// 2. Dial server and expect it to be closed
	clientConn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		// Dialer might fail to establish connection, which is also an acceptable rejection behavior.
		return
	}
	defer clientConn.Close()

	// 3. Try to read from connection, expecting close frame CloseServiceRestart
	_, _, err = clientConn.ReadMessage()
	if err == nil {
		t.Fatal("Expected read to fail due to connection closure")
	}

	closeErr, ok := err.(*websocket.CloseError)
	if !ok {
		t.Fatalf("Expected CloseError, got: %v", err)
	}

	if closeErr.Code != websocket.CloseServiceRestart {
		t.Errorf("Expected close code %d (CloseServiceRestart), got %d", websocket.CloseServiceRestart, closeErr.Code)
	}
}
