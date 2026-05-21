package manager

import (
	"connection-manager/internal/worker"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestNewClient(t *testing.T) {
	clientId := "29f3b713-0a7c-4dd8-b53d-048fb26b839a"
	conn := &websocket.Conn{}

	newClient := NewClient(clientId, conn)

	if newClient.ClientId != clientId {
		t.Errorf("Expected ClientId to be %s, got %s", clientId, newClient.ClientId)
	}

	if newClient.Conn != conn {
		t.Errorf("Expected Conn to be correctly assigned")
	}

	if newClient.MessageChannel == nil {
		t.Errorf("Expected MessageChannel to be initialized, but got nil")
	}

	if cap(newClient.MessageChannel) != MAX_CLIENT_MESSAGE_SIZE {
		t.Errorf("Expected MessageChannel capacity to be %d, got %d", MAX_CLIENT_MESSAGE_SIZE, cap(newClient.MessageChannel))
	}
}

func TestRegister(t *testing.T) {
	cm := &ClientManager{Clients: make(map[string]*Client)}
	conn := &websocket.Conn{}
	clientId := "client-1"

	// 1. Test successful registration
	err := cm.Register(clientId, conn)
	if err != nil {
		t.Errorf("Expected successful registration, got error: %v", err)
	}

	if _, exists := cm.Clients[clientId]; !exists {
		t.Errorf("Expected client to be registered in map")
	}

	// 2. Test duplicate registration (should not return error according to logic)
	err = cm.Register(clientId, conn)
	if err != nil {
		t.Errorf("Expected no error on duplicate registration, got: %v", err)
	}

	// 3. Test registration when server is shutting down
	cm.IsShutting = true
	err = cm.Register("client-2", conn)
	if err == nil {
		t.Errorf("Expected an error when trying to register while server is shutting down")
	}
}

func TestUnRegister(t *testing.T) {
	cm := &ClientManager{Clients: make(map[string]*Client)}
	conn := &websocket.Conn{}
	clientId := "client-1"

	// Set up the client first

	cm.Register(clientId, conn)

	// 1. Test unregistering with mismatched connection (should keep the client)
	diffConn := &websocket.Conn{} // A different memory address
	cm.UnRegister(clientId, diffConn)
	if _, exists := cm.Clients[clientId]; !exists {
		t.Errorf("Expected client to remain registered because connection mismatched")
	}

	// 2. Test successful unregister
	cm.UnRegister(clientId, conn)
	if _, exists := cm.Clients[clientId]; exists {
		t.Errorf("Expected client to be removed from map")
	}

	// 3. Test unregistering a non-existent client (should simply log and not panic)
	// We can't easily assert the log, but we can ensure it doesn't crash the test
	cm.UnRegister("non-existent-client", conn)

}

func TestReadPump(t *testing.T) {
	const numClients = 12

	upgrader := websocket.Upgrader{}
	serverConns := make(chan *websocket.Conn, numClients)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("Upgrade failed: %v", err)
			return
		}
		serverConns <- conn
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")

	cm := &ClientManager{Clients: make(map[string]*Client)}
	pool := worker.NewWorkerPool(4)
	pool.Start()
	defer pool.Stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	type entry struct {
		clientConn *websocket.Conn
		done       chan struct{}
	}
	entries := make([]entry, numClients)

	// Dial all clients and register them before starting any ReadPump.
	for i := 0; i < numClients; i++ {
		clientConn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
		if err != nil {
			t.Fatalf("client %d: Dial failed: %v", i, err)
		}
		entries[i] = entry{clientConn: clientConn, done: make(chan struct{})}
	}

	// Collect server-side connections and start ReadPumps.
	for i := 0; i < numClients; i++ {
		var serverConn *websocket.Conn
		select {
		case serverConn = <-serverConns:
		case <-time.After(2 * time.Second):
			t.Fatalf("client %d: timed out waiting for server connection", i)
		}

		clientId := fmt.Sprintf("client-%d", i+1)
		client := NewClient(clientId, serverConn)
		cm.Lock()
		cm.Clients[clientId] = client
		cm.Unlock()

		done := entries[i].done
		go func() {
			client.ReadPump(ctx, cm, pool)
			close(done)
		}()
	}

	// Send a message from each client.
	for i, e := range entries {
		msg := fmt.Sprintf("hello from client-%d", i+1)
		if err := e.clientConn.WriteMessage(websocket.TextMessage, []byte(msg)); err != nil {
			t.Errorf("client %d: Write failed: %v", i+1, err)
		}
	}

	time.Sleep(50 * time.Millisecond)

	// Close all client connections to unblock ReadPump, then cancel context.
	for _, e := range entries {
		e.clientConn.Close()
	}
	cancel()

	// Verify all ReadPumps exit gracefully.
	for i, e := range entries {
		select {
		case <-e.done:
		case <-time.After(2 * time.Second):
			t.Errorf("client %d: ReadPump did not exit in time", i+1)
		}
	}
}

func TestWritePump(t *testing.T) {
	const numClients = 12

	upgrader := websocket.Upgrader{}
	serverConns := make(chan *websocket.Conn, numClients)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Errorf("Upgrade failed: %v", err)
			return
		}
		serverConns <- conn
	}))
	defer server.Close()

	wsURL := "ws" + strings.TrimPrefix(server.URL, "http")

	cm := &ClientManager{Clients: make(map[string]*Client)}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	type entry struct {
		clientConn *websocket.Conn
		done       chan struct{}
	}
	entries := make([]entry, numClients)

	// Dial all clients and register them before starting any WritePump.
	for i := 0; i < numClients; i++ {
		clientConn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
		if err != nil {
			t.Fatalf("client %d: Dial failed: %v", i, err)
		}
		entries[i] = entry{clientConn: clientConn, done: make(chan struct{})}
	}

	// 4. Start WritePump in a separate goroutine with a cancelable context:
	//    go client.WritePump(ctx)

	for i := 0; i < numClients; i++ {
		var serverConn *websocket.Conn
		select {
		case serverConn = <-serverConns:
		case <-time.After(2 * time.Second):
			t.Fatalf("client %d: timed out waiting for server connection", i)
		}

		clientId := fmt.Sprintf("client-%d", i+1)
		client := NewClient(clientId, serverConn)
		cm.Lock()
		cm.Clients[clientId] = client
		cm.Unlock()

		done := entries[i].done
		go func() {
			client.WritePump(ctx)
			close(done)
		}()
	}

	// 5. Push a message (e.g., "test-message") into client.MessageChannel.

	for i := 0; i < numClients; i++ {
		clientId := fmt.Sprintf("client-%d", i+1)
		clientMessage := fmt.Sprintf("Hello %s!!!, this is test message", clientId)
		cm.SendMessage(clientId, clientMessage)
	}

	// 6. Read from the dialer client-side connection and assert that the message received matches.

	for i, e := range entries {
		_, message, err := e.clientConn.ReadMessage()
		if err != nil {
			t.Errorf("client %d: Reading Message has failed: %v", i+1, err)
		}
		extractedMessage := string(message)
		fmt.Printf("Message Received : %s\n", extractedMessage)

		// Assert the message contents
		expectedMessage := fmt.Sprintf("Hello client-%d!!!, this is test message", i+1)
		if extractedMessage != expectedMessage {
			t.Errorf("client %d: Expected message %q, got %q", i+1, expectedMessage, extractedMessage)
		}
	}

	// 7. Test shutdown: close client.MessageChannel (or cancel context) and assert that WritePump exits gracefully (e.g., using a channel/sync.WaitGroup).
	cancel()

	for i, e := range entries {
		select {
		case <-e.done:
			// WritePump exited successfully
		case <-time.After(2 * time.Second):
			t.Errorf("client %d: WritePump did not exit in time", i+1)
		}
		e.clientConn.Close()
	}
}

func TestDrain(t *testing.T) {
	cm := &ClientManager{Clients: make(map[string]*Client)}

	// Register multiple clients
	clientIDs := []string{"client-1", "client-2", "client-3"}
	conns := make([]*websocket.Conn, len(clientIDs))

	for i, id := range clientIDs {
		conns[i] = &websocket.Conn{}
		err := cm.Register(id, conns[i])
		if err != nil {
			t.Fatalf("Failed to register %s: %v", id, err)
		}
	}

	// 1. Fill client-3's message channel to capacity to verify non-blocking drop logic.
	fullClient, exists := cm.GetClient("client-3")
	if !exists {
		t.Fatalf("client-3 not found")
	}
	for i := 0; i < cap(fullClient.MessageChannel); i++ {
		fullClient.MessageChannel <- "fill"
	}

	// 2. Call Drain
	cm.Drain()

	// 3. Verify IsShutting is set to true
	if !cm.IsShutting {
		t.Errorf("Expected IsShutting to be true after Drain")
	}

	// 4. Verify client-1 and client-2 received the shutdown message
	for _, id := range []string{"client-1", "client-2"} {
		client, exists := cm.GetClient(id)
		if !exists {
			t.Errorf("client %s not found", id)
			continue
		}
		select {
		case msg := <-client.MessageChannel:
			expected := "Server shutting down"
			if msg != expected {
				t.Errorf("Expected message %q for client %s, got %q", expected, id, msg)
			}
		default:
			t.Errorf("Expected client %s to have received a shutdown message", id)
		}
	}

	// 5. Verify client-3 did not block and that its queue has no shutdown message
	for i := 0; i < cap(fullClient.MessageChannel); i++ {
		select {
		case msg := <-fullClient.MessageChannel:
			if msg == "Server shutting down" {
				t.Errorf("Did not expect client-3 to receive shutdown message because its queue was full")
			}
		default:
			t.Errorf("Expected to read a message from client-3's queue")
		}
	}
}

func TestSendMessage_BufferFull(t *testing.T) {
	// 1. Initialize ClientManager and register a client
	cm := &ClientManager{Clients: make(map[string]*Client)}
	clientId := "test-client"
	conn := &websocket.Conn{}

	err := cm.Register(clientId, conn)
	if err != nil {
		t.Fatalf("Failed to register client: %v", err)
	}

	client, exists := cm.GetClient(clientId)
	if !exists {
		t.Fatalf("Client was not found after registration")
	}

	// 2. Fill the MessageChannel buffer completely (MAX_CLIENT_MESSAGE_SIZE is 100)
	for i := 0; i < MAX_CLIENT_MESSAGE_SIZE; i++ {
		msg := fmt.Sprintf("message-%d", i)
		cm.SendMessage(clientId, msg)
	}

	// 3. Verify that sending an extra message does NOT block (using a channel timeout check)
	done := make(chan struct{})
	go func() {
		cm.SendMessage(clientId, "dropped-message")
		close(done)
	}()

	select {
	case <-done:
		// Success: SendMessage returned immediately without blocking
	case <-time.After(100 * time.Millisecond):
		t.Fatal("SendMessage blocked when the client message buffer was full")
	}

	// 4. Verify that all 100 original messages exist, and the 101st was indeed dropped
	for i := 0; i < MAX_CLIENT_MESSAGE_SIZE; i++ {
		select {
		case msg := <-client.MessageChannel:
			expected := fmt.Sprintf("message-%d", i)
			if msg != expected {
				t.Errorf("Expected message %q, got %q", expected, msg)
			}
		default:
			t.Errorf("Expected to read message-%d, but channel was empty", i)
		}
	}

	// 5. Verify the channel is now completely empty and didn't queue the 101st message
	select {
	case msg := <-client.MessageChannel:
		t.Errorf("Expected channel to be empty, but found message: %q", msg)
	default:
		// Success: channel is empty, confirming "dropped-message" was dropped
	}
}

func TestPriorityMapping(t *testing.T) {
	tests := []struct {
		message  string
		expected worker.Priority
	}{
		{"ping", worker.High},
		{"heartbeat", worker.High},
		{"close", worker.High},
		{"hello", worker.Low},
		{"random-message", worker.Low},
		{"", worker.Low},
	}

	for _, tc := range tests {
		t.Run(tc.message, func(t *testing.T) {
			actual := priority(tc.message)
			if actual != tc.expected {
				t.Errorf("For message %q, expected priority %v, got %v", tc.message, tc.expected, actual)
			}
		})
	}
}

func TestListenMessage(t *testing.T) {
	cm := &ClientManager{Clients: make(map[string]*Client)}
	clientId := "test-client"
	conn := &websocket.Conn{}

	err := cm.Register(clientId, conn)
	if err != nil {
		t.Fatalf("Failed to register client: %v", err)
	}

	client, _ := cm.GetClient(clientId)

	// Create a worker pool but do not start it (we just want to check queues)
	pool := worker.NewWorkerPool(1)

	// 1. Test low priority message
	cm.ListenMessage(clientId, "hello world", pool)

	select {
	case job := <-pool.LowPriorityQueue:
		if job.ClientID != clientId {
			t.Errorf("Expected ClientID %q, got %q", clientId, job.ClientID)
		}
		if job.Message != "hello world" {
			t.Errorf("Expected Message %q, got %q", "hello world", job.Message)
		}
		if job.ReplyChan != client.MessageChannel {
			t.Errorf("Expected ReplyChan to be the client's MessageChannel")
		}
	default:
		t.Errorf("Expected job to be queued in LowPriorityQueue")
	}

	// 2. Test high priority message
	cm.ListenMessage(clientId, "ping", pool)

	select {
	case job := <-pool.HighPriorityQueue:
		if job.ClientID != clientId {
			t.Errorf("Expected ClientID %q, got %q", clientId, job.ClientID)
		}
		if job.Message != "ping" {
			t.Errorf("Expected Message %q, got %q", "ping", job.Message)
		}
		if job.ReplyChan != client.MessageChannel {
			t.Errorf("Expected ReplyChan to be the client's MessageChannel")
		}
	default:
		t.Errorf("Expected job to be queued in HighPriorityQueue")
	}

	// 3. Test non-existent client (should do nothing)
	cm.ListenMessage("non-existent", "ping", pool)

	if len(pool.HighPriorityQueue) != 0 {
		t.Errorf("Expected HighPriorityQueue to be empty after calling ListenMessage with invalid client ID")
	}
	if len(pool.LowPriorityQueue) != 0 {
		t.Errorf("Expected LowPriorityQueue to be empty after calling ListenMessage with invalid client ID")
	}
}

