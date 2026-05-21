package transport

import (
	"connection-manager/internal/manager"
	"connection-manager/internal/worker"
	"context"
	"log"
	"net/http"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	// Allow any origin for testing
	CheckOrigin: func(r *http.Request) bool { return true },
}

type WebSocketServer struct {
	Manager    *manager.ClientManager
	WorkerPool *worker.Pool
}

func (s *WebSocketServer) ServeWS(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println(err)
		return
	}

	clientId := uuid.New().String()

	err = s.Manager.Register(clientId, conn)
	if err != nil {
		log.Println(err)
		// Server is shutting down — tell the client cleanly
		conn.WriteMessage(
			websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseServiceRestart, "server shutting down"),
		)
		conn.Close()
		return
	}

	client, exists := s.Manager.GetClient(clientId)
	if !exists {
		log.Println("client not found after registration")
		// Internal bug — close with internal error code
		conn.WriteMessage(
			websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseInternalServerErr, "internal error"),
		)
		conn.Close()
		return
	}

	defer s.Manager.UnRegister(clientId, conn)

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	go client.WritePump(ctx)
	client.ReadPump(ctx, s.Manager, s.WorkerPool)
}
