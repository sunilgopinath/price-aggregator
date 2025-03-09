// handlers/sse.go
package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"pricenotification/internal/cache"
	"pricenotification/internal/logger"

	"go.uber.org/zap"
)

// AlertMessage represents an alert that will be streamed
type AlertMessage struct {
	UserID    string  `json:"user_id"`
	Symbol    string  `json:"symbol"`
	Threshold float64 `json:"threshold"`
	Triggered string  `json:"triggered"` // "above" or "below"
	Timestamp string  `json:"timestamp"`
}

// SSE Clients
var (
	clients = make(map[chan AlertMessage]bool)
	mu      sync.Mutex
)

// Redis channel name for alerts
const alertsChannel = "price_alerts"

// Initialize Redis subscription for alerts
var alertSubscriber *cache.RedisSubscriber

// InitSSE initializes the SSE system
func InitSSE() {
	// Create a Redis subscriber for alerts
	var err error
	alertSubscriber, err = cache.NewRedisSubscriber(alertsChannel)
	if err != nil {
		logger.Log.Error("Failed to create Redis subscriber", zap.Error(err))
		return
	}

	// Start listening for published alerts
	go listenForAlerts()
}

// listenForAlerts continuously listens for alerts from Redis and broadcasts to clients
func listenForAlerts() {
	logger.Log.Info("Starting to listen for alerts from Redis")
	
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		msg, err := alertSubscriber.ReceiveMessage(ctx)
		cancel()
		
		if err != nil {
			logger.Log.Error("Error receiving message from Redis", zap.Error(err))
			time.Sleep(1 * time.Second) // Wait before retry
			continue
		}

		var alert AlertMessage
		if err := json.Unmarshal([]byte(msg.Payload), &alert); err != nil {
			logger.Log.Error("Error unmarshaling alert message", zap.Error(err))
			continue
		}

		// Broadcast to all connected clients
		logger.Log.Info("Received alert from Redis", 
			zap.String("symbol", alert.Symbol),
			zap.String("triggered", alert.Triggered))
			
		broadcastToClients(alert)
	}
}

// StreamAlertsHandler handles SSE connections
func StreamAlertsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
	w.Header().Set("Access-Control-Allow-Methods", "GET")
	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}

	clientChan := make(chan AlertMessage, 10)

	mu.Lock()
	clients[clientChan] = true
	clientCount := len(clients)
	mu.Unlock()

	logger.Log.Info("New SSE client connected", zap.Int("total_clients", clientCount))

	defer func() {
		mu.Lock()
		delete(clients, clientChan)
		clientCount := len(clients)
		mu.Unlock()
		close(clientChan)
		logger.Log.Info("SSE client disconnected", zap.Int("total_clients", clientCount))
	}()

	// Send heartbeats to keep connection alive
	go func() {
		heartbeatTicker := time.NewTicker(15 * time.Second)
		defer heartbeatTicker.Stop()
		
		for {
			select {
			case <-heartbeatTicker.C:
				select {
				case clientChan <- AlertMessage{Timestamp: time.Now().Format(time.RFC3339)}:
					// Heartbeat sent successfully
				default:
					// Channel is blocked or closed, exit goroutine
					return
				}
			case <-r.Context().Done():
				// Request context done, exit goroutine
				return
			}
		}
	}()

	// Stream events to client
	for alert := range clientChan {
		alertData, err := json.Marshal(alert)
		if err != nil {
			logger.Log.Error("Failed to marshal alert data", zap.Error(err))
			continue
		}
		
		fmt.Fprintf(w, "data: %s\n\n", alertData)
		flusher.Flush()
	}
}

// broadcastToClients sends alert to all connected SSE clients
func broadcastToClients(alert AlertMessage) {
	mu.Lock()
	defer mu.Unlock()

	logger.Log.Info("Broadcasting alert to clients", 
		zap.Int("client_count", len(clients)),
		zap.String("symbol", alert.Symbol))

	if len(clients) == 0 {
		logger.Log.Warn("No SSE clients available! Skipping alert broadcast.")
		return
	}

	for clientChan := range clients {
		select {
		case clientChan <- alert:
			// Alert sent successfully
		default:
			logger.Log.Warn("Alert dropped due to slow client")
		}
	}
}

// BroadcastAlert publishes alert to Redis for distribution
func BroadcastAlert(alert AlertMessage) {
	logger.Log.Info("Publishing alert to Redis", 
		zap.String("symbol", alert.Symbol),
		zap.String("user_id", alert.UserID))
		
	alertJSON, err := json.Marshal(alert)
	if err != nil {
		logger.Log.Error("Failed to marshal alert", zap.Error(err))
		return
	}

	// Publish to Redis channel
	err = cache.PublishMessage(alertsChannel, string(alertJSON))
	if err != nil {
		logger.Log.Error("Failed to publish alert to Redis", zap.Error(err))
		return
	}

	logger.Log.Info("Alert published to Redis successfully", 
		zap.String("symbol", alert.Symbol))
}