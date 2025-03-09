package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"pricenotification/internal/cache"
	"pricenotification/internal/database"
	"pricenotification/internal/handlers"
	"pricenotification/internal/logger"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// Price update structure (from Kafka)
type PriceUpdate struct {
	Exchange  string  `json:"exchange"`
	Symbol    string  `json:"symbol"`
	Price     float64 `json:"price"`
	Timestamp string  `json:"timestamp"`
}

func main() {
	logger.InitLogger()
	
	// Initialize Redis - important addition
	cache.InitRedis()
	
	// Initialize database (reusing internal/database)
	err := database.InitDB("postgres://alertsuser:alertspassword@localhost:5432/alertsdb?sslmode=disable")
	if err != nil {
		log.Fatal("❌ Database connection failed:", err)
	}

	// Create Kafka consumer
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": "localhost:9094",
		"group.id":          "price-processing-group",
		"auto.offset.reset": "earliest",
	})
	if err != nil {
		log.Fatal("❌ Failed to create Kafka consumer:", err)
	}
	defer consumer.Close()

	// Subscribe to price updates
	err = consumer.Subscribe("price.updates", nil)
	if err != nil {
		log.Fatal("❌ Failed to subscribe to Kafka topic:", err)
	}

	fmt.Println("✅ Listening for price updates...")

	// Consume messages
	for {
		msg, err := consumer.ReadMessage(-1)
		if err != nil {
			fmt.Println("Kafka consumer error:", err)
			continue
		}

		// Parse price update message
		var priceUpdate PriceUpdate
		if err := json.Unmarshal(msg.Value, &priceUpdate); err != nil {
			log.Println("❌ Error parsing price update:", err)
			continue
		}

		fmt.Printf("📌 Received price update: %+v\n", priceUpdate)

		// Check if the price matches any alerts
		processPriceUpdate(priceUpdate)
	}
}

// Map to track last triggered alerts (symbol → last notified timestamp)
var lastAlertTime = make(map[string]time.Time)

// Cooldown duration before re-triggering the same alert
const cooldown = 30 * time.Second // Adjust as needed

func processPriceUpdate(priceUpdate PriceUpdate) {
	ctx := context.Background()
	alerts, err := database.GetAlertsBySymbol(ctx, priceUpdate.Symbol)
	if err != nil {
		log.Println("❌ Failed to fetch alerts:", err)
		return
	}

	for _, alert := range alerts {
		triggered := false
		alertKey := fmt.Sprintf("%s_%s", alert.UserID, alert.Symbol) // Unique key per user-symbol alert

		// Enforce cooldown: Don't send the same alert too frequently
		if lastTime, exists := lastAlertTime[alertKey]; exists {
			if time.Since(lastTime) < cooldown {
				fmt.Printf("⏳ Alert suppressed for %s (cooldown active)\n", alertKey)
				continue
			}
		}

		if alert.LowerThreshold != nil && priceUpdate.Price <= *alert.LowerThreshold {
			triggered = true
			sendSSEAlert(alert.UserID, priceUpdate.Symbol, *alert.LowerThreshold, "below")
		}

		if alert.UpperThreshold != nil && priceUpdate.Price >= *alert.UpperThreshold {
			triggered = true
			sendSSEAlert(alert.UserID, priceUpdate.Symbol, *alert.UpperThreshold, "above")
		}

		if triggered {
			// Update the last triggered time to prevent duplicates
			lastAlertTime[alertKey] = time.Now()

			// TODO: Send notification (Email/SMS/WebSocket)
			fmt.Println("📌 Notification to be sent!")
		}
	}
}

// Sends alert to SSE clients
func sendSSEAlert(userID, symbol string, threshold float64, triggered string) {
	alert := handlers.AlertMessage{
		UserID:    userID,
		Symbol:    symbol,
		Threshold: threshold,
		Triggered: triggered,
		Timestamp: time.Now().Format(time.RFC3339),
	}

	// Debug log to confirm alert is being sent
	fmt.Printf("🚀 Triggering SSE Alert: %+v\n", alert)

	// This now publishes to Redis, which will be picked up by the web server
	handlers.BroadcastAlert(alert)
}