package main

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"github.com/gorilla/websocket"
)

// Coinbase WebSocket URL for BTC-USD trades
const coinbaseWS = "wss://ws-feed.exchange.coinbase.com"

// Kafka broker details
const kafkaBroker = "localhost:9094"


// Coinbase WebSocket message format
type SubscriptionMessage struct {
	Type       string   `json:"type"`
	ProductIDs []string `json:"product_ids"`
	Channels   []string `json:"channels"`
}

// Trade message structure from Coinbase
type TradeMessage struct {
	Type      string `json:"type"`
	ProductID string `json:"product_id"`
	Price     string `json:"price"`
	Size      string `json:"size"`
	Time      string `json:"time"`
}

// Standardized price update format
type PriceUpdate struct {
	Exchange  string  `json:"exchange"`
	Symbol    string  `json:"symbol"`
	Price     float64 `json:"price"`
	Timestamp string  `json:"timestamp"`
}

// Kafka producer
func newKafkaProducer() *kafka.Producer {
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": kafkaBroker})
	if err != nil {
		log.Fatal("Failed to create Kafka producer:", err)
	}
	return p
}

// Publish message to Kafka
func publishToKafka(producer *kafka.Producer, priceData PriceUpdate) {
	value, err := json.Marshal(priceData)
	if err != nil {
		log.Println("Error marshaling JSON:", err)
		return
	}

	kafkaTopic := "price.updates"
	err = producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &kafkaTopic, Partition: kafka.PartitionAny},
		Value:          value,
	}, nil)

	if err != nil {
		log.Println("Error producing Kafka message:", err)
	} else {
		fmt.Println("Sent to Kafka:", string(value))
	}
}

// Connect to Coinbase WebSocket
func connectWebSocket() *websocket.Conn {
	var backoff = 1 * time.Second

	for {
		fmt.Println("Connecting to Coinbase WebSocket...")
		c, _, err := websocket.DefaultDialer.Dial(coinbaseWS, nil)
		if err != nil {
			log.Printf("WebSocket connection failed: %v. Retrying in %v...\n", err, backoff)
			time.Sleep(backoff)
			if backoff < 30*time.Second {
				backoff *= 2
			}
			continue
		}
		fmt.Println("Connected to Coinbase WebSocket!")
		return c
	}
}

func main() {
	producer := newKafkaProducer()
	defer producer.Close()

	for {
		c := connectWebSocket()
		defer c.Close()

		// Subscribe to BTC-USD trades
		subscribe := SubscriptionMessage{
			Type:       "subscribe",
			ProductIDs: []string{"BTC-USD"},
			Channels:   []string{"matches"},
		}
		if err := c.WriteJSON(subscribe); err != nil {
			log.Println("Subscription failed:", err)
			break
		}

		fmt.Println("Subscribed to BTC/USD trades.")

		// Read messages from WebSocket
		for {
			_, message, err := c.ReadMessage()
			if err != nil {
				log.Println("WebSocket error:", err)
				break
			}

			var trade TradeMessage
			if err := json.Unmarshal(message, &trade); err != nil {
				log.Println("Error parsing message:", err)
				continue
			}

			// Process only "match" messages (completed trades)
			if trade.Type == "match" {
				priceUpdate := PriceUpdate{
					Exchange:  "coinbase",
					Symbol:    trade.ProductID,
					Price:     parsePrice(trade.Price),
					Timestamp: trade.Time,
				}

				fmt.Printf("Trade: %s | Price: %.2f\n", priceUpdate.Symbol, priceUpdate.Price)

				// Publish trade data to Kafka
				publishToKafka(producer, priceUpdate)
			}
		}
	}
}

// Convert price string to float64
func parsePrice(priceStr string) float64 {
	var price float64
	json.Unmarshal([]byte(priceStr), &price)
	return price
}
