// internal/cache/pubsub.go
package cache

import (
	"context"
	"pricenotification/internal/logger"

	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

// PublishMessage publishes a message to a Redis channel
func PublishMessage(channel string, message string) error {
	ctx := context.Background()
	return RedisClient.Publish(ctx, channel, message).Err()
}

// RedisSubscriber represents a subscription to a Redis channel
type RedisSubscriber struct {
	pubsub *redis.PubSub
}

// NewRedisSubscriber creates a new Redis subscriber
func NewRedisSubscriber(channel string) (*RedisSubscriber, error) {
	ctx := context.Background()
	pubsub := RedisClient.Subscribe(ctx, channel)
	
	// Confirm subscription
	_, err := pubsub.Receive(ctx)
	if err != nil {
		return nil, err
	}
	
	logger.Log.Info("Subscribed to Redis channel", zap.String("channel", channel))
	return &RedisSubscriber{pubsub: pubsub}, nil
}

// ReceiveMessage waits for and returns the next message
func (s *RedisSubscriber) ReceiveMessage(ctx context.Context) (*redis.Message, error) {
	return s.pubsub.ReceiveMessage(ctx)
}

// Close closes the subscription
func (s *RedisSubscriber) Close() error {
	return s.pubsub.Close()
}