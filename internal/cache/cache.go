package cache

import (
	"context"
	"log"
	"pricenotification/internal/logger"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.uber.org/zap"
)

var RedisClient *redis.Client // Exported for redis_rate

var (
	cacheHitsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cache_hits_total",
			Help: "Total number of cache hits",
		},
		[]string{"endpoint", "instance"},
	)
	cacheMissesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "cache_misses_total",
			Help: "Total number of cache misses",
		},
		[]string{"endpoint", "instance"},
	)
)

func init() {
	prometheus.MustRegister(cacheHitsTotal)
	prometheus.MustRegister(cacheMissesTotal)
}

func InitRedis() {
	RedisClient = redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	_, err := RedisClient.Ping(context.Background()).Result()
	if err != nil {
		log.Fatalf("Failed to connect to Redis: %v", err)
	}
}

func GetCache(ctx context.Context, key string, endpoint, instance string) (string, error) {
	val, err := RedisClient.Get(ctx, key).Result()
	if err == redis.Nil {
		cacheMissesTotal.WithLabelValues(endpoint, instance).Inc()
		return "", nil
	}
	if err != nil {
		return "", err
	}
	cacheHitsTotal.WithLabelValues(endpoint, instance).Inc()
	return val, err
}

func SetCache(ctx context.Context, key, value string, ttl time.Duration, endpoint, instance string) error {
	return RedisClient.Set(ctx, key, value, ttl).Err()
}

func InvalidateByPrefix(ctx context.Context, prefix string, endpoint string, instance string) {
	tracer := otel.Tracer("real-time-notification")
	ctx, span := tracer.Start(ctx, "InvalidateByPrefix")
	defer span.End()

	// Get all keys matching the prefix
	keys, err := getAllKeys(ctx, prefix)
	if err != nil {
		logger.Log.Error("Failed to get cache keys for invalidation",
			zap.String("prefix", prefix),
			zap.String("endpoint", endpoint),
			zap.String("instance", instance),
			zap.Error(err),
		)
		return
	}

	// Count invalidated keys
	invalidatedCount := 0

	// Iterate through keys and delete those matching the prefix
	for _, key := range keys {
		if err := RedisClient.Del(ctx, key).Err(); err != nil {
			logger.Log.Warn("Failed to invalidate cache key",
				zap.String("key", key),
				zap.String("prefix", prefix),
				zap.String("endpoint", endpoint),
				zap.String("instance", instance),
				zap.Error(err),
			)
		} else {
			invalidatedCount++
		}
	}

	logger.Log.Info("Cache invalidation completed",
		zap.String("prefix", prefix),
		zap.String("endpoint", endpoint),
		zap.String("instance", instance),
		zap.Int("invalidated_keys", invalidatedCount),
	)
}

// Retrieve all keys matching a prefix from Redis
func getAllKeys(ctx context.Context, prefix string) ([]string, error) {
	var cursor uint64
	var keys []string
	for {
		// SCAN command with match filter for prefix
		foundKeys, nextCursor, err := RedisClient.Scan(ctx, cursor, prefix+"*", 1000).Result()
		if err != nil {
			return nil, err
		}

		keys = append(keys, foundKeys...)
		cursor = nextCursor

		// If cursor is 0, we've scanned everything
		if cursor == 0 {
			break
		}
	}
	return keys, nil
}
