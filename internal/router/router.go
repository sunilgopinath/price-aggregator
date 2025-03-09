package router

import (
	"io"
	"log"
	"net"
	"net/http"
	"time"

	"pricenotification/internal/cache"

	"github.com/go-redis/redis_rate/v10"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var limiter *redis_rate.Limiter

func rateLimit(next http.Handler, instance string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		clientIP, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			log.Printf("Failed to parse client IP: %v", err)
			clientIP = r.RemoteAddr
		}
		key := "ratelimit:" + clientIP + ":" + instance
		limit := redis_rate.Limit{
			Rate:   10,
			Burst:  10,
			Period: time.Minute,
		}
		res, err := limiter.Allow(r.Context(), key, limit)
		if err != nil {
			log.Printf("Rate limit error: %v", err)
			http.Error(w, "Internal Server Error", http.StatusInternalServerError)
			return
		}
		log.Printf("Rate limit check: key=%s, Allowed=%d, Remaining=%d", key, res.Allowed, res.Remaining)
		if res.Allowed == 0 {
			http.Error(w, "Too Many Requests", http.StatusTooManyRequests)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func SetupRoutes(instance string) *http.ServeMux {
	if limiter == nil {
		limiter = redis_rate.NewLimiter(cache.RedisClient)
	}

	mux := http.NewServeMux()
	mux.Handle("/events", rateLimit(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		proxy(w, r, "http://localhost:8081/events")
	}), instance))
	mux.Handle("/metrics", promhttp.Handler())
	return mux
}

func proxy(w http.ResponseWriter, r *http.Request, url string) {
	req, err := http.NewRequest(r.Method, url, r.Body)
	if err != nil {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		http.Error(w, "Service unavailable", http.StatusServiceUnavailable)
		return
	}
	defer resp.Body.Close()
	for k, v := range resp.Header {
		w.Header()[k] = v
	}
	w.WriteHeader(resp.StatusCode)
	io.Copy(w, resp.Body)
}