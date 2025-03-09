package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"strings"

	"pricenotification/internal/cache"
	"pricenotification/internal/database"
	"pricenotification/internal/handlers"
	"pricenotification/internal/logger"
	"pricenotification/internal/tracing"

	"go.uber.org/zap"
)

func main() {
	port := flag.String("port", "8081", "Port for alerts service")
	instance := flag.String("instance", "gateway-1", "Instance ID for this server")
	dbConn := flag.String("db", "postgres://alertsuser:alertspassword@localhost:5432/alertsdb?sslmode=disable", "Database connection string")
	flag.Parse()

	logger.InitLogger()

	// Initialize Redis
	cache.InitRedis()

	// Initialize database connection
	if err := database.InitDB(*dbConn); err != nil {
		logger.Log.Fatal("Failed to initialize database", zap.Error(err))
	}

	// Initialize SSE system - important addition
	handlers.InitSSE()

	shutdown, err := tracing.InitTracer()
	if err != nil {
		logger.Log.Fatal("Failed to initialize tracer", zap.Error(err))
	}
	defer func() {
		ctx := context.Background()
		if err := shutdown(ctx); err != nil {
			logger.Log.Error("Failed to shutdown tracer", zap.Error(err))
		}
	}()

	// Setup routes
	mux := http.NewServeMux()

	// SSE Endpoint for real-time alerts
	mux.HandleFunc("/alerts/stream", handlers.StreamAlertsHandler)

	fs := http.FileServer(http.Dir("./frontend"))
	mux.Handle("/", fs)
	
	// Handler for all alert operations
	mux.HandleFunc("/alerts", func(w http.ResponseWriter, r *http.Request) {
		// Handle root path or paths with ID
		handlers.AlertsHandler(w, r, *instance)
	})
	
	// Handler for alert operations with ID
	mux.HandleFunc("/alerts/", func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/alerts/") {
			handlers.AlertsHandler(w, r, *instance)
		} else {
			http.NotFound(w, r)
		}
	})

	logger.Log.Info("Alerts service starting on", zap.String("port", *port))
	log.Fatal(http.ListenAndServe(":"+*port, mux))
}