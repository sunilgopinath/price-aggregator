package handlers

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"pricenotification/internal/cache"
	"pricenotification/internal/database"
	"pricenotification/internal/logger"
	"pricenotification/internal/models"

	"github.com/google/uuid"

	"go.opentelemetry.io/otel"
	"go.uber.org/zap"
)

type Response struct {
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

type CreateAlertRequest struct {
	UserID         string   `json:"user_id"`
	Symbol         string   `json:"symbol"`
	UpperThreshold *float64 `json:"upper_threshold,omitempty"`
	LowerThreshold *float64 `json:"lower_threshold,omitempty"`
}

type UpdateAlertRequest struct {
	Symbol         string   `json:"symbol,omitempty"`
	UpperThreshold *float64 `json:"upper_threshold,omitempty"`
	LowerThreshold *float64 `json:"lower_threshold,omitempty"`
}

// AlertsHandler handles all alert operations based on the HTTP method
func AlertsHandler(w http.ResponseWriter, r *http.Request, instance string) {
	// Extract ID from path if present (for GET, PUT, DELETE on specific alert)
	// URL pattern: /alerts/{id}
	path := r.URL.Path
	pathParts := strings.Split(path, "/")
	
	// Root alerts endpoint
	if len(pathParts) <= 2 || pathParts[2] == "" {
		// Handle collection endpoints
		switch r.Method {
		case http.MethodGet:
			BrowseAlertsHandler(w, r, instance)
		case http.MethodPost:
			CreateAlertHandler(w, r, instance)
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
		return
	}
	
	// Get alert ID from path
	alertID := pathParts[2]
	
	// Handle single alert endpoints
	switch r.Method {
	case http.MethodGet:
		GetAlertHandler(w, r, alertID, instance)
	case http.MethodPut, http.MethodPatch:
		UpdateAlertHandler(w, r, alertID, instance)
	case http.MethodDelete:
		DeleteAlertHandler(w, r, alertID, instance)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// BrowseAlertsHandler lists all alerts, optionally filtered by user_id or symbol
func BrowseAlertsHandler(w http.ResponseWriter, r *http.Request, instance string) {
	ctx := r.Context()
	tracer := otel.Tracer("real-time-notification")
	ctx, span := tracer.Start(ctx, "BrowseAlertsHandler")
	defer span.End()

	traceID := span.SpanContext().TraceID().String()
	cacheKey := generateCacheKey(r, "browse_alerts_")

	cached, err := cache.GetCache(ctx, cacheKey, "/alerts", instance)
	if err == nil && cached != "" {
		logger.Log.Info("Cache hit for /alerts",
			zap.String("trace_id", traceID),
			zap.String("cache_key", cacheKey),
		)
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(cached))
		return
	}

	logger.Log.Info("Cache miss for /alerts, processing request",
		zap.String("trace_id", traceID),
		zap.String("cache_key", cacheKey),
	)

	// Get query parameters
	userID := r.URL.Query().Get("user_id")
	symbol := r.URL.Query().Get("symbol")

	var alerts []*models.Alert
	var dbErr error

	if userID != "" {
		alerts, dbErr = database.GetAlertsByUserID(ctx, userID)
	} else if symbol != "" {
		alerts, dbErr = database.GetAlertsBySymbol(ctx, symbol)
	} else {
		alerts, dbErr = database.GetAllAlerts(ctx)
	}

	if dbErr != nil {
		logger.Log.Error("Failed to fetch alerts",
			zap.String("trace_id", traceID),
			zap.Error(dbErr),
		)
		http.Error(w, "Failed to fetch alerts", http.StatusInternalServerError)
		return
	}

	response := Response{
		Message: "Alerts retrieved successfully",
		Data:    alerts,
	}
	
	respBytes, err := json.Marshal(response)
	if err != nil {
		logger.Log.Error("Failed to encode JSON response",
			zap.String("trace_id", traceID),
			zap.Error(err),
		)
		http.Error(w, "Failed to encode JSON response", http.StatusInternalServerError)
		return
	}

	if cacheErr := cache.SetCache(ctx, cacheKey, string(respBytes), 30*time.Second, "/alerts", instance); cacheErr != nil {
		logger.Log.Warn("Failed to store response in cache",
			zap.String("trace_id", traceID),
			zap.String("cache_key", cacheKey),
			zap.Error(cacheErr),
		)
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(respBytes)
}

// CreateAlertHandler handles creating a new alert
func CreateAlertHandler(w http.ResponseWriter, r *http.Request, instance string) {
	ctx := r.Context()
	tracer := otel.Tracer("real-time-notification")
	ctx, span := tracer.Start(ctx, "CreateAlertHandler")
	defer span.End()

	traceID := span.SpanContext().TraceID().String()

	var req CreateAlertRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		logger.Log.Error("Failed to parse request body",
			zap.String("trace_id", traceID),
			zap.Error(err),
		)
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// Validation
	if req.UserID == "" || req.Symbol == "" {
		logger.Log.Error("Missing required fields",
			zap.String("trace_id", traceID),
		)
		http.Error(w, "Missing required fields: user_id, symbol", http.StatusBadRequest)
		return
	}

	if req.UpperThreshold == nil && req.LowerThreshold == nil {
		logger.Log.Error("At least one threshold must be specified",
			zap.String("trace_id", traceID),
		)
		http.Error(w, "At least one threshold (upper or lower) must be specified", http.StatusBadRequest)
		return
	}

	// Create the alert
	now := time.Now()
	alert := &models.Alert{
		ID:             uuid.New().String(),
		UserID:         req.UserID,
		Symbol:         req.Symbol,
		UpperThreshold: req.UpperThreshold,
		LowerThreshold: req.LowerThreshold,
		CreatedAt:      now,
		UpdatedAt:      now,
	}

	if err := database.CreateAlert(ctx, alert); err != nil {
		logger.Log.Error("Failed to create alert",
			zap.String("trace_id", traceID),
			zap.Error(err),
		)
		http.Error(w, "Failed to create alert", http.StatusInternalServerError)
		return
	}

	// Invalidate cache for browse alerts
	cache.InvalidateByPrefix(ctx, "browse_alerts_", "/alerts", instance)

	response := Response{
		Message: "Alert created successfully",
		Data:    alert,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(response)
}

// GetAlertHandler retrieves a specific alert by ID
func GetAlertHandler(w http.ResponseWriter, r *http.Request, alertID string, instance string) {
	ctx := r.Context()
	tracer := otel.Tracer("real-time-notification")
	ctx, span := tracer.Start(ctx, "GetAlertHandler")
	defer span.End()

	traceID := span.SpanContext().TraceID().String()

	alert, err := database.GetAlertByID(ctx, alertID)
	if err != nil {
		logger.Log.Error("Failed to fetch alert",
			zap.String("trace_id", traceID),
			zap.String("alert_id", alertID),
			zap.Error(err),
		)
		http.Error(w, "Alert not found", http.StatusNotFound)
		return
	}

	response := Response{
		Message: "Alert retrieved successfully",
		Data:    alert,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// UpdateAlertHandler updates an existing alert
func UpdateAlertHandler(w http.ResponseWriter, r *http.Request, alertID string, instance string) {
	ctx := r.Context()
	tracer := otel.Tracer("real-time-notification")
	ctx, span := tracer.Start(ctx, "UpdateAlertHandler")
	defer span.End()

	traceID := span.SpanContext().TraceID().String()

	// Get the existing alert
	existingAlert, err := database.GetAlertByID(ctx, alertID)
	if err != nil {
		logger.Log.Error("Failed to fetch alert for update",
			zap.String("trace_id", traceID),
			zap.String("alert_id", alertID),
			zap.Error(err),
		)
		http.Error(w, "Alert not found", http.StatusNotFound)
		return
	}

	// Parse the update request
	var req UpdateAlertRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		logger.Log.Error("Failed to parse request body",
			zap.String("trace_id", traceID),
			zap.Error(err),
		)
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	// Update fields if provided
	if req.Symbol != "" {
		existingAlert.Symbol = req.Symbol
	}
	
	if req.UpperThreshold != nil {
		existingAlert.UpperThreshold = req.UpperThreshold
	}
	
	if req.LowerThreshold != nil {
		existingAlert.LowerThreshold = req.LowerThreshold
	}

	// Ensure at least one threshold is set
	if existingAlert.UpperThreshold == nil && existingAlert.LowerThreshold == nil {
		logger.Log.Error("At least one threshold must be specified",
			zap.String("trace_id", traceID),
		)
		http.Error(w, "At least one threshold (upper or lower) must be specified", http.StatusBadRequest)
		return
	}

	existingAlert.UpdatedAt = time.Now()

	// Save the updated alert
	if err := database.UpdateAlert(ctx, existingAlert); err != nil {
		logger.Log.Error("Failed to update alert",
			zap.String("trace_id", traceID),
			zap.String("alert_id", alertID),
			zap.Error(err),
		)
		http.Error(w, "Failed to update alert", http.StatusInternalServerError)
		return
	}

	// Invalidate cache for browse alerts
	cache.InvalidateByPrefix(ctx, "browse_alerts_", "/alerts", instance)

	response := Response{
		Message: "Alert updated successfully",
		Data:    existingAlert,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

// DeleteAlertHandler deletes an alert
func DeleteAlertHandler(w http.ResponseWriter, r *http.Request, alertID string, instance string) {
	ctx := r.Context()
	tracer := otel.Tracer("real-time-notification")
	ctx, span := tracer.Start(ctx, "DeleteAlertHandler")
	defer span.End()

	traceID := span.SpanContext().TraceID().String()

	if err := database.DeleteAlert(ctx, alertID); err != nil {
		logger.Log.Error("Failed to delete alert",
			zap.String("trace_id", traceID),
			zap.String("alert_id", alertID),
			zap.Error(err),
		)
		http.Error(w, "Failed to delete alert", http.StatusInternalServerError)
		return
	}

	// Invalidate cache for browse alerts
	cache.InvalidateByPrefix(ctx, "browse_alerts_", "/alerts", instance)

	response := Response{
		Message: "Alert deleted successfully",
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func generateCacheKey(r *http.Request, prefix string) string {
	queryParams := r.URL.Query()
	var keys []string
	for k := range queryParams {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var queryString []string
	for _, k := range keys {
		queryString = append(queryString, fmt.Sprintf("%s=%s", k, strings.Join(queryParams[k], ",")))
	}
	joinedParams := strings.Join(queryString, "&")

	hash := sha256.Sum256([]byte(joinedParams))
	return prefix + hex.EncodeToString(hash[:8])
}