package database

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"pricenotification/internal/logger"
	"pricenotification/internal/models"

	_ "github.com/lib/pq"
	"go.uber.org/zap"
)

var db *sql.DB

// InitDB initializes the database connection
func InitDB(connStr string) error {
	var err error
	db, err = sql.Open("postgres", connStr)
	if err != nil {
		return err
	}

	// Set connection pool parameters
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	// Test the connection
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	
	if err := db.PingContext(ctx); err != nil {
		return err
	}

	logger.Log.Info("Database connection established")
	return nil
}

// CreateAlert inserts a new alert into the database
func CreateAlert(ctx context.Context, alert *models.Alert) error {
	query := `
		INSERT INTO alerts (id, user_id, symbol, upper_threshold, lower_threshold, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`
	
	_, err := db.ExecContext(
		ctx,
		query,
		alert.ID,
		alert.UserID,
		alert.Symbol,
		alert.UpperThreshold,
		alert.LowerThreshold,
		alert.CreatedAt,
		alert.UpdatedAt,
	)
	
	if err != nil {
		logger.Log.Error("Failed to create alert in database", 
			zap.String("alert_id", alert.ID),
			zap.Error(err),
		)
		return err
	}
	
	return nil
}

// GetAlertByID retrieves an alert by its ID
func GetAlertByID(ctx context.Context, id string) (*models.Alert, error) {
	query := `
		SELECT id, user_id, symbol, upper_threshold, lower_threshold, created_at, updated_at
		FROM alerts
		WHERE id = $1
	`
	
	var alert models.Alert
	var upperThreshold, lowerThreshold sql.NullFloat64
	
	err := db.QueryRowContext(ctx, query, id).Scan(
		&alert.ID,
		&alert.UserID,
		&alert.Symbol,
		&upperThreshold,
		&lowerThreshold,
		&alert.CreatedAt,
		&alert.UpdatedAt,
	)
	
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, errors.New("alert not found")
		}
		logger.Log.Error("Failed to retrieve alert", 
			zap.String("alert_id", id),
			zap.Error(err),
		)
		return nil, err
	}
	
	// Convert nullable fields
	if upperThreshold.Valid {
		val := upperThreshold.Float64
		alert.UpperThreshold = &val
	}
	
	if lowerThreshold.Valid {
		val := lowerThreshold.Float64
		alert.LowerThreshold = &val
	}
	
	return &alert, nil
}

// GetAlertsByUserID retrieves all alerts for a specific user
func GetAlertsByUserID(ctx context.Context, userID string) ([]*models.Alert, error) {
	query := `
		SELECT id, user_id, symbol, upper_threshold, lower_threshold, created_at, updated_at
		FROM alerts
		WHERE user_id = $1
		ORDER BY created_at DESC
	`
	
	rows, err := db.QueryContext(ctx, query, userID)
	if err != nil {
		logger.Log.Error("Failed to query alerts by user ID", 
			zap.String("user_id", userID),
			zap.Error(err),
		)
		return nil, err
	}
	defer rows.Close()
	
	return scanAlerts(rows)
}

// GetAlertsBySymbol retrieves all alerts for a specific crypto symbol
func GetAlertsBySymbol(ctx context.Context, symbol string) ([]*models.Alert, error) {
	query := `
		SELECT id, user_id, symbol, upper_threshold, lower_threshold, created_at, updated_at
		FROM alerts
		WHERE symbol = $1
		ORDER BY created_at DESC
	`
	
	rows, err := db.QueryContext(ctx, query, symbol)
	if err != nil {
		logger.Log.Error("Failed to query alerts by symbol", 
			zap.String("symbol", symbol),
			zap.Error(err),
		)
		return nil, err
	}
	defer rows.Close()
	
	return scanAlerts(rows)
}

// GetAllAlerts retrieves all alerts
func GetAllAlerts(ctx context.Context) ([]*models.Alert, error) {
	query := `
		SELECT id, user_id, symbol, upper_threshold, lower_threshold, created_at, updated_at
		FROM alerts
		ORDER BY created_at DESC
	`
	
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		logger.Log.Error("Failed to query all alerts", zap.Error(err))
		return nil, err
	}
	defer rows.Close()
	
	return scanAlerts(rows)
}

// UpdateAlert updates an existing alert
func UpdateAlert(ctx context.Context, alert *models.Alert) error {
	query := `
		UPDATE alerts
		SET symbol = $1, upper_threshold = $2, lower_threshold = $3, updated_at = $4
		WHERE id = $5
	`
	
	_, err := db.ExecContext(
		ctx,
		query,
		alert.Symbol,
		alert.UpperThreshold,
		alert.LowerThreshold,
		alert.UpdatedAt,
		alert.ID,
	)
	
	if err != nil {
		logger.Log.Error("Failed to update alert", 
			zap.String("alert_id", alert.ID),
			zap.Error(err),
		)
		return err
	}
	
	return nil
}

// DeleteAlert deletes an alert by ID
func DeleteAlert(ctx context.Context, id string) error {
	query := `DELETE FROM alerts WHERE id = $1`
	
	result, err := db.ExecContext(ctx, query, id)
	if err != nil {
		logger.Log.Error("Failed to delete alert", 
			zap.String("alert_id", id),
			zap.Error(err),
		)
		return err
	}
	
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	
	if rowsAffected == 0 {
		return errors.New("alert not found")
	}
	
	return nil
}

// Helper function to scan alert rows
func scanAlerts(rows *sql.Rows) ([]*models.Alert, error) {
	var alerts []*models.Alert
	
	for rows.Next() {
		var alert models.Alert
		var upperThreshold, lowerThreshold sql.NullFloat64
		
		err := rows.Scan(
			&alert.ID,
			&alert.UserID,
			&alert.Symbol,
			&upperThreshold,
			&lowerThreshold,
			&alert.CreatedAt,
			&alert.UpdatedAt,
		)
		
		if err != nil {
			return nil, err
		}
		
		// Convert nullable fields
		if upperThreshold.Valid {
			val := upperThreshold.Float64
			alert.UpperThreshold = &val
		}
		
		if lowerThreshold.Valid {
			val := lowerThreshold.Float64
			alert.LowerThreshold = &val
		}
		
		alerts = append(alerts, &alert)
	}
	
	if err := rows.Err(); err != nil {
		return nil, err
	}
	
	return alerts, nil
}

