package models

import (
	"time"
)

// Alert represents a price alert for a cryptocurrency
type Alert struct {
	ID             string     `json:"id" db:"id"`
	UserID         string     `json:"user_id" db:"user_id"`
	Symbol         string     `json:"symbol" db:"symbol"`
	UpperThreshold *float64   `json:"upper_threshold,omitempty" db:"upper_threshold"`
	LowerThreshold *float64   `json:"lower_threshold,omitempty" db:"lower_threshold"`
	CreatedAt      time.Time  `json:"created_at" db:"created_at"`
	UpdatedAt      time.Time  `json:"updated_at" db:"updated_at"`
}