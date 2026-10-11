// Package domain holds VenuePulse entities shared across packages, free of
// storage and transport concerns.
package domain

import "time"

type Incident struct {
	ID          string    `json:"id"`
	VenueID     string    `json:"venue_id"`
	ZoneID      string    `json:"zone_id"`
	Severity    string    `json:"severity"`
	Category    string    `json:"category"`
	Status      string    `json:"status"`
	Description string    `json:"description"`
	ReportedAt  time.Time `json:"reported_at"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}
