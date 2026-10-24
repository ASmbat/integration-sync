// Package connector defines the pluggable interface that every third-party
// integration implements. The goal is that adding a new vendor is a matter
// of implementing this one interface, not touching scheduling, status
// tracking or the API layer.
package connector

import (
	"context"
	"time"
)

// Status describes the outcome of a sync attempt.
type Status string

const (
	StatusOK          Status = "ok"
	StatusError       Status = "error"
	StatusRateLimited Status = "rate_limited"
	StatusNeverRun    Status = "never_run"
)

// SyncResult is what a Connector reports back after a sync attempt.
type SyncResult struct {
	Status        Status    `json:"status"`
	RecordsSynced int       `json:"records_synced"`
	Message       string    `json:"message,omitempty"`
	SyncedAt      time.Time `json:"synced_at"`
}

// Connector is the seam every integration implements. Real connectors wrap
// a specific third-party API (auth, pagination, rate limits); this
// interface is what the rest of the system depends on, so the scheduler,
// status store and HTTP layer never need to know about individual vendors.
type Connector interface {
	// ID is a stable, unique identifier for this connector (e.g. "github").
	ID() string
	// DisplayName is a human-readable name shown in the dashboard.
	DisplayName() string
	// Sync pulls data from the third-party API. It
	// should be safe to call repeatedly and cheap to retry after failure.
	Sync(ctx context.Context) (SyncResult, error)
}
