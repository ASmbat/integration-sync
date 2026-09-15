package connector

import (
	"context"
	"sync/atomic"
	"time"
)

// MockConnector simulates a flaky third-party vendor: it fails on a
// configurable cadence so the scheduler's retry/backoff behaviour can be
// exercised without depending on a real, rate-limited API. Useful both as
// a demo connector and as a test double.
type MockConnector struct {
	Name       string
	FailEveryN int // 0 disables simulated failures
	calls      int64
}

func NewMockConnector(name string, failEveryN int) *MockConnector {
	return &MockConnector{Name: name, FailEveryN: failEveryN}
}

func (c *MockConnector) ID() string          { return c.Name }
func (c *MockConnector) DisplayName() string { return c.Name }

func (c *MockConnector) Sync(ctx context.Context) (SyncResult, error) {
	n := atomic.AddInt64(&c.calls, 1)

	if c.FailEveryN > 0 && n%int64(c.FailEveryN) == 0 {
		return SyncResult{
			Status:   StatusRateLimited,
			Message:  "simulated rate limit",
			SyncedAt: time.Now(),
		}, nil
	}

	return SyncResult{
		Status:        StatusOK,
		RecordsSynced: int(n) * 7, // arbitrary but deterministic
		SyncedAt:      time.Now(),
	}, nil
}
