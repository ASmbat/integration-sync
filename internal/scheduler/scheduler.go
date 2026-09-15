// Package scheduler runs registered connectors on independent intervals
// and records their results in a StatusStore, decoupling "when do we sync"
// from "how does each vendor sync."
package scheduler

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/ASmbat/integration-sync/internal/connector"
	"github.com/ASmbat/integration-sync/internal/store"
)

type job struct {
	conn     connector.Connector
	interval time.Duration
}

type Scheduler struct {
	statusStore store.StatusStore
	jobs        []job
	mu          sync.Mutex
}

func New(statusStore store.StatusStore) *Scheduler {
	return &Scheduler{statusStore: statusStore}
}

// Register adds a connector to be synced on the given interval. Call
// before Start; safe to call again later to add connectors dynamically.
func (s *Scheduler) Register(c connector.Connector, interval time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.jobs = append(s.jobs, job{conn: c, interval: interval})
}

// Start launches one goroutine per registered connector and runs an
// immediate sync before entering the ticker loop. It blocks until ctx is
// cancelled.
func (s *Scheduler) Start(ctx context.Context) {
	s.mu.Lock()
	jobs := append([]job(nil), s.jobs...)
	s.mu.Unlock()

	var wg sync.WaitGroup
	for _, j := range jobs {
		wg.Add(1)
		go func(j job) {
			defer wg.Done()
			s.runLoop(ctx, j)
		}(j)
	}
	wg.Wait()
}

func (s *Scheduler) runLoop(ctx context.Context, j job) {
	s.syncOnce(ctx, j.conn)

	ticker := time.NewTicker(j.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.syncOnce(ctx, j.conn)
		}
	}
}

// SyncNow triggers an immediate, out-of-cycle sync for a single connector
// (used by the manual "sync now" API endpoint).
func (s *Scheduler) SyncNow(ctx context.Context, id string) (connector.SyncResult, bool) {
	// Look up the connector under the lock, but release it before actually
	// running Sync. Sync is a network call to a third-party vendor that can
	// take seconds (or the full context timeout) to return; holding the
	// scheduler's mutex across that call would serialize manual syncs for
	// every connector behind whichever one happens to be slow or stuck,
	// even though connectors are otherwise fully independent.
	s.mu.Lock()
	var target connector.Connector
	for _, j := range s.jobs {
		if j.conn.ID() == id {
			target = j.conn
			break
		}
	}
	s.mu.Unlock()

	if target == nil {
		return connector.SyncResult{}, false
	}
	return s.syncOnce(ctx, target), true
}

func (s *Scheduler) syncOnce(ctx context.Context, c connector.Connector) connector.SyncResult {
	result, err := s.invoke(ctx, c)
	if err != nil {
		result = connector.SyncResult{
			Status:   connector.StatusError,
			Message:  err.Error(),
			SyncedAt: time.Now(),
		}
	}
	s.statusStore.Set(c.ID(), result)
	log.Printf("connector=%s status=%s records=%d msg=%q", c.ID(), result.Status, result.RecordsSynced, result.Message)
	return result
}

// invoke runs c.Sync, converting a panic into an error instead of letting
// it unwind out of this goroutine. Connectors wrap third-party vendor code
// (parsing whatever a vendor's API happens to return), and an unrecovered
// panic in any goroutine crashes the entire process — so without this, a
// bug triggered by one vendor's response would take down every other
// connector and the whole API, not just the one that misbehaved. That's
// exactly the isolation this package exists to provide.
func (s *Scheduler) invoke(ctx context.Context, c connector.Connector) (result connector.SyncResult, err error) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("scheduler: recovered panic in connector %q: %v", c.ID(), r)
			err = fmt.Errorf("connector panicked: %v", r)
		}
	}()
	return c.Sync(ctx)
}
