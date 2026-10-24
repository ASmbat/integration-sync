// Package scheduler runs registered connectors on independent intervals
// and records their results in a StatusStore. It decides *when* to sync;
// each connector decides *how*.
package scheduler

import (
	"context"
	"errors"
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
	running  sync.Mutex // one Sync at a time per connector
}

var (
	ErrDuplicateID     = errors.New("connector id already registered")
	ErrInvalidInterval = errors.New("interval must be positive")
	ErrAlreadyStarted  = errors.New("scheduler already started")
)

type Scheduler struct {
	statusStore store.StatusStore

	mu      sync.Mutex
	jobs    []*job
	ctx     context.Context // set while Start is running
	started bool
	stopped bool
	loops   sync.WaitGroup
}

func New(statusStore store.StatusStore) *Scheduler {
	return &Scheduler{statusStore: statusStore}
}

// Register adds a connector to sync every interval. It can be called before
// or after Start; a connector registered while the scheduler is running
// starts its loop right away. IDs must be unique.
func (s *Scheduler) Register(c connector.Connector, interval time.Duration) error {
	if interval <= 0 {
		return fmt.Errorf("%w: %s for %q", ErrInvalidInterval, interval, c.ID())
	}
	j := &job{conn: c, interval: interval}

	s.mu.Lock()
	defer s.mu.Unlock()
	for _, existing := range s.jobs {
		if existing.conn.ID() == c.ID() {
			return fmt.Errorf("%w: %q", ErrDuplicateID, c.ID())
		}
	}
	s.jobs = append(s.jobs, j)
	if s.started && !s.stopped {
		s.startLoop(s.ctx, j)
	}
	return nil
}

// Start runs one loop per connector, each doing an immediate sync and then
// syncing on its interval. It blocks until ctx is cancelled and all loops
// have returned. A Scheduler can be started only once.
func (s *Scheduler) Start(ctx context.Context) error {
	s.mu.Lock()
	if s.started {
		s.mu.Unlock()
		return ErrAlreadyStarted
	}
	s.started = true
	s.ctx = ctx
	for _, j := range s.jobs {
		s.startLoop(ctx, j)
	}
	s.mu.Unlock()

	<-ctx.Done()

	s.mu.Lock()
	s.stopped = true // no new loops after this point
	s.mu.Unlock()
	s.loops.Wait()
	return nil
}

// startLoop must be called with s.mu held.
func (s *Scheduler) startLoop(ctx context.Context, j *job) {
	s.loops.Add(1)
	go func() {
		defer s.loops.Done()
		s.runLoop(ctx, j)
	}()
}

func (s *Scheduler) runLoop(ctx context.Context, j *job) {
	s.syncOnce(ctx, j)

	ticker := time.NewTicker(j.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Skip this tick if a manual sync is still running, instead
			// of queueing up overlapping syncs against the vendor.
			if !j.running.TryLock() {
				continue
			}
			s.syncLocked(ctx, j)
			j.running.Unlock()
		}
	}
}

// SyncNow runs an immediate, out-of-cycle sync for one connector (used by
// the manual "sync now" endpoint). If a sync for that connector is already
// running, it waits for it to finish first.
func (s *Scheduler) SyncNow(ctx context.Context, id string) (connector.SyncResult, bool) {
	// Only the lookup holds s.mu. Sync can take seconds, and holding the
	// scheduler lock during it would block every other connector.
	s.mu.Lock()
	var target *job
	for _, j := range s.jobs {
		if j.conn.ID() == id {
			target = j
			break
		}
	}
	s.mu.Unlock()

	if target == nil {
		return connector.SyncResult{}, false
	}
	return s.syncOnce(ctx, target), true
}

func (s *Scheduler) syncOnce(ctx context.Context, j *job) connector.SyncResult {
	j.running.Lock()
	defer j.running.Unlock()
	return s.syncLocked(ctx, j)
}

// syncLocked runs the sync and stores the result. j.running must be held.
func (s *Scheduler) syncLocked(ctx context.Context, j *job) connector.SyncResult {
	result, err := invoke(ctx, j.conn)
	switch {
	case errors.Is(ctx.Err(), context.Canceled):
		// Shutdown: keep the last real status instead of overwriting it
		// with a "context canceled" error that says nothing about the vendor.
		log.Printf("connector=%s sync cancelled", j.conn.ID())
		return connector.SyncResult{Status: connector.StatusError, Message: "sync cancelled", SyncedAt: time.Now()}
	case errors.Is(ctx.Err(), context.DeadlineExceeded):
		// A timeout is real information about the vendor, so it is saved.
		result, err = connector.SyncResult{Status: connector.StatusError, Message: "sync timed out", SyncedAt: time.Now()}, nil
	}
	if err != nil {
		result = connector.SyncResult{
			Status:   connector.StatusError,
			Message:  err.Error(),
			SyncedAt: time.Now(),
		}
	}
	s.statusStore.Set(j.conn.ID(), result)
	log.Printf("connector=%s status=%s records=%d msg=%q", j.conn.ID(), result.Status, result.RecordsSynced, result.Message)
	return result
}

// invoke runs c.Sync and turns a panic into an error, so a bug in one
// vendor's connector can't crash the process and every other connector.
func invoke(ctx context.Context, c connector.Connector) (result connector.SyncResult, err error) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("scheduler: recovered panic in connector %q: %v", c.ID(), r)
			err = fmt.Errorf("connector panicked: %v", r)
		}
	}()
	return c.Sync(ctx)
}
