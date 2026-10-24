package scheduler

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ASmbat/integration-sync/internal/connector"
	"github.com/ASmbat/integration-sync/internal/store"
)

// notifyStore wraps the memory store and reports every Set on a channel,
// so tests can wait for a sync without time.Sleep.
type notifyStore struct {
	*store.MemoryStatusStore
	sets chan string
}

func newNotifyStore() *notifyStore {
	return &notifyStore{MemoryStatusStore: store.NewMemoryStatusStore(), sets: make(chan string, 100)}
}

func (s *notifyStore) Set(id string, r connector.SyncResult) {
	s.MemoryStatusStore.Set(id, r)
	s.sets <- id
}

func (s *notifyStore) waitFor(t *testing.T, id string) {
	t.Helper()
	timeout := time.After(time.Second)
	for {
		select {
		case got := <-s.sets:
			if got == id {
				return
			}
		case <-timeout:
			t.Fatalf("timed out waiting for a sync of %q", id)
		}
	}
}

// blockingConnector signals on started when Sync begins and returns only
// after release is closed. It also records the highest number of Sync
// calls that ran at the same time.
type blockingConnector struct {
	id         string
	started    chan struct{}
	release    chan struct{}
	active     int64
	maxActive  int64
	totalCalls int64
}

func newBlockingConnector(id string) *blockingConnector {
	return &blockingConnector{id: id, started: make(chan struct{}, 10), release: make(chan struct{})}
}

func (c *blockingConnector) ID() string          { return c.id }
func (c *blockingConnector) DisplayName() string { return c.id }
func (c *blockingConnector) Sync(ctx context.Context) (connector.SyncResult, error) {
	n := atomic.AddInt64(&c.active, 1)
	defer atomic.AddInt64(&c.active, -1)
	atomic.AddInt64(&c.totalCalls, 1)
	for {
		m := atomic.LoadInt64(&c.maxActive)
		if n <= m || atomic.CompareAndSwapInt64(&c.maxActive, m, n) {
			break
		}
	}
	c.started <- struct{}{}
	<-c.release
	return connector.SyncResult{Status: connector.StatusOK, SyncedAt: time.Now()}, nil
}

type panicConnector struct{ id string }

func (c *panicConnector) ID() string          { return c.id }
func (c *panicConnector) DisplayName() string { return c.id }
func (c *panicConnector) Sync(context.Context) (connector.SyncResult, error) {
	panic("unexpected vendor response")
}

func mustRegister(t *testing.T, s *Scheduler, c connector.Connector, interval time.Duration) {
	t.Helper()
	if err := s.Register(c, interval); err != nil {
		t.Fatalf("register %q: %v", c.ID(), err)
	}
}

func TestScheduler_RegisterValidates(t *testing.T) {
	sched := New(store.NewMemoryStatusStore())
	if err := sched.Register(connector.NewMockConnector("a", 0), 0); !errors.Is(err, ErrInvalidInterval) {
		t.Errorf("zero interval (would panic in time.NewTicker): got %v", err)
	}
	mustRegister(t, sched, connector.NewMockConnector("a", 0), time.Hour)
	if err := sched.Register(connector.NewMockConnector("a", 0), time.Hour); !errors.Is(err, ErrDuplicateID) {
		t.Errorf("duplicate id: got %v", err)
	}
}

func TestScheduler_StartTwiceFails(t *testing.T) {
	st := newNotifyStore()
	sched := New(st)
	mustRegister(t, sched, connector.NewMockConnector("demo", 0), time.Hour)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = sched.Start(ctx); close(done) }()
	st.waitFor(t, "demo") // the first Start is running

	if err := sched.Start(ctx); !errors.Is(err, ErrAlreadyStarted) {
		t.Fatalf("expected ErrAlreadyStarted, got %v", err)
	}
	cancel()
	<-done
}

// ctxConnector returns a known-good result, unless its context is done.
type ctxConnector struct {
	id      string
	started chan struct{}
}

func (c *ctxConnector) ID() string          { return c.id }
func (c *ctxConnector) DisplayName() string { return c.id }
func (c *ctxConnector) Sync(ctx context.Context) (connector.SyncResult, error) {
	if c.started != nil {
		c.started <- struct{}{}
		<-ctx.Done()
		return connector.SyncResult{}, ctx.Err()
	}
	return connector.SyncResult{Status: connector.StatusOK, RecordsSynced: 5, SyncedAt: time.Now()}, nil
}

func TestScheduler_CancelledSyncKeepsLastStatus(t *testing.T) {
	st := store.NewMemoryStatusStore()
	sched := New(st)
	c := &ctxConnector{id: "v"}
	mustRegister(t, sched, c, time.Hour)

	sched.SyncNow(context.Background(), "v") // saves a good status

	c.started = make(chan struct{}, 1)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { sched.SyncNow(ctx, "v"); close(done) }()
	<-c.started
	cancel() // e.g. shutdown
	<-done

	if r, _ := st.Get("v"); r.Status != connector.StatusOK || r.RecordsSynced != 5 {
		t.Fatalf("cancelled sync overwrote the last good status: %+v", r)
	}
}

func TestScheduler_TimedOutSyncIsRecorded(t *testing.T) {
	st := store.NewMemoryStatusStore()
	sched := New(st)
	mustRegister(t, sched, &ctxConnector{id: "v", started: make(chan struct{}, 1)}, time.Hour)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	sched.SyncNow(ctx, "v")

	if r, _ := st.Get("v"); r.Status != connector.StatusError || r.Message != "sync timed out" {
		t.Fatalf("expected a saved timeout error, got %+v", r)
	}
}

func TestScheduler_RunsImmediateSyncOnStart(t *testing.T) {
	st := newNotifyStore()
	sched := New(st)
	mustRegister(t, sched, connector.NewMockConnector("demo", 0), time.Hour)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = sched.Start(ctx) }()

	st.waitFor(t, "demo")
	if r, _ := st.Get("demo"); r.Status != connector.StatusOK {
		t.Errorf("expected StatusOK, got %s", r.Status)
	}
}

func TestScheduler_RegisterAfterStartRuns(t *testing.T) {
	st := newNotifyStore()
	sched := New(st)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = sched.Start(ctx) }()

	mustRegister(t, sched, connector.NewMockConnector("late", 0), time.Hour)
	st.waitFor(t, "late") // would time out if late registrations were ignored
}

func TestScheduler_StartReturnsAfterCancel(t *testing.T) {
	sched := New(store.NewMemoryStatusStore())
	mustRegister(t, sched, connector.NewMockConnector("demo", 0), time.Hour)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = sched.Start(ctx); close(done) }()
	cancel()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Start did not return after its context was cancelled")
	}
}

func TestScheduler_SyncNow(t *testing.T) {
	st := store.NewMemoryStatusStore()
	sched := New(st)
	mustRegister(t, sched, connector.NewMockConnector("demo", 0), time.Hour)

	result, found := sched.SyncNow(context.Background(), "demo")
	if !found || result.Status != connector.StatusOK {
		t.Fatalf("got found=%v result=%+v", found, result)
	}
	if stored, ok := st.Get("demo"); !ok || stored.RecordsSynced != result.RecordsSynced {
		t.Error("expected SyncNow result to be saved in the store")
	}
}

func TestScheduler_SyncNow_UnknownConnector(t *testing.T) {
	sched := New(store.NewMemoryStatusStore())
	if _, found := sched.SyncNow(context.Background(), "does-not-exist"); found {
		t.Error("expected found=false for an unregistered connector")
	}
}

func TestScheduler_SyncNow_DoesNotWaitForOtherConnectors(t *testing.T) {
	sched := New(store.NewMemoryStatusStore())
	slow := newBlockingConnector("slow")
	defer close(slow.release)
	mustRegister(t, sched, slow, time.Hour)
	mustRegister(t, sched, connector.NewMockConnector("fast", 0), time.Hour)

	go sched.SyncNow(context.Background(), "slow")
	<-slow.started // the slow sync is now in progress

	done := make(chan struct{})
	go func() { sched.SyncNow(context.Background(), "fast"); close(done) }()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("SyncNow for the fast connector is blocked behind the slow one")
	}
}

func TestScheduler_NoOverlappingSyncsForSameConnector(t *testing.T) {
	sched := New(store.NewMemoryStatusStore())
	c := newBlockingConnector("vendor")
	mustRegister(t, sched, c, time.Hour)

	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); sched.SyncNow(context.Background(), "vendor") }()
	}
	for i := 0; i < 3; i++ {
		<-c.started
		c.release <- struct{}{} // let one sync finish at a time
	}
	wg.Wait()

	if got := atomic.LoadInt64(&c.maxActive); got != 1 {
		t.Fatalf("expected at most 1 concurrent Sync, saw %d", got)
	}
	if got := atomic.LoadInt64(&c.totalCalls); got != 3 {
		t.Fatalf("expected 3 syncs, got %d", got)
	}
}

func TestScheduler_SyncNow_RecoversConnectorPanic(t *testing.T) {
	st := store.NewMemoryStatusStore()
	sched := New(st)
	mustRegister(t, sched, &panicConnector{id: "panicky"}, time.Hour)

	// An unrecovered panic would crash the test binary instead of failing.
	result, found := sched.SyncNow(context.Background(), "panicky")
	if !found || result.Status != connector.StatusError {
		t.Fatalf("got found=%v result=%+v", found, result)
	}
	if stored, ok := st.Get("panicky"); !ok || stored.Status != connector.StatusError {
		t.Error("expected the error status to be saved in the store")
	}
}

func TestScheduler_RunLoop_SurvivesConnectorPanic(t *testing.T) {
	st := newNotifyStore()
	sched := New(st)
	mustRegister(t, sched, &panicConnector{id: "panicky"}, 10*time.Millisecond)
	mustRegister(t, sched, connector.NewMockConnector("healthy", 0), time.Hour)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = sched.Start(ctx); close(done) }()

	st.waitFor(t, "healthy")
	st.waitFor(t, "panicky")
	st.waitFor(t, "panicky") // the loop kept ticking after the first panic
	cancel()
	<-done

	if r, _ := st.Get("panicky"); r.Status != connector.StatusError {
		t.Errorf("expected error status for the panicking connector, got %+v", r)
	}
}

func TestScheduler_StopLeavesNoGoroutines(t *testing.T) {
	before := runtime.NumGoroutine()

	sched := New(store.NewMemoryStatusStore())
	for _, id := range []string{"a", "b", "c"} {
		mustRegister(t, sched, connector.NewMockConnector(id, 0), time.Millisecond)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = sched.Start(ctx); close(done) }()
	time.Sleep(20 * time.Millisecond) // let the loops tick a few times
	cancel()
	<-done

	deadline := time.Now().Add(time.Second)
	for runtime.NumGoroutine() > before {
		if time.Now().After(deadline) {
			t.Fatalf("goroutine leak: %d running, want %d", runtime.NumGoroutine(), before)
		}
		time.Sleep(time.Millisecond)
	}
}
