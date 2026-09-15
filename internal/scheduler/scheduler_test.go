package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/ASmbat/integration-sync/internal/connector"
	"github.com/ASmbat/integration-sync/internal/store"
)

// slowConnector simulates a vendor whose Sync call takes a while — a slow
// or struggling third-party API, not necessarily a broken one.
type slowConnector struct {
	id string
	d  time.Duration
}

func (c *slowConnector) ID() string          { return c.id }
func (c *slowConnector) DisplayName() string { return c.id }
func (c *slowConnector) Sync(ctx context.Context) (connector.SyncResult, error) {
	time.Sleep(c.d)
	return connector.SyncResult{Status: connector.StatusOK, SyncedAt: time.Now()}, nil
}

// panicConnector simulates a connector whose vendor-specific code panics
// (e.g. on an unexpected response shape) instead of returning an error.
type panicConnector struct{ id string }

func (c *panicConnector) ID() string          { return c.id }
func (c *panicConnector) DisplayName() string { return c.id }
func (c *panicConnector) Sync(ctx context.Context) (connector.SyncResult, error) {
	var m map[string]int
	m["x"] = 1 // panics: assignment to entry in nil map
	return connector.SyncResult{}, nil
}

func TestScheduler_RunsImmediateSyncOnStart(t *testing.T) {
	statusStore := store.NewMemoryStatusStore()
	sched := New(statusStore)
	mock := connector.NewMockConnector("demo", 0)
	sched.Register(mock, time.Hour) // long interval; we only care about the immediate run

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	go sched.Start(ctx)

	// Give the immediate sync goroutine a moment to run.
	time.Sleep(50 * time.Millisecond)

	result, ok := statusStore.Get("demo")
	if !ok {
		t.Fatal("expected a status to be recorded after start")
	}
	if result.Status != connector.StatusOK {
		t.Errorf("expected StatusOK, got %s", result.Status)
	}
}

func TestScheduler_SyncNow(t *testing.T) {
	statusStore := store.NewMemoryStatusStore()
	sched := New(statusStore)
	mock := connector.NewMockConnector("demo", 0)
	sched.Register(mock, time.Hour)

	result, found := sched.SyncNow(context.Background(), "demo")
	if !found {
		t.Fatal("expected connector to be found")
	}
	if result.Status != connector.StatusOK {
		t.Errorf("expected StatusOK, got %s", result.Status)
	}

	stored, ok := statusStore.Get("demo")
	if !ok || stored.RecordsSynced != result.RecordsSynced {
		t.Errorf("expected SyncNow result to be persisted in the store")
	}
}

func TestScheduler_SyncNow_UnknownConnector(t *testing.T) {
	sched := New(store.NewMemoryStatusStore())
	_, found := sched.SyncNow(context.Background(), "does-not-exist")
	if found {
		t.Error("expected found=false for unregistered connector")
	}
}

func TestScheduler_SyncNow_DoesNotBlockOnUnrelatedSlowConnector(t *testing.T) {
	sched := New(store.NewMemoryStatusStore())
	sched.Register(&slowConnector{id: "slow", d: 300 * time.Millisecond}, time.Hour)
	sched.Register(connector.NewMockConnector("fast", 0), time.Hour)

	go sched.SyncNow(context.Background(), "slow")
	time.Sleep(30 * time.Millisecond) // let the slow sync actually start and take the lookup lock

	done := make(chan time.Duration, 1)
	go func() {
		start := time.Now()
		sched.SyncNow(context.Background(), "fast")
		done <- time.Since(start)
	}()

	select {
	case elapsed := <-done:
		if elapsed > 150*time.Millisecond {
			t.Fatalf("SyncNow for an unrelated connector took %s; it should not wait on the slow connector's Sync call", elapsed)
		}
	case <-time.After(time.Second):
		t.Fatal("SyncNow for the fast connector never returned — it's blocked behind the slow one")
	}
}

func TestScheduler_SyncNow_RecoversConnectorPanic(t *testing.T) {
	statusStore := store.NewMemoryStatusStore()
	sched := New(statusStore)
	sched.Register(&panicConnector{id: "panicky"}, time.Hour)

	// If the panic isn't recovered, this test process crashes outright
	// rather than failing normally — so getting a result back at all is
	// part of what's being verified.
	result, found := sched.SyncNow(context.Background(), "panicky")
	if !found {
		t.Fatal("expected connector to be found")
	}
	if result.Status != connector.StatusError {
		t.Errorf("expected a panicking connector to report StatusError, got %s", result.Status)
	}

	stored, ok := statusStore.Get("panicky")
	if !ok || stored.Status != connector.StatusError {
		t.Errorf("expected the error status to be persisted in the store")
	}
}

func TestScheduler_RunLoop_SurvivesConnectorPanic(t *testing.T) {
	statusStore := store.NewMemoryStatusStore()
	sched := New(statusStore)
	sched.Register(&panicConnector{id: "panicky"}, 30*time.Millisecond)
	sched.Register(connector.NewMockConnector("healthy", 0), time.Hour)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	sched.Start(ctx) // would crash the test binary if the panic weren't recovered

	if _, ok := statusStore.Get("healthy"); !ok {
		t.Error("expected the healthy connector to still have run despite the other one panicking")
	}
	if status, ok := statusStore.Get("panicky"); !ok || status.Status != connector.StatusError {
		t.Errorf("expected the panicking connector's status to be recorded as an error, got %+v (ok=%v)", status, ok)
	}
}
