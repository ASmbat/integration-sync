package store

import (
	"testing"
	"time"

	"github.com/ASmbat/integration-sync/internal/connector"
)

func TestMemoryStatusStore_SetGet(t *testing.T) {
	s := NewMemoryStatusStore()
	if _, ok := s.Get("github"); ok {
		t.Fatal("expected no status before the first Set")
	}

	want := connector.SyncResult{Status: connector.StatusOK, RecordsSynced: 3, SyncedAt: time.Unix(1, 0)}
	s.Set("github", want)

	got, ok := s.Get("github")
	if !ok || got != want {
		t.Fatalf("got %+v (ok=%v), want %+v", got, ok, want)
	}
}

func TestMemoryStatusStore_AllReturnsCopy(t *testing.T) {
	s := NewMemoryStatusStore()
	s.Set("a", connector.SyncResult{Status: connector.StatusOK})

	all := s.All()
	all["b"] = connector.SyncResult{} // must not leak into the store

	if _, ok := s.Get("b"); ok {
		t.Fatal("All should return a copy, not the internal map")
	}
	if len(s.All()) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(s.All()))
	}
}
