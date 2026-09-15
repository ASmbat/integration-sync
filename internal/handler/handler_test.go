package handler

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ASmbat/integration-sync/internal/connector"
	"github.com/ASmbat/integration-sync/internal/scheduler"
	"github.com/ASmbat/integration-sync/internal/store"
)

func newTestHandler() (*Handler, *scheduler.Scheduler) {
	statusStore := store.NewMemoryStatusStore()
	sched := scheduler.New(statusStore)
	sched.Register(connector.NewMockConnector("demo", 0), time.Hour)
	return New(statusStore, sched), sched
}

func TestListStatuses_EmptyInitially(t *testing.T) {
	h, _ := newTestHandler()

	req := httptest.NewRequest(http.MethodGet, "/connectors", nil)
	rr := httptest.NewRecorder()
	h.Routes().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
	if rr.Body.String() != "{}\n" {
		t.Errorf("expected empty object, got %s", rr.Body.String())
	}
}

func TestSyncNow_UpdatesStatus(t *testing.T) {
	h, _ := newTestHandler()

	req := httptest.NewRequest(http.MethodPost, "/connectors/demo/sync", nil)
	rr := httptest.NewRecorder()
	h.Routes().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rr.Code, rr.Body.String())
	}

	getReq := httptest.NewRequest(http.MethodGet, "/connectors/demo", nil)
	getRR := httptest.NewRecorder()
	h.Routes().ServeHTTP(getRR, getReq)

	if getRR.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", getRR.Code)
	}
}

func TestSyncNow_UnknownConnector(t *testing.T) {
	h, _ := newTestHandler()

	req := httptest.NewRequest(http.MethodPost, "/connectors/nope/sync", nil)
	rr := httptest.NewRecorder()
	h.Routes().ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", rr.Code)
	}
}

func TestGetStatus_NotYetSynced(t *testing.T) {
	h, _ := newTestHandler()

	req := httptest.NewRequest(http.MethodGet, "/connectors/demo", nil)
	rr := httptest.NewRecorder()
	h.Routes().ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Fatalf("expected 404 before any sync has run, got %d", rr.Code)
	}
}

func TestHealthz(t *testing.T) {
	h, _ := newTestHandler()

	req := httptest.NewRequest(http.MethodGet, "/healthz", nil)
	rr := httptest.NewRecorder()
	h.Routes().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", rr.Code)
	}
}
