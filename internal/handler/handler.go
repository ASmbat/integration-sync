package handler

import (
	"encoding/json"
	"net/http"

	"github.com/ASmbat/integration-sync/internal/scheduler"
	"github.com/ASmbat/integration-sync/internal/store"
)

type Handler struct {
	statusStore store.StatusStore
	scheduler   *scheduler.Scheduler
}

func New(statusStore store.StatusStore, sched *scheduler.Scheduler) *Handler {
	return &Handler{statusStore: statusStore, scheduler: sched}
}

func (h *Handler) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /connectors", h.listStatuses)
	mux.HandleFunc("GET /connectors/{id}", h.getStatus)
	mux.HandleFunc("POST /connectors/{id}/sync", h.syncNow)
	mux.HandleFunc("GET /healthz", h.health)
	return mux
}

func (h *Handler) listStatuses(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, h.statusStore.All())
}

func (h *Handler) getStatus(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	status, ok := h.statusStore.Get(id)
	if !ok {
		writeError(w, http.StatusNotFound, "no status recorded for connector "+id)
		return
	}
	writeJSON(w, http.StatusOK, status)
}

func (h *Handler) syncNow(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	result, found := h.scheduler.SyncNow(r.Context(), id)
	if !found {
		writeError(w, http.StatusNotFound, "unknown connector "+id)
		return
	}
	writeJSON(w, http.StatusOK, result)
}

func (h *Handler) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
