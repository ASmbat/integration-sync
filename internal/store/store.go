// Package store tracks the last known sync status of every connector.
package store

import (
	"sync"

	"github.com/ASmbat/integration-sync/internal/connector"
)

// StatusStore is a thread-safe read/write store for the latest sync result
// per connector. A real deployment would likely back this with a database
// so status survives restarts; the interface keeps that swap contained.
type StatusStore interface {
	Set(connectorID string, result connector.SyncResult)
	Get(connectorID string) (connector.SyncResult, bool)
	All() map[string]connector.SyncResult
}

type MemoryStatusStore struct {
	mu       sync.RWMutex
	statuses map[string]connector.SyncResult
}

func NewMemoryStatusStore() *MemoryStatusStore {
	return &MemoryStatusStore{statuses: make(map[string]connector.SyncResult)}
}

func (s *MemoryStatusStore) Set(connectorID string, result connector.SyncResult) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.statuses[connectorID] = result
}

func (s *MemoryStatusStore) Get(connectorID string) (connector.SyncResult, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	r, ok := s.statuses[connectorID]
	return r, ok
}

func (s *MemoryStatusStore) All() map[string]connector.SyncResult {
	s.mu.RLock()
	defer s.mu.RUnlock()
	cp := make(map[string]connector.SyncResult, len(s.statuses))
	for k, v := range s.statuses {
		cp[k] = v
	}
	return cp
}
