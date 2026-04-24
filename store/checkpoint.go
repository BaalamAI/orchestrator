package store

import (
	"context"
	"sync"

	"github.com/baalamai/orchestrator"
)

// MemoryCheckpoint is an in-memory implementation of orchestrator.CheckpointStore.
// It is concurrency-safe and performs a shallow copy of each checkpoint on Save/Load
// so callers can't accidentally mutate stored state.
type MemoryCheckpoint struct {
	mu    sync.Mutex
	items map[string]*orchestrator.Checkpoint
}

// NewMemoryCheckpoint returns an initialized MemoryCheckpoint.
func NewMemoryCheckpoint() *MemoryCheckpoint {
	return &MemoryCheckpoint{items: make(map[string]*orchestrator.Checkpoint)}
}

// Save stores a shallow copy of ckpt under its TurnID.
func (s *MemoryCheckpoint) Save(_ context.Context, ckpt *orchestrator.Checkpoint) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.items[ckpt.TurnID] = copyCheckpoint(ckpt)
	return nil
}

// Load returns a shallow copy of the checkpoint or (nil, nil) if absent.
func (s *MemoryCheckpoint) Load(_ context.Context, turnID string) (*orchestrator.Checkpoint, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ckpt, ok := s.items[turnID]
	if !ok {
		return nil, nil
	}
	return copyCheckpoint(ckpt), nil
}

// Clear removes the checkpoint for turnID; no-op if absent.
func (s *MemoryCheckpoint) Clear(_ context.Context, turnID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.items, turnID)
	return nil
}

func copyCheckpoint(src *orchestrator.Checkpoint) *orchestrator.Checkpoint {
	if src == nil {
		return nil
	}
	cp := *src
	if src.State != nil {
		cp.State = make(map[string]any, len(src.State))
		for k, v := range src.State {
			cp.State[k] = v
		}
	}
	if src.Messages != nil {
		cp.Messages = make([]orchestrator.Message, len(src.Messages))
		copy(cp.Messages, src.Messages)
	}
	if src.ErrorCounts != nil {
		cp.ErrorCounts = make(map[string]int, len(src.ErrorCounts))
		for k, v := range src.ErrorCounts {
			cp.ErrorCounts[k] = v
		}
	}
	if src.SharedContext != nil {
		cp.SharedContext = make(map[string]any, len(src.SharedContext))
		for k, v := range src.SharedContext {
			cp.SharedContext[k] = v
		}
	}
	if src.Usage != nil {
		u := *src.Usage
		if src.Usage.Breakdown != nil {
			u.Breakdown = make([]orchestrator.ModelUsage, len(src.Usage.Breakdown))
			copy(u.Breakdown, src.Usage.Breakdown)
		}
		cp.Usage = &u
	}
	return &cp
}
