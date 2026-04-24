package orchestrator

import (
	"context"
	"sync"
	"time"
)

// Checkpoint is an atomic snapshot of the pipeline loop state at a phase boundary.
// It is persisted by CheckpointStore after each successful step and loaded on the
// next Engine.Run call for the same Turn.TurnID to resume execution mid-turn
// after a crash.
type Checkpoint struct {
	// TurnID identifies the turn this checkpoint belongs to.
	TurnID string
	// Step is the number of completed supervisor→agent iterations at snapshot time.
	Step int
	// Phase is the last executed phase.
	Phase Phase
	// LastEvent is the event emitted by the last agent.
	LastEvent EventType
	// State is a shallow copy of the StateStore.State() at snapshot time.
	// Store implementations are responsible for also persisting their own state,
	// so this field is primarily for audit / partial recovery when the store is transient.
	State map[string]any
	// Messages is a shallow copy of the conversation history.
	Messages []Message
	// Usage is the accumulated token usage up to this step.
	Usage *Usage
	// ErrorCounts serializes the errorLedger by ErrorCategory string.
	ErrorCounts map[string]int
	// SharedContext is the cross-phase context map propagated between phases.
	SharedContext map[string]any
	// CreatedAt is the wall-clock time the checkpoint was written.
	CreatedAt time.Time
}

// CheckpointStore is the outbound port for mid-turn checkpoint persistence.
// Implementations (Redis, Mongo, in-memory for tests) must be concurrency-safe
// and should enforce a TTL — checkpoints are only useful for a single turn's
// duration (typically < 60s); stale checkpoints should be garbage-collected.
type CheckpointStore interface {
	// Save persists a checkpoint, overwriting any existing one for the same TurnID.
	// Non-fatal: callers log and continue on error.
	Save(ctx context.Context, ckpt *Checkpoint) error
	// Load returns the latest checkpoint for turnID, or (nil, nil) if none exists.
	// Errors other than "not found" should be returned; not-found returns (nil, nil).
	Load(ctx context.Context, turnID string) (*Checkpoint, error)
	// Clear removes the checkpoint for turnID. Called on successful turn completion.
	Clear(ctx context.Context, turnID string) error
}

// MemoryCheckpointStore is an in-memory implementation of CheckpointStore for tests.
// It is concurrency-safe and performs a shallow copy of each checkpoint on Save/Load
// so callers can't accidentally mutate stored state.
type MemoryCheckpointStore struct {
	mu    sync.Mutex
	items map[string]*Checkpoint
}

// NewMemoryCheckpointStore returns an initialized MemoryCheckpointStore.
func NewMemoryCheckpointStore() *MemoryCheckpointStore {
	return &MemoryCheckpointStore{items: make(map[string]*Checkpoint)}
}

// Save stores a shallow copy of ckpt under its TurnID.
func (s *MemoryCheckpointStore) Save(_ context.Context, ckpt *Checkpoint) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.items[ckpt.TurnID] = copyCheckpoint(ckpt)
	return nil
}

// Load returns a shallow copy of the checkpoint or (nil, nil) if absent.
func (s *MemoryCheckpointStore) Load(_ context.Context, turnID string) (*Checkpoint, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ckpt, ok := s.items[turnID]
	if !ok {
		return nil, nil
	}
	return copyCheckpoint(ckpt), nil
}

// Clear removes the checkpoint for turnID; no-op if absent.
func (s *MemoryCheckpointStore) Clear(_ context.Context, turnID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.items, turnID)
	return nil
}

// copyCheckpoint performs a shallow copy safe for in-memory storage.
func copyCheckpoint(src *Checkpoint) *Checkpoint {
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
		cp.Messages = make([]Message, len(src.Messages))
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
			u.Breakdown = make([]ModelUsage, len(src.Usage.Breakdown))
			copy(u.Breakdown, src.Usage.Breakdown)
		}
		cp.Usage = &u
	}
	return &cp
}
