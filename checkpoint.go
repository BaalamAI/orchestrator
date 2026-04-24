package orchestrator

import (
	"context"
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
// Implementations (Redis, Mongo, store.MemoryCheckpoint for tests) must be
// concurrency-safe and should enforce a TTL — checkpoints are only useful for
// a single turn's duration.
type CheckpointStore interface {
	// Save persists a checkpoint, overwriting any existing one for the same TurnID.
	// Non-fatal: callers log and continue on error.
	Save(ctx context.Context, ckpt *Checkpoint) error
	// Load returns the latest checkpoint for turnID, or (nil, nil) if none exists.
	Load(ctx context.Context, turnID string) (*Checkpoint, error)
	// Clear removes the checkpoint for turnID. Called on successful turn completion.
	Clear(ctx context.Context, turnID string) error
}
