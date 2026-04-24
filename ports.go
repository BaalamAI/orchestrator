package orchestrator

import "context"

// ── Ports ────────────────────────────────────────────────────────────
// Interfaces the Engine depends on. Each port has one concrete adapter in
// this module (supervisor/, store/) and possibly more in consumer services.
//
// LLM-specific ports (Client, Tool, CostCalculator-of-tokens) live in the
// llm/ subpackage alongside the tool-loop adapter that consumes them.

// StateStore is the outbound port for state and conversation history persistence.
// Implementations: store.Memory (in-memory) and RedisStateAdapter (production).
type StateStore interface {
	// State returns the full state map.
	State() map[string]any
	// SetState sets a single key in the state.
	SetState(key string, value any)
	// DeleteState removes a single key from the state.
	DeleteState(key string)
	// HasFlag returns a boolean flag from the state.
	HasFlag(key string) bool
	// Messages returns the conversation history.
	Messages() []Message
	// AddMessage appends a message to the conversation history and persists.
	AddMessage(role, text string) error
	// Restore replaces state and message history from a checkpoint snapshot.
	Restore(state map[string]any, messages []Message) error
	// Save persists the current state.
	Save() error
}

// Supervisor decides which phase to execute next in each iteration of the engine loop.
// DecideNextStep returns the next phase, a human-readable reason, and an error.
// Usage returns and clears the accumulated token usage from the last decision.
type Supervisor interface {
	DecideNextStep(ctx context.Context, store StateStore, userText string, currentPhase Phase, lastEvent EventType) (Phase, string, error)
	Usage() *Usage
}

// SupervisorValidator is an optional interface a Supervisor can implement so
// the builder can validate that every referenced phase has a registered node.
// Concrete supervisors in the supervisor/ subpackage implement this; unknown
// supervisors are skipped.
type SupervisorValidator interface {
	ValidatePhases(hasPhase func(string) bool) error
}

// ParallelSupervisor is an optional interface that supervisors can implement
// to return multiple candidate phases for concurrent execution. The engine
// partitions candidates into concurrent-safe and non-safe groups: safe phases
// run in parallel, non-safe phases run sequentially afterward.
type ParallelSupervisor interface {
	Supervisor
	DecideNextSteps(ctx context.Context, store StateStore, userText string, currentPhase Phase, lastEvent EventType) ([]Phase, string, error)
}

// IntentRouter classifies user intent via LLM or rules.
// Used optionally by supervisor.StateMachine at the start of a turn when no
// deterministic rules (transitions or flags) match.
type IntentRouter interface {
	Route(ctx context.Context, store StateStore, text string) (Phase, error)
	Usage() *Usage
}

// ErrorClassifier maps provider-specific errors to orchestrator retry categories.
// Callers wire a concrete classifier via WithClassifier. Without one, the engine
// uses noopClassifier, which treats every non-nil error as CategoryTransient.
type ErrorClassifier interface {
	Classify(err error) ErrorCategory
}

// CostCalculator computes USD cost from per-call token usage for budget enforcement.
// Implementations own the pricing table for the models they support.
type CostCalculator interface {
	Calculate(model string, promptTokens, completionTokens int) float64
}

type noopClassifier struct{}

func (noopClassifier) Classify(err error) ErrorCategory {
	if err == nil {
		return CategoryUnknown
	}
	return CategoryTransient
}

type noopCostCalculator struct{}

func (noopCostCalculator) Calculate(string, int, int) float64 { return 0 }
