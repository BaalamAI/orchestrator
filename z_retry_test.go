package orchestrator

import (
	"context"
	"fmt"
	"sync"
	"testing"
)

// ── RetryPolicy tests ────────────────────────────────────────────────

func TestEngine_Retry_SucceedsAfterTransientError(t *testing.T) {
	attempts := 0
	node := func(_ context.Context, _ StateView, _ *Turn, _ *NodeInput) (*NodeResult, error) {
		attempts++
		if attempts < 3 {
			return nil, fmt.Errorf("transient error")
		}
		return &NodeResult{Event: EventWaitUser, Answer: "recovered"}, nil
	}

	engine := NewPipelineBuilder().
		WithSupervisor(&StateMachineSupervisor{DefaultPhase: "a"}).
		RegisterNode("a", node).
		WithRetry(RetryPolicy{MaxAttempts: 3}).
		MustBuild()

	result, err := engine.Run(context.Background(), NewMemoryStore(), NewTurn("c1", "hi"))
	if err != nil {
		t.Fatalf("expected success after retry, got: %v", err)
	}
	if result.Answer != "recovered" {
		t.Errorf("expected 'recovered', got %q", result.Answer)
	}
	if attempts != 3 {
		t.Errorf("expected 3 attempts, got %d", attempts)
	}
}

func TestEngine_Retry_MaxAttemptsExhausted(t *testing.T) {
	attempts := 0
	node := func(_ context.Context, _ StateView, _ *Turn, _ *NodeInput) (*NodeResult, error) {
		attempts++
		return nil, fmt.Errorf("persistent error")
	}

	engine := NewPipelineBuilder().
		WithSupervisor(&StateMachineSupervisor{DefaultPhase: "a"}).
		RegisterNode("a", node).
		WithRetry(RetryPolicy{MaxAttempts: 3}).
		MustBuild()

	_, err := engine.Run(context.Background(), NewMemoryStore(), NewTurn("c1", "hi"))
	if err == nil {
		t.Fatal("expected error after exhausting retries")
	}
	if attempts != 3 {
		t.Errorf("expected exactly 3 attempts, got %d", attempts)
	}
}

func TestEngine_Retry_ShouldRetry_SkipsNonRetryableError(t *testing.T) {
	attempts := 0
	node := func(_ context.Context, _ StateView, _ *Turn, _ *NodeInput) (*NodeResult, error) {
		attempts++
		return nil, fmt.Errorf("permanent error")
	}

	engine := NewPipelineBuilder().
		WithSupervisor(&StateMachineSupervisor{DefaultPhase: "a"}).
		RegisterNode("a", node).
		WithRetry(RetryPolicy{
			MaxAttempts: 3,
			ShouldRetry: func(err error) bool { return false },
		}).
		MustBuild()

	_, err := engine.Run(context.Background(), NewMemoryStore(), NewTurn("c1", "hi"))
	if err == nil {
		t.Fatal("expected error")
	}
	if attempts != 1 {
		t.Errorf("expected 1 attempt (no retry), got %d", attempts)
	}
}

func TestEngine_Retry_HonorsContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	attempts := 0
	node := func(_ context.Context, _ StateView, _ *Turn, _ *NodeInput) (*NodeResult, error) {
		attempts++
		cancel() // cancel after first attempt
		return nil, fmt.Errorf("error")
	}

	engine := NewPipelineBuilder().
		WithSupervisor(&StateMachineSupervisor{DefaultPhase: "a"}).
		RegisterNode("a", node).
		WithRetry(RetryPolicy{MaxAttempts: 5}).
		MustBuild()

	_, err := engine.Run(ctx, NewMemoryStore(), NewTurn("c1", "hi"))
	if err == nil {
		t.Fatal("expected error from cancelled context")
	}
	if attempts != 1 {
		t.Errorf("expected 1 attempt before context cancellation, got %d", attempts)
	}
}

func TestEngine_Retry_NodeFunc_SucceedsAfterTransientError(t *testing.T) {
	attempts := 0
	node := func(_ context.Context, _ StateView, _ *Turn, _ *NodeInput) (*NodeResult, error) {
		attempts++
		if attempts < 2 {
			return nil, fmt.Errorf("transient error")
		}
		return &NodeResult{Event: EventWaitUser, Answer: "ok"}, nil
	}

	engine := NewPipelineBuilder().
		WithSupervisor(&StateMachineSupervisor{DefaultPhase: "a"}).
		RegisterNode("a", node).
		WithRetry(RetryPolicy{MaxAttempts: 2}).
		MustBuild()

	result, err := engine.Run(context.Background(), NewMemoryStore(), NewTurn("c1", "hi"))
	if err != nil {
		t.Fatalf("expected success after retry, got: %v", err)
	}
	if result.Answer != "ok" {
		t.Errorf("expected 'ok', got %q", result.Answer)
	}
	if attempts != 2 {
		t.Errorf("expected 2 attempts, got %d", attempts)
	}
}

func TestEngine_Retry_FallbackOnExhausted(t *testing.T) {
	failingNode := func(_ context.Context, _ StateView, _ *Turn, _ *NodeInput) (*NodeResult, error) {
		return nil, fmt.Errorf("always fails")
	}

	engine := NewPipelineBuilder().
		WithSupervisor(&StateMachineSupervisor{DefaultPhase: "primary"}).
		RegisterNode("primary", failingNode).
		RegisterNode("fallback", dummyNode("fallback answer", EventWaitUser)).
		WithRetry(RetryPolicy{
			MaxAttempts: 2,
			OnRetryExhausted: func(phase Phase, _ error) (Phase, error) {
				if phase == "primary" {
					return "fallback", nil
				}
				return "", fmt.Errorf("no fallback for %s", phase)
			},
		}).
		MustBuild()

	result, err := engine.Run(context.Background(), NewMemoryStore(), NewTurn("c1", "hi"))
	if err != nil {
		t.Fatalf("expected fallback to succeed, got: %v", err)
	}
	if result.Answer != "fallback answer" {
		t.Errorf("expected 'fallback answer', got %q", result.Answer)
	}
}

func TestEngine_Retry_DefaultNoRetry(t *testing.T) {
	attempts := 0
	node := func(_ context.Context, _ StateView, _ *Turn, _ *NodeInput) (*NodeResult, error) {
		attempts++
		return nil, fmt.Errorf("error")
	}

	// No WithRetry call — zero value should mean no retry
	engine := buildSimpleNodeEngine("a", node)

	_, err := engine.Run(context.Background(), NewMemoryStore(), NewTurn("c1", "hi"))
	if err == nil {
		t.Fatal("expected error")
	}
	if attempts != 1 {
		t.Errorf("expected exactly 1 attempt (no retry), got %d", attempts)
	}
}

// ── Parallel Execution tests ────────────────────────────────────────

type parallelSupervisor struct {
	phases []Phase
}

func (p *parallelSupervisor) DecideNextStep(_ context.Context, _ StateStore, _ string, _ Phase, _ EventType) (Phase, string, error) {
	return p.phases[0], "parallel", nil
}

func (p *parallelSupervisor) DecideNextSteps(_ context.Context, _ StateStore, _ string, _ Phase, _ EventType) ([]Phase, string, error) {
	return p.phases, "parallel batch", nil
}

func (p *parallelSupervisor) Usage() *Usage { return nil }

func TestEngine_ParallelExecution_ConcurrentNodes(t *testing.T) {
	var mu sync.Mutex
	var executedPhases []string

	makeNode := func(name string) NodeFunc {
		return func(_ context.Context, _ StateView, _ *Turn, _ *NodeInput) (*NodeResult, error) {
			mu.Lock()
			executedPhases = append(executedPhases, name)
			mu.Unlock()
			return &NodeResult{
				Event:  EventWaitUser,
				Answer: name,
				Delta:  StateDelta{Updates: map[string]any{name + "_done": true}},
			}, nil
		}
	}

	engine := NewPipelineBuilder().
		WithSupervisor(&parallelSupervisor{phases: []Phase{"search", "enrich", "validate"}}).
		RegisterConcurrentNode("search", makeNode("search")).
		RegisterConcurrentNode("enrich", makeNode("enrich")).
		RegisterNode("validate", makeNode("validate")). // non-safe, runs last
		MustBuild()

	store := NewMemoryStore()
	result, err := engine.Run(context.Background(), store, NewTurn("c1", "hi"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// All 3 phases should have executed
	if len(executedPhases) != 3 {
		t.Fatalf("expected 3 phases executed, got %d: %v", len(executedPhases), executedPhases)
	}

	// validate (non-safe) must be last
	if executedPhases[2] != "validate" {
		t.Errorf("expected validate last (non-safe), got order: %v", executedPhases)
	}

	// Result should have the last answer
	if result.Answer != "validate" {
		t.Errorf("expected answer from last phase 'validate', got %q", result.Answer)
	}

	// All deltas should be applied
	state := store.State()
	for _, key := range []string{"search_done", "enrich_done", "validate_done"} {
		if state[key] != true {
			t.Errorf("expected %s=true in state", key)
		}
	}
}
