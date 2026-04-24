package orchestrator_test

import (
	"context"
	"fmt"
	"sync"
	"testing"

	orch "github.com/baalamai/orchestrator"
	"github.com/baalamai/orchestrator/store"
	"github.com/baalamai/orchestrator/supervisor"
)

// ── RetryPolicy tests ────────────────────────────────────────────────

func TestEngine_Retry_SucceedsAfterTransientError(t *testing.T) {
	attempts := 0
	node := func(_ context.Context, _ orch.StateView, _ *orch.Turn, _ *orch.NodeInput) (*orch.NodeResult, error) {
		attempts++
		if attempts < 3 {
			return nil, fmt.Errorf("transient error")
		}
		return &orch.NodeResult{Event: orch.EventWaitUser, Answer: "recovered"}, nil
	}

	engine := orch.NewPipelineBuilder().
		WithSupervisor(&supervisor.StateMachine{DefaultPhase: "a"}).
		RegisterNode("a", node).
		WithRetry(orch.RetryPolicy{MaxAttempts: 3}).
		MustBuild()

	result, err := engine.Run(context.Background(), store.NewMemory(), orch.NewTurn("c1", "hi"))
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
	node := func(_ context.Context, _ orch.StateView, _ *orch.Turn, _ *orch.NodeInput) (*orch.NodeResult, error) {
		attempts++
		return nil, fmt.Errorf("persistent error")
	}

	engine := orch.NewPipelineBuilder().
		WithSupervisor(&supervisor.StateMachine{DefaultPhase: "a"}).
		RegisterNode("a", node).
		WithRetry(orch.RetryPolicy{MaxAttempts: 3}).
		MustBuild()

	_, err := engine.Run(context.Background(), store.NewMemory(), orch.NewTurn("c1", "hi"))
	if err == nil {
		t.Fatal("expected error after exhausting retries")
	}
	if attempts != 3 {
		t.Errorf("expected exactly 3 attempts, got %d", attempts)
	}
}

func TestEngine_Retry_ShouldRetry_SkipsNonRetryableError(t *testing.T) {
	attempts := 0
	node := func(_ context.Context, _ orch.StateView, _ *orch.Turn, _ *orch.NodeInput) (*orch.NodeResult, error) {
		attempts++
		return nil, fmt.Errorf("permanent error")
	}

	engine := orch.NewPipelineBuilder().
		WithSupervisor(&supervisor.StateMachine{DefaultPhase: "a"}).
		RegisterNode("a", node).
		WithRetry(orch.RetryPolicy{
			MaxAttempts: 3,
			ShouldRetry: func(err error) bool { return false },
		}).
		MustBuild()

	_, err := engine.Run(context.Background(), store.NewMemory(), orch.NewTurn("c1", "hi"))
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
	node := func(_ context.Context, _ orch.StateView, _ *orch.Turn, _ *orch.NodeInput) (*orch.NodeResult, error) {
		attempts++
		cancel() // cancel after first attempt
		return nil, fmt.Errorf("error")
	}

	engine := orch.NewPipelineBuilder().
		WithSupervisor(&supervisor.StateMachine{DefaultPhase: "a"}).
		RegisterNode("a", node).
		WithRetry(orch.RetryPolicy{MaxAttempts: 5}).
		MustBuild()

	_, err := engine.Run(ctx, store.NewMemory(), orch.NewTurn("c1", "hi"))
	if err == nil {
		t.Fatal("expected error from cancelled context")
	}
	if attempts != 1 {
		t.Errorf("expected 1 attempt before context cancellation, got %d", attempts)
	}
}

func TestEngine_Retry_NodeFunc_SucceedsAfterTransientError(t *testing.T) {
	attempts := 0
	node := func(_ context.Context, _ orch.StateView, _ *orch.Turn, _ *orch.NodeInput) (*orch.NodeResult, error) {
		attempts++
		if attempts < 2 {
			return nil, fmt.Errorf("transient error")
		}
		return &orch.NodeResult{Event: orch.EventWaitUser, Answer: "ok"}, nil
	}

	engine := orch.NewPipelineBuilder().
		WithSupervisor(&supervisor.StateMachine{DefaultPhase: "a"}).
		RegisterNode("a", node).
		WithRetry(orch.RetryPolicy{MaxAttempts: 2}).
		MustBuild()

	result, err := engine.Run(context.Background(), store.NewMemory(), orch.NewTurn("c1", "hi"))
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
	failingNode := func(_ context.Context, _ orch.StateView, _ *orch.Turn, _ *orch.NodeInput) (*orch.NodeResult, error) {
		return nil, fmt.Errorf("always fails")
	}

	engine := orch.NewPipelineBuilder().
		WithSupervisor(&supervisor.StateMachine{DefaultPhase: "primary"}).
		RegisterNode("primary", failingNode).
		RegisterNode("fallback", dummyNode("fallback answer", orch.EventWaitUser)).
		WithRetry(orch.RetryPolicy{
			MaxAttempts: 2,
			OnRetryExhausted: func(phase orch.Phase, _ error) (orch.Phase, error) {
				if phase == "primary" {
					return "fallback", nil
				}
				return "", fmt.Errorf("no fallback for %s", phase)
			},
		}).
		MustBuild()

	result, err := engine.Run(context.Background(), store.NewMemory(), orch.NewTurn("c1", "hi"))
	if err != nil {
		t.Fatalf("expected fallback to succeed, got: %v", err)
	}
	if result.Answer != "fallback answer" {
		t.Errorf("expected 'fallback answer', got %q", result.Answer)
	}
}

func TestEngine_Retry_DefaultNoRetry(t *testing.T) {
	attempts := 0
	node := func(_ context.Context, _ orch.StateView, _ *orch.Turn, _ *orch.NodeInput) (*orch.NodeResult, error) {
		attempts++
		return nil, fmt.Errorf("error")
	}

	// No WithRetry call — zero value should mean no retry
	engine := buildSimpleNodeEngine("a", node)

	_, err := engine.Run(context.Background(), store.NewMemory(), orch.NewTurn("c1", "hi"))
	if err == nil {
		t.Fatal("expected error")
	}
	if attempts != 1 {
		t.Errorf("expected exactly 1 attempt (no retry), got %d", attempts)
	}
}

// ── Parallel Execution tests ────────────────────────────────────────

type parallelSupervisor struct {
	phases []orch.Phase
}

func (p *parallelSupervisor) DecideNextStep(_ context.Context, _ orch.StateStore, _ string, _ orch.Phase, _ orch.EventType) (orch.Phase, string, error) {
	return p.phases[0], "parallel", nil
}

func (p *parallelSupervisor) DecideNextSteps(_ context.Context, _ orch.StateStore, _ string, _ orch.Phase, _ orch.EventType) ([]orch.Phase, string, error) {
	return p.phases, "parallel batch", nil
}

func (p *parallelSupervisor) Usage() *orch.Usage { return nil }

func TestEngine_ParallelExecution_ConcurrentNodes(t *testing.T) {
	var mu sync.Mutex
	var executedPhases []string

	makeNode := func(name string) orch.NodeFunc {
		return func(_ context.Context, _ orch.StateView, _ *orch.Turn, _ *orch.NodeInput) (*orch.NodeResult, error) {
			mu.Lock()
			executedPhases = append(executedPhases, name)
			mu.Unlock()
			return &orch.NodeResult{
				Event:  orch.EventWaitUser,
				Answer: name,
				Delta:  orch.StateDelta{Updates: map[string]any{name + "_done": true}},
			}, nil
		}
	}

	engine := orch.NewPipelineBuilder().
		WithSupervisor(&parallelSupervisor{phases: []orch.Phase{"search", "enrich", "validate"}}).
		RegisterConcurrentNode("search", makeNode("search")).
		RegisterConcurrentNode("enrich", makeNode("enrich")).
		RegisterNode("validate", makeNode("validate")). // non-safe, runs last
		MustBuild()

	store := store.NewMemory()
	result, err := engine.Run(context.Background(), store, orch.NewTurn("c1", "hi"))
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
