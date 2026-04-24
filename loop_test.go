package orchestrator_test

import (
	"context"
	"fmt"
	"testing"

	orch "github.com/baalamai/orchestrator"
	"github.com/baalamai/orchestrator/store"
	"github.com/baalamai/orchestrator/supervisor"
)

func TestEngine_EventWaitUser_BreaksLoop(t *testing.T) {
	callCount := 0
	node := func(_ context.Context, _ orch.StateView, _ *orch.Turn, _ *orch.NodeInput) (*orch.NodeResult, error) {
		callCount++
		return &orch.NodeResult{Event: orch.EventWaitUser, Answer: "waiting"}, nil
	}

	engine := buildSimpleNodeEngine("a", node)
	engine.Run(context.Background(), store.NewMemory(), orch.NewTurn("c1", "hi"))

	if callCount != 1 {
		t.Errorf("expected node called once (wait_user breaks), got %d", callCount)
	}
}

func TestEngine_PhaseComplete_Cascades(t *testing.T) {
	var phases []string
	makeNode := func(phase string, event orch.EventType) orch.NodeFunc {
		return func(_ context.Context, _ orch.StateView, _ *orch.Turn, _ *orch.NodeInput) (*orch.NodeResult, error) {
			phases = append(phases, phase)
			return &orch.NodeResult{Event: event, Answer: phase}, nil
		}
	}

	engine := orch.NewPipelineBuilder().
		WithSupervisor(&supervisor.StateMachine{
			DefaultPhase: "diagnostic",
			Transitions: []supervisor.TransitionRule{
				{From: "diagnostic", To: "payment"},
				{From: "payment", To: "complete"},
			},
		}).
		RegisterNode("diagnostic", makeNode("diagnostic", orch.EventPhaseComplete)).
		RegisterNode("payment", makeNode("payment", orch.EventPhaseComplete)).
		RegisterNode("complete", makeNode("complete", orch.EventWaitUser)).
		MustBuild()

	result, err := engine.Run(context.Background(), store.NewMemory(), orch.NewTurn("c1", "hi"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	expected := []string{"diagnostic", "payment", "complete"}
	if len(phases) != len(expected) {
		t.Fatalf("expected phases %v, got %v", expected, phases)
	}
	for i, p := range expected {
		if phases[i] != p {
			t.Errorf("phase %d: expected %s, got %s", i, p, phases[i])
		}
	}
	if result.Answer != "complete" {
		t.Errorf("expected final answer complete, got %s", result.Answer)
	}
}

func TestEngine_AntiStutter_BreaksOnSamePhase(t *testing.T) {
	callCount := 0
	node := func(_ context.Context, _ orch.StateView, _ *orch.Turn, _ *orch.NodeInput) (*orch.NodeResult, error) {
		callCount++
		return &orch.NodeResult{Event: orch.EventStepSuccess, Answer: "step"}, nil
	}

	engine := buildSimpleNodeEngine("a", node)
	engine.Run(context.Background(), store.NewMemory(), orch.NewTurn("c1", "hi"))

	// First call executes, second call supervisor returns same phase with step_success → anti-stutter breaks
	if callCount != 1 {
		t.Errorf("expected 1 call (anti-stutter), got %d", callCount)
	}
}

func TestEngine_RepeatablePhase_AllowsReentry(t *testing.T) {
	callCount := 0
	node := func(_ context.Context, _ orch.StateView, _ *orch.Turn, _ *orch.NodeInput) (*orch.NodeResult, error) {
		callCount++
		if callCount >= 3 {
			return &orch.NodeResult{Event: orch.EventWaitUser, Answer: "done"}, nil
		}
		return &orch.NodeResult{Event: orch.EventStepSuccess, Answer: "again"}, nil
	}

	engine := orch.NewPipelineBuilder().
		MaxSteps(5).
		WithSupervisor(&supervisor.StateMachine{DefaultPhase: "chat"}).
		RegisterNode("chat", node).
		Repeatable("chat").
		MustBuild()

	engine.Run(context.Background(), store.NewMemory(), orch.NewTurn("c1", "hi"))

	if callCount != 3 {
		t.Errorf("expected 3 calls for repeatable phase, got %d", callCount)
	}
}

func TestEngine_StateUpdatesApplied(t *testing.T) {
	node := func(_ context.Context, _ orch.StateView, _ *orch.Turn, _ *orch.NodeInput) (*orch.NodeResult, error) {
		return &orch.NodeResult{
			Event:  orch.EventPhaseComplete,
			Answer: "done",
			Delta: orch.StateDelta{Updates: map[string]any{
				"diagnostic_complete": true,
				"product":             "herbicida",
			}},
		}, nil
	}

	engine := orch.NewPipelineBuilder().
		WithSupervisor(&supervisor.StateMachine{
			DefaultPhase: "diagnostic",
			FlagRules:    []supervisor.FlagRule{{Flag: "diagnostic_complete", Phase: "payment"}},
		}).
		RegisterNode("diagnostic", node).
		RegisterNode("payment", dummyNode("pay", orch.EventWaitUser)).
		MustBuild()

	store := store.NewMemory()
	engine.Run(context.Background(), store, orch.NewTurn("c1", "hi"))

	if !store.HasFlag("diagnostic_complete") {
		t.Error("expected diagnostic_complete flag set")
	}
	state := store.State()
	if state["product"] != "herbicida" {
		t.Errorf("expected product=herbicida, got %v", state["product"])
	}
}

func TestEngine_SharedContext_PassedBetweenPhases(t *testing.T) {
	// Phase 1 sets shared context; Phase 2 reads it from input.SharedContext
	var receivedContext map[string]any

	phase1 := func(_ context.Context, _ orch.StateView, _ *orch.Turn, _ *orch.NodeInput) (*orch.NodeResult, error) {
		return &orch.NodeResult{
			Event:         orch.EventPhaseComplete,
			Answer:        "phase1",
			SharedContext: map[string]any{"suggested_model": "premium", "priority": 1},
		}, nil
	}
	phase2 := func(_ context.Context, _ orch.StateView, _ *orch.Turn, input *orch.NodeInput) (*orch.NodeResult, error) {
		receivedContext = input.SharedContext
		return &orch.NodeResult{Event: orch.EventWaitUser, Answer: "phase2"}, nil
	}

	engine := orch.NewPipelineBuilder().
		WithSupervisor(&supervisor.StateMachine{
			DefaultPhase: "diag",
			Transitions:  []supervisor.TransitionRule{{From: "diag", To: "quote"}},
		}).
		RegisterNode("diag", phase1).
		RegisterNode("quote", phase2).
		MustBuild()

	engine.Run(context.Background(), store.NewMemory(), orch.NewTurn("c1", "hi"))

	if receivedContext == nil {
		t.Fatal("expected phase2 to receive shared context from phase1")
	}
	if receivedContext["suggested_model"] != "premium" {
		t.Errorf("expected suggested_model=premium, got %v", receivedContext["suggested_model"])
	}
	if receivedContext["priority"] != 1 {
		t.Errorf("expected priority=1, got %v", receivedContext["priority"])
	}
}

func TestEngine_ErrorThreshold_StopsPipeline(t *testing.T) {
	callCount := 0
	node := func(_ context.Context, _ orch.StateView, _ *orch.Turn, _ *orch.NodeInput) (*orch.NodeResult, error) {
		callCount++
		if callCount <= 2 {
			return nil, fmt.Errorf("transient error")
		}
		return &orch.NodeResult{Event: orch.EventWaitUser, Answer: "ok"}, nil
	}

	engine := orch.NewPipelineBuilder().
		MaxSteps(10).
		WithSupervisor(&supervisor.StateMachine{DefaultPhase: "a"}).
		RegisterNode("a", node).
		Repeatable("a").
		WithRetry(orch.RetryPolicy{MaxAttempts: 5}).
		WithBudget(orch.BudgetConfig{MaxTransientErrors: 2}).
		MustBuild()

	result, err := engine.Run(context.Background(), store.NewMemory(), orch.NewTurn("c1", "hi"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Answer != "ok" {
		t.Errorf("expected 'ok', got %q", result.Answer)
	}
	// The ledger should have recorded the transient errors
	if result.Metadata == nil {
		t.Fatal("expected metadata with error ledger")
	}
	ledger, ok := result.Metadata[orch.MetaErrorLedger]
	if !ok {
		t.Fatal("expected error_ledger in metadata")
	}
	counts, ok := ledger.(map[string]int)
	if !ok {
		t.Fatal("expected error_ledger to be map[string]int")
	}
	if counts["transient"] != 2 {
		t.Errorf("expected 2 transient errors, got %d", counts["transient"])
	}
	// Should have stopped via error threshold
	if result.Metadata[orch.MetaErrorThreshold] != true {
		t.Error("expected error_threshold=true in metadata")
	}
}
