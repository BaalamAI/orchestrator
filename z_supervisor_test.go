package orchestrator

import (
	"context"
	"fmt"
	"testing"
)

// --- Mock IntentRouter ---

type mockRouter struct {
	target Phase
	err    error
	usage  *Usage
}

func (m *mockRouter) Route(_ context.Context, _ StateStore, _ string) (Phase, error) {
	return m.target, m.err
}

func (m *mockRouter) Usage() *Usage {
	return m.usage
}

// --- StateMachineSupervisor Tests ---

func TestStateMachine_TransitionOnPhaseComplete(t *testing.T) {
	sup := &StateMachineSupervisor{
		Transitions: []TransitionRule{
			{From: "diagnostic", To: "payment"},
			{From: "payment", To: "register"},
		},
		DefaultPhase: "diagnostic",
	}

	phase, _, err := sup.DecideNextStep(context.Background(), NewMemoryStore(), "", "diagnostic", EventPhaseComplete)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if phase != "payment" {
		t.Errorf("expected payment, got %s", phase)
	}
}

func TestStateMachine_ConditionalTransition(t *testing.T) {
	sup := &StateMachineSupervisor{
		ConditionalTransitions: []ConditionalTransition{
			{
				From:      "payment",
				To:        "register",
				Condition: func(s StateStore) bool { return s.HasFlag("chosen_payment") },
			},
		},
		Transitions: []TransitionRule{
			{From: "payment", To: "complete"},
		},
		DefaultPhase: "diagnostic",
	}

	store := NewMemoryStore()

	// Without flag → falls to simple transition
	phase, _, _ := sup.DecideNextStep(context.Background(), store, "", "payment", EventPhaseComplete)
	if phase != "complete" {
		t.Errorf("expected complete (simple transition), got %s", phase)
	}

	// With flag → conditional transition
	store.SetState("chosen_payment", true)
	phase, _, _ = sup.DecideNextStep(context.Background(), store, "", "payment", EventPhaseComplete)
	if phase != "register" {
		t.Errorf("expected register (conditional), got %s", phase)
	}
}

func TestStateMachine_FlagRules(t *testing.T) {
	sup := &StateMachineSupervisor{
		FlagRules: []FlagRule{
			{Flag: "register_complete", Phase: "complete"},
			{Flag: "chosen_payment", Phase: "register"},
			{Flag: "diagnostic_complete", Phase: "payment"},
		},
		DefaultPhase: "diagnostic",
	}

	store := NewMemoryStore()
	store.SetState("diagnostic_complete", true)

	// First matching flag wins
	phase, _, _ := sup.DecideNextStep(context.Background(), store, "", "", EventStepSuccess)
	if phase != "payment" {
		t.Errorf("expected payment from flag rule, got %s", phase)
	}

	// Higher priority flag takes precedence
	store.SetState("register_complete", true)
	phase, _, _ = sup.DecideNextStep(context.Background(), store, "", "", EventStepSuccess)
	if phase != "complete" {
		t.Errorf("expected complete from higher priority flag, got %s", phase)
	}
}

func TestStateMachine_FlagRuleSamePhase(t *testing.T) {
	sup := &StateMachineSupervisor{
		FlagRules: []FlagRule{
			{Flag: "in_payment", Phase: "payment"},
		},
		DefaultPhase: "diagnostic",
	}

	store := NewMemoryStore()
	store.SetState("in_payment", true)

	// When already in the same phase, flag rule doesn't re-enter
	phase, _, _ := sup.DecideNextStep(context.Background(), store, "", "payment", EventStepSuccess)
	if phase != "payment" {
		t.Errorf("expected to stay in payment, got %s", phase)
	}
}

func TestStateMachine_RouterAtTurnStart(t *testing.T) {
	router := &mockRouter{
		target: "quote",
		usage:  &Usage{PromptTokens: 100, CompletionTokens: 50, TotalTokens: 150},
	}
	sup := &StateMachineSupervisor{
		DefaultPhase: "diagnostic",
		Router:       router,
	}

	phase, _, err := sup.DecideNextStep(context.Background(), NewMemoryStore(), "cuanto cuesta?", "", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if phase != "quote" {
		t.Errorf("expected quote from router, got %s", phase)
	}

	// Usage should be captured
	usage := sup.Usage()
	if usage == nil {
		t.Fatal("expected usage from router")
	}
	if usage.PromptTokens != 100 {
		t.Errorf("expected 100 prompt tokens, got %d", usage.PromptTokens)
	}

	// Usage clears after read
	if sup.Usage() != nil {
		t.Error("expected nil usage after second Usage call")
	}
}

func TestStateMachine_RouterError_FallbackToDefault(t *testing.T) {
	router := &mockRouter{err: fmt.Errorf("timeout")}
	sup := &StateMachineSupervisor{
		DefaultPhase: "diagnostic",
		Router:       router,
	}

	phase, _, err := sup.DecideNextStep(context.Background(), NewMemoryStore(), "hola", "", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if phase != "diagnostic" {
		t.Errorf("expected diagnostic fallback, got %s", phase)
	}
}

func TestStateMachine_NoRouterNoFlags_Default(t *testing.T) {
	sup := &StateMachineSupervisor{
		DefaultPhase: "diagnostic",
	}

	phase, _, _ := sup.DecideNextStep(context.Background(), NewMemoryStore(), "hola", "", "")
	if phase != "diagnostic" {
		t.Errorf("expected diagnostic default, got %s", phase)
	}
}

func TestStateMachine_ContinueCurrentPhase(t *testing.T) {
	sup := &StateMachineSupervisor{
		DefaultPhase: "diagnostic",
	}

	phase, _, _ := sup.DecideNextStep(context.Background(), NewMemoryStore(), "", "info", EventStepSuccess)
	if phase != "info" {
		t.Errorf("expected to continue in info, got %s", phase)
	}
}

// --- LinearSupervisor Tests ---

func TestLinear_StartsFirstPhase(t *testing.T) {
	sup := NewLinearSupervisor("a", "b", "c")

	phase, _, err := sup.DecideNextStep(context.Background(), nil, "", "", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if phase != "a" {
		t.Errorf("expected first phase a, got %s", phase)
	}
}

func TestLinear_AdvancesOnPhaseComplete(t *testing.T) {
	sup := NewLinearSupervisor("a", "b", "c")

	// Start
	sup.DecideNextStep(context.Background(), nil, "", "", "")

	// Advance to b
	phase, _, _ := sup.DecideNextStep(context.Background(), nil, "", "a", EventPhaseComplete)
	if phase != "b" {
		t.Errorf("expected b, got %s", phase)
	}

	// Advance to c
	phase, _, _ = sup.DecideNextStep(context.Background(), nil, "", "b", EventPhaseComplete)
	if phase != "c" {
		t.Errorf("expected c, got %s", phase)
	}

	// Stay at c (last phase)
	phase, _, _ = sup.DecideNextStep(context.Background(), nil, "", "c", EventPhaseComplete)
	if phase != "c" {
		t.Errorf("expected to stay at c, got %s", phase)
	}
}

func TestLinear_StaysWithoutPhaseComplete(t *testing.T) {
	sup := NewLinearSupervisor("a", "b")
	sup.DecideNextStep(context.Background(), nil, "", "", "")

	phase, _, _ := sup.DecideNextStep(context.Background(), nil, "", "a", EventStepSuccess)
	if phase != "a" {
		t.Errorf("expected to stay at a without phase_complete, got %s", phase)
	}
}

func TestLinear_NoPhases_Error(t *testing.T) {
	sup := NewLinearSupervisor()
	_, _, err := sup.DecideNextStep(context.Background(), nil, "", "", "")
	if err == nil {
		t.Error("expected error for empty phases")
	}
}

func TestLinear_Usage_ReturnsNil(t *testing.T) {
	sup := NewLinearSupervisor("a")
	if sup.Usage() != nil {
		t.Error("expected nil usage")
	}
}

func TestStateMachine_RouterOverridesFlagsAtTurnStart(t *testing.T) {
	// When a router is configured and it's turn start (currentPhase==""),
	// the router should decide the phase — even if flag rules would force a different one.
	// This lets users ask new questions after completing a flow (e.g. info query after register_complete).
	router := &mockRouter{target: "info"}
	sup := &StateMachineSupervisor{
		FlagRules: []FlagRule{
			{Flag: "register_complete", Phase: "complete"},
		},
		DefaultPhase: "diagnostic",
		Router:       router,
	}

	store := NewMemoryStore()
	store.SetState("register_complete", true)

	phase, _, err := sup.DecideNextStep(context.Background(), store, "dame los certificados organicos", "", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if phase != "info" {
		t.Errorf("expected router to override flag rules at turn start, got %s", phase)
	}
}

func TestStateMachine_FlagRulesApplyMidFlow(t *testing.T) {
	// Mid-flow (currentPhase != ""), flag rules should still work — the router is NOT consulted.
	router := &mockRouter{target: "info"}
	sup := &StateMachineSupervisor{
		FlagRules: []FlagRule{
			{Flag: "register_complete", Phase: "complete"},
		},
		DefaultPhase: "diagnostic",
		Router:       router,
	}

	store := NewMemoryStore()
	store.SetState("register_complete", true)

	// Mid-flow: currentPhase="register", flag says go to "complete"
	phase, _, err := sup.DecideNextStep(context.Background(), store, "", "register", EventStepSuccess)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if phase != "complete" {
		t.Errorf("expected flag rule to force complete mid-flow, got %s", phase)
	}
}

func TestStateMachine_RouterError_FallsBackToFlagRules(t *testing.T) {
	// If the router fails at turn start, flag rules should be used as fallback.
	router := &mockRouter{err: fmt.Errorf("timeout")}
	sup := &StateMachineSupervisor{
		FlagRules: []FlagRule{
			{Flag: "register_complete", Phase: "complete"},
		},
		DefaultPhase: "diagnostic",
		Router:       router,
	}

	store := NewMemoryStore()
	store.SetState("register_complete", true)

	phase, _, err := sup.DecideNextStep(context.Background(), store, "hola", "", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if phase != "complete" {
		t.Errorf("expected flag rules as router fallback, got %s", phase)
	}
}

func TestStateMachine_PhaseCompleteOnSamePhaseWithTransition(t *testing.T) {
	sup := &StateMachineSupervisor{
		Transitions:  []TransitionRule{{From: "a", To: "b"}},
		DefaultPhase: "a",
	}

	// Phase "a" completes → should transition to "b" even though "a" is current
	phase, _, err := sup.DecideNextStep(context.Background(), NewMemoryStore(), "", "a", EventPhaseComplete)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if phase != "b" {
		t.Errorf("expected transition to b on phase_complete, got %s", phase)
	}
}

func TestLinear_SinglePhase(t *testing.T) {
	sup := NewLinearSupervisor("only")

	// Start
	phase, _, err := sup.DecideNextStep(context.Background(), nil, "", "", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if phase != "only" {
		t.Errorf("expected only, got %s", phase)
	}

	// PhaseComplete on single phase → stays
	phase, _, _ = sup.DecideNextStep(context.Background(), nil, "", "only", EventPhaseComplete)
	if phase != "only" {
		t.Errorf("expected to stay at only, got %s", phase)
	}
}
