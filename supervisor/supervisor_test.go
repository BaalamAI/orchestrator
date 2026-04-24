package supervisor

import (
	"context"
	"fmt"
	"testing"

	"github.com/baalamai/orchestrator"
	"github.com/baalamai/orchestrator/store"
)

// --- Mock IntentRouter ---

type mockRouter struct {
	target orchestrator.Phase
	err    error
	usage  *orchestrator.Usage
}

func (m *mockRouter) Route(_ context.Context, _ orchestrator.StateStore, _ string) (orchestrator.Phase, error) {
	return m.target, m.err
}

func (m *mockRouter) Usage() *orchestrator.Usage {
	return m.usage
}

// --- StateMachine Tests ---

func TestStateMachine_TransitionOnPhaseComplete(t *testing.T) {
	sup := &StateMachine{
		Transitions: []TransitionRule{
			{From: "diagnostic", To: "payment"},
			{From: "payment", To: "register"},
		},
		DefaultPhase: "diagnostic",
	}

	phase, _, err := sup.DecideNextStep(context.Background(), store.NewMemory(), "", "diagnostic", orchestrator.EventPhaseComplete)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if phase != "payment" {
		t.Errorf("expected payment, got %s", phase)
	}
}

func TestStateMachine_ConditionalTransition(t *testing.T) {
	sup := &StateMachine{
		ConditionalTransitions: []ConditionalTransition{
			{
				From:      "payment",
				To:        "register",
				Condition: func(s orchestrator.StateStore) bool { return s.HasFlag("chosen_payment") },
			},
		},
		Transitions: []TransitionRule{
			{From: "payment", To: "complete"},
		},
		DefaultPhase: "diagnostic",
	}

	st := store.NewMemory()

	// Without flag → falls to simple transition
	phase, _, _ := sup.DecideNextStep(context.Background(), st, "", "payment", orchestrator.EventPhaseComplete)
	if phase != "complete" {
		t.Errorf("expected complete (simple transition), got %s", phase)
	}

	// With flag → conditional transition
	st.SetState("chosen_payment", true)
	phase, _, _ = sup.DecideNextStep(context.Background(), st, "", "payment", orchestrator.EventPhaseComplete)
	if phase != "register" {
		t.Errorf("expected register (conditional), got %s", phase)
	}
}

func TestStateMachine_FlagRules(t *testing.T) {
	sup := &StateMachine{
		FlagRules: []FlagRule{
			{Flag: "register_complete", Phase: "complete"},
			{Flag: "chosen_payment", Phase: "register"},
			{Flag: "diagnostic_complete", Phase: "payment"},
		},
		DefaultPhase: "diagnostic",
	}

	st := store.NewMemory()
	st.SetState("diagnostic_complete", true)

	// First matching flag wins
	phase, _, _ := sup.DecideNextStep(context.Background(), st, "", "", orchestrator.EventStepSuccess)
	if phase != "payment" {
		t.Errorf("expected payment from flag rule, got %s", phase)
	}

	// Higher priority flag takes precedence
	st.SetState("register_complete", true)
	phase, _, _ = sup.DecideNextStep(context.Background(), st, "", "", orchestrator.EventStepSuccess)
	if phase != "complete" {
		t.Errorf("expected complete from higher priority flag, got %s", phase)
	}
}

func TestStateMachine_FlagRuleSamePhase(t *testing.T) {
	sup := &StateMachine{
		FlagRules: []FlagRule{
			{Flag: "in_payment", Phase: "payment"},
		},
		DefaultPhase: "diagnostic",
	}

	st := store.NewMemory()
	st.SetState("in_payment", true)

	// When already in the same phase, flag rule doesn't re-enter
	phase, _, _ := sup.DecideNextStep(context.Background(), st, "", "payment", orchestrator.EventStepSuccess)
	if phase != "payment" {
		t.Errorf("expected to stay in payment, got %s", phase)
	}
}

func TestStateMachine_RouterAtTurnStart(t *testing.T) {
	router := &mockRouter{
		target: "quote",
		usage:  &orchestrator.Usage{PromptTokens: 100, CompletionTokens: 50, TotalTokens: 150},
	}
	sup := &StateMachine{
		DefaultPhase: "diagnostic",
		Router:       router,
	}

	phase, _, err := sup.DecideNextStep(context.Background(), store.NewMemory(), "cuanto cuesta?", "", "")
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
	sup := &StateMachine{
		DefaultPhase: "diagnostic",
		Router:       router,
	}

	phase, _, err := sup.DecideNextStep(context.Background(), store.NewMemory(), "hola", "", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if phase != "diagnostic" {
		t.Errorf("expected diagnostic fallback, got %s", phase)
	}
}

func TestStateMachine_NoRouterNoFlags_Default(t *testing.T) {
	sup := &StateMachine{
		DefaultPhase: "diagnostic",
	}

	phase, _, _ := sup.DecideNextStep(context.Background(), store.NewMemory(), "hola", "", "")
	if phase != "diagnostic" {
		t.Errorf("expected diagnostic default, got %s", phase)
	}
}

func TestStateMachine_ContinueCurrentPhase(t *testing.T) {
	sup := &StateMachine{
		DefaultPhase: "diagnostic",
	}

	phase, _, _ := sup.DecideNextStep(context.Background(), store.NewMemory(), "", "info", orchestrator.EventStepSuccess)
	if phase != "info" {
		t.Errorf("expected to continue in info, got %s", phase)
	}
}

// --- Linear Tests ---

func TestLinear_StartsFirstPhase(t *testing.T) {
	sup := NewLinear("a", "b", "c")

	phase, _, err := sup.DecideNextStep(context.Background(), nil, "", "", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if phase != "a" {
		t.Errorf("expected first phase a, got %s", phase)
	}
}

func TestLinear_AdvancesOnPhaseComplete(t *testing.T) {
	sup := NewLinear("a", "b", "c")

	// Start
	sup.DecideNextStep(context.Background(), nil, "", "", "")

	// Advance to b
	phase, _, _ := sup.DecideNextStep(context.Background(), nil, "", "a", orchestrator.EventPhaseComplete)
	if phase != "b" {
		t.Errorf("expected b, got %s", phase)
	}

	// Advance to c
	phase, _, _ = sup.DecideNextStep(context.Background(), nil, "", "b", orchestrator.EventPhaseComplete)
	if phase != "c" {
		t.Errorf("expected c, got %s", phase)
	}

	// Stay at c (last phase)
	phase, _, _ = sup.DecideNextStep(context.Background(), nil, "", "c", orchestrator.EventPhaseComplete)
	if phase != "c" {
		t.Errorf("expected to stay at c, got %s", phase)
	}
}

func TestLinear_StaysWithoutPhaseComplete(t *testing.T) {
	sup := NewLinear("a", "b")
	sup.DecideNextStep(context.Background(), nil, "", "", "")

	phase, _, _ := sup.DecideNextStep(context.Background(), nil, "", "a", orchestrator.EventStepSuccess)
	if phase != "a" {
		t.Errorf("expected to stay at a without phase_complete, got %s", phase)
	}
}

func TestLinear_NoPhases_Error(t *testing.T) {
	sup := NewLinear()
	_, _, err := sup.DecideNextStep(context.Background(), nil, "", "", "")
	if err == nil {
		t.Error("expected error for empty phases")
	}
}

func TestLinear_Usage_ReturnsNil(t *testing.T) {
	sup := NewLinear("a")
	if sup.Usage() != nil {
		t.Error("expected nil usage")
	}
}

func TestStateMachine_RouterOverridesFlagsAtTurnStart(t *testing.T) {
	// When a router is configured and it's turn start (currentPhase==""),
	// the router should decide the phase — even if flag rules would force a different one.
	// This lets users ask new questions after completing a flow (e.g. info query after register_complete).
	router := &mockRouter{target: "info"}
	sup := &StateMachine{
		FlagRules: []FlagRule{
			{Flag: "register_complete", Phase: "complete"},
		},
		DefaultPhase: "diagnostic",
		Router:       router,
	}

	st := store.NewMemory()
	st.SetState("register_complete", true)

	phase, _, err := sup.DecideNextStep(context.Background(), st, "dame los certificados organicos", "", "")
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
	sup := &StateMachine{
		FlagRules: []FlagRule{
			{Flag: "register_complete", Phase: "complete"},
		},
		DefaultPhase: "diagnostic",
		Router:       router,
	}

	st := store.NewMemory()
	st.SetState("register_complete", true)

	// Mid-flow: currentPhase="register", flag says go to "complete"
	phase, _, err := sup.DecideNextStep(context.Background(), st, "", "register", orchestrator.EventStepSuccess)
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
	sup := &StateMachine{
		FlagRules: []FlagRule{
			{Flag: "register_complete", Phase: "complete"},
		},
		DefaultPhase: "diagnostic",
		Router:       router,
	}

	st := store.NewMemory()
	st.SetState("register_complete", true)

	phase, _, err := sup.DecideNextStep(context.Background(), st, "hola", "", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if phase != "complete" {
		t.Errorf("expected flag rules as router fallback, got %s", phase)
	}
}

func TestStateMachine_PhaseCompleteOnSamePhaseWithTransition(t *testing.T) {
	sup := &StateMachine{
		Transitions:  []TransitionRule{{From: "a", To: "b"}},
		DefaultPhase: "a",
	}

	// Phase "a" completes → should transition to "b" even though "a" is current
	phase, _, err := sup.DecideNextStep(context.Background(), store.NewMemory(), "", "a", orchestrator.EventPhaseComplete)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if phase != "b" {
		t.Errorf("expected transition to b on phase_complete, got %s", phase)
	}
}

func TestLinear_SinglePhase(t *testing.T) {
	sup := NewLinear("only")

	// Start
	phase, _, err := sup.DecideNextStep(context.Background(), nil, "", "", "")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if phase != "only" {
		t.Errorf("expected only, got %s", phase)
	}

	// PhaseComplete on single phase → stays
	phase, _, _ = sup.DecideNextStep(context.Background(), nil, "", "only", orchestrator.EventPhaseComplete)
	if phase != "only" {
		t.Errorf("expected to stay at only, got %s", phase)
	}
}
