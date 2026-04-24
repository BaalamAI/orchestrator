package orchestrator

import (
	"context"
	"fmt"
	"sync"
)

// --- StateMachineSupervisor ---

// FlagRule maps a state flag to the phase it should force.
type FlagRule struct {
	// Flag is the state key to check (e.g. "register_complete", "diagnostic_complete").
	Flag string
	// Phase is the phase to force when the flag is true.
	Phase Phase
}

// TransitionRule maps a completed phase to the next phase.
type TransitionRule struct {
	// From is the phase that must complete (EventPhaseComplete) to trigger this rule.
	From Phase
	// To is the phase to execute next.
	To Phase
}

// ConditionalTransition adds a condition check on top of a transition.
type ConditionalTransition struct {
	// From is the phase that must complete to evaluate this rule.
	From Phase
	// To is the target phase if Condition returns true.
	To Phase
	// Condition receives the current store and must return true for the transition to fire.
	Condition func(store StateStore) bool
}

// StateMachineSupervisor combines deterministic rules with optional LLM routing.
// Decision priority: (1) event transitions on phase_complete, (2) state flag rules
// ordered by priority (first match wins), (3) LLM intent routing at turn start.
type StateMachineSupervisor struct {
	// Transitions defines deterministic phase-to-phase routes triggered on EventPhaseComplete.
	// Example: {From: "diagnostic", To: "payment"} means completing diagnostic cascades to payment.
	Transitions []TransitionRule

	// ConditionalTransitions are like Transitions but with an extra condition check.
	// Evaluated before simple Transitions. The Condition func receives the current store
	// and must return true for the transition to fire.
	ConditionalTransitions []ConditionalTransition

	// FlagRules maps state flags to forced phases. Evaluated in order; first match wins.
	// Example: {Flag: "register_complete", Phase: "complete"} forces the "complete" phase
	// when the "register_complete" flag is true in the store.
	FlagRules []FlagRule

	// DefaultPhase is the fallback phase when no rules match and no Router is configured.
	// Also used as the starting phase on the first turn if no flags apply.
	DefaultPhase Phase

	// Router is an optional LLM-based intent classifier. Only invoked at turn start
	// (currentPhase == "") when no deterministic rules matched. If nil, DefaultPhase is used.
	Router IntentRouter

	mu        sync.Mutex
	lastUsage *Usage
}

// Usage returns and clears the accumulated token usage from the last decision.
func (s *StateMachineSupervisor) Usage() *Usage {
	s.mu.Lock()
	defer s.mu.Unlock()
	u := s.lastUsage
	s.lastUsage = nil
	return u
}

// DecideNextStep applies the 3-level decision hierarchy:
// 1. Event transitions (deterministic, on phase_complete)
// 2. At turn start: LLM intent routing first (allows new queries after flow completion)
// 3. Mid-flow: state flag rules (ordered by priority, first match wins)
func (s *StateMachineSupervisor) DecideNextStep(ctx context.Context, store StateStore, userText string, currentPhase Phase, lastEvent EventType) (Phase, string, error) {
	// 1. Event transitions
	if lastEvent == EventPhaseComplete {
		// Check conditional transitions first
		for _, ct := range s.ConditionalTransitions {
			if ct.From == currentPhase && ct.Condition(store) {
				return ct.To, fmt.Sprintf("Event table: %s + phase_complete + condition -> %s", currentPhase, ct.To), nil
			}
		}
		// Then simple transitions
		for _, t := range s.Transitions {
			if t.From == currentPhase {
				return t.To, fmt.Sprintf("Event table: %s + phase_complete -> %s", currentPhase, t.To), nil
			}
		}
	}

	// 2. At turn start → always ask the router first.
	// This lets users start new queries (info, tool, new diagnostic) even when
	// flags from a previous flow (e.g. register_complete) are still in state.
	if currentPhase == "" && s.Router != nil {
		routedPhase, err := s.Router.Route(ctx, store, userText)
		if err == nil {
			s.mu.Lock()
			s.lastUsage = s.Router.Usage()
			s.mu.Unlock()
			return routedPhase, fmt.Sprintf("Router intent classification: %s", routedPhase), nil
		}
		// On router failure, fall through to flag rules as fallback
	}

	// 3. State flag rules (mid-flow, or turn-start fallback when router fails/absent)
	selectedPhase := s.DefaultPhase

	for _, rule := range s.FlagRules {
		if store.HasFlag(rule.Flag) {
			selectedPhase = rule.Phase
			break
		}
	}

	if selectedPhase != s.DefaultPhase {
		if currentPhase == "" || currentPhase != selectedPhase {
			return selectedPhase, fmt.Sprintf("State flag forced transition to: %s", selectedPhase), nil
		}
	}

	// 4. Default: stay in current phase or go to default
	if currentPhase == "" {
		return s.DefaultPhase, "No rules matched, using default phase", nil
	}
	return currentPhase, "Continuing current phase", nil
}

// --- LinearSupervisor ---

// LinearSupervisor executes phases in a fixed sequence, advancing
// to the next phase each time the current one emits EventPhaseComplete.
type LinearSupervisor struct {
	phases []Phase
	index  int
}

// NewLinearSupervisor creates a supervisor that steps through phases in order.
func NewLinearSupervisor(phases ...Phase) *LinearSupervisor {
	return &LinearSupervisor{phases: phases}
}

// Usage always returns nil since LinearSupervisor does not use LLM calls.
func (s *LinearSupervisor) Usage() *Usage {
	return nil
}

// DecideNextStep returns the current phase or advances to the next on EventPhaseComplete.
func (s *LinearSupervisor) DecideNextStep(_ context.Context, _ StateStore, _ string, currentPhase Phase, lastEvent EventType) (Phase, string, error) {
	if len(s.phases) == 0 {
		return "", "no phases configured", fmt.Errorf("linear supervisor: no phases configured")
	}

	// First call or phase completed → advance
	if currentPhase == "" {
		s.index = 0
		return s.phases[0], "Linear: starting first phase", nil
	}

	if lastEvent == EventPhaseComplete && s.index < len(s.phases)-1 {
		s.index++
		return s.phases[s.index], fmt.Sprintf("Linear: advancing to phase %d", s.index), nil
	}

	return currentPhase, "Linear: continuing current phase", nil
}
