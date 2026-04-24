// Package supervisor provides concrete Supervisor implementations that drive
// the orchestrator's phase-selection loop:
//
//   - StateMachine: deterministic rules + optional LLM intent routing.
//   - Linear: fixed sequence of phases.
//
// Both implementations also satisfy orchestrator.SupervisorValidator so the
// PipelineBuilder can validate that every referenced phase has a registered node.
package supervisor

import (
	"context"
	"fmt"
	"sync"

	"github.com/baalamai/orchestrator"
)

// FlagRule maps a state flag to the phase it should force.
type FlagRule struct {
	// Flag is the state key to check (e.g. "register_complete", "diagnostic_complete").
	Flag string
	// Phase is the phase to force when the flag is true.
	Phase orchestrator.Phase
}

// TransitionRule maps a completed phase to the next phase.
type TransitionRule struct {
	// From is the phase that must complete (EventPhaseComplete) to trigger this rule.
	From orchestrator.Phase
	// To is the phase to execute next.
	To orchestrator.Phase
}

// ConditionalTransition adds a condition check on top of a transition.
type ConditionalTransition struct {
	// From is the phase that must complete to evaluate this rule.
	From orchestrator.Phase
	// To is the target phase if Condition returns true.
	To orchestrator.Phase
	// Condition receives the current store and must return true for the transition to fire.
	Condition func(store orchestrator.StateStore) bool
}

// StateMachine combines deterministic rules with optional LLM routing.
// Decision priority: (1) event transitions on phase_complete, (2) state flag rules
// ordered by priority (first match wins), (3) LLM intent routing at turn start.
type StateMachine struct {
	// Transitions defines deterministic phase-to-phase routes triggered on EventPhaseComplete.
	Transitions []TransitionRule

	// ConditionalTransitions are like Transitions but with an extra condition check.
	// Evaluated before simple Transitions.
	ConditionalTransitions []ConditionalTransition

	// FlagRules maps state flags to forced phases. Evaluated in order; first match wins.
	FlagRules []FlagRule

	// DefaultPhase is the fallback phase when no rules match and no Router is configured.
	// Also used as the starting phase on the first turn if no flags apply.
	DefaultPhase orchestrator.Phase

	// Router is an optional LLM-based intent classifier. Only invoked at turn start
	// (currentPhase == "") when no deterministic rules matched.
	Router orchestrator.IntentRouter

	mu        sync.Mutex
	lastUsage *orchestrator.Usage
}

// Usage returns and clears the accumulated token usage from the last decision.
func (s *StateMachine) Usage() *orchestrator.Usage {
	s.mu.Lock()
	defer s.mu.Unlock()
	u := s.lastUsage
	s.lastUsage = nil
	return u
}

// DecideNextStep applies the 3-level decision hierarchy:
//  1. Event transitions (deterministic, on phase_complete)
//  2. At turn start: LLM intent routing first (allows new queries after flow completion)
//  3. Mid-flow: state flag rules (ordered by priority, first match wins)
func (s *StateMachine) DecideNextStep(ctx context.Context, store orchestrator.StateStore, userText string, currentPhase orchestrator.Phase, lastEvent orchestrator.EventType) (orchestrator.Phase, string, error) {
	// 1. Event transitions
	if lastEvent == orchestrator.EventPhaseComplete {
		for _, ct := range s.ConditionalTransitions {
			if ct.From == currentPhase && ct.Condition(store) {
				return ct.To, fmt.Sprintf("Event table: %s + phase_complete + condition -> %s", currentPhase, ct.To), nil
			}
		}
		for _, t := range s.Transitions {
			if t.From == currentPhase {
				return t.To, fmt.Sprintf("Event table: %s + phase_complete -> %s", currentPhase, t.To), nil
			}
		}
	}

	// 2. At turn start → always ask the router first.
	if currentPhase == "" && s.Router != nil {
		routedPhase, err := s.Router.Route(ctx, store, userText)
		if err == nil {
			s.mu.Lock()
			s.lastUsage = s.Router.Usage()
			s.mu.Unlock()
			return routedPhase, fmt.Sprintf("Router intent classification: %s", routedPhase), nil
		}
	}

	// 3. State flag rules
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

	// 4. Default
	if currentPhase == "" {
		return s.DefaultPhase, "No rules matched, using default phase", nil
	}
	return currentPhase, "Continuing current phase", nil
}

// ValidatePhases implements orchestrator.SupervisorValidator. It checks that
// every phase referenced by the state machine has a registered node; the
// builder calls this during Build().
func (s *StateMachine) ValidatePhases(hasPhase func(string) bool) error {
	if s.DefaultPhase == "" {
		return fmt.Errorf("state machine default phase is required")
	}
	if !hasPhase(s.DefaultPhase) {
		return fmt.Errorf("default phase %q has no registered agent", s.DefaultPhase)
	}
	for _, rule := range s.FlagRules {
		if !hasPhase(rule.Phase) {
			return fmt.Errorf("flag rule target phase %q has no registered agent", rule.Phase)
		}
	}
	for _, t := range s.Transitions {
		if !hasPhase(t.From) {
			return fmt.Errorf("transition source phase %q has no registered agent", t.From)
		}
		if !hasPhase(t.To) {
			return fmt.Errorf("transition target phase %q has no registered agent", t.To)
		}
	}
	for _, ct := range s.ConditionalTransitions {
		if !hasPhase(ct.From) {
			return fmt.Errorf("conditional transition source phase %q has no registered agent", ct.From)
		}
		if !hasPhase(ct.To) {
			return fmt.Errorf("conditional transition target phase %q has no registered agent", ct.To)
		}
	}
	return nil
}
