package supervisor

import (
	"context"
	"fmt"

	"github.com/baalamai/orchestrator"
)

// Linear executes phases in a fixed sequence, advancing to the next phase each
// time the current one emits EventPhaseComplete.
type Linear struct {
	phases []orchestrator.Phase
}

// NewLinear creates a supervisor that steps through phases in order.
func NewLinear(phases ...orchestrator.Phase) *Linear {
	return &Linear{phases: phases}
}

// Usage always returns nil since Linear does not use LLM calls.
func (s *Linear) Usage() *orchestrator.Usage {
	return nil
}

// DecideNextStep returns the current phase or advances to the next on EventPhaseComplete.
func (s *Linear) DecideNextStep(_ context.Context, _ orchestrator.StateStore, _ string, currentPhase orchestrator.Phase, lastEvent orchestrator.EventType) (orchestrator.Phase, string, error) {
	if len(s.phases) == 0 {
		return "", "no phases configured", fmt.Errorf("linear supervisor: no phases configured")
	}

	if currentPhase == "" {
		return s.phases[0], "Linear: starting first phase", nil
	}

	if lastEvent == orchestrator.EventPhaseComplete {
		for i, phase := range s.phases {
			if phase != currentPhase {
				continue
			}
			if i < len(s.phases)-1 {
				return s.phases[i+1], fmt.Sprintf("Linear: advancing to phase %d", i+1), nil
			}
			break
		}
	}

	return currentPhase, "Linear: continuing current phase", nil
}

// ValidatePhases implements orchestrator.SupervisorValidator.
func (s *Linear) ValidatePhases(hasPhase func(string) bool) error {
	for _, p := range s.phases {
		if !hasPhase(p) {
			return fmt.Errorf("linear phase %q has no registered agent", p)
		}
	}
	return nil
}

// Phases returns a copy of the configured phase sequence.
func (s *Linear) Phases() []orchestrator.Phase {
	cp := make([]orchestrator.Phase, len(s.phases))
	copy(cp, s.phases)
	return cp
}
