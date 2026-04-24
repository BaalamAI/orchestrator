package orchestrator

import (
	"context"
	"time"
)

// TurnPredicate is a simple predicate that inspects only the Turn.
// Usable with all Conditional* wrappers via adapter functions below.
type TurnPredicate func(turn *Turn) bool

// ConditionalPreprocess wraps a PreprocessHook with a predicate.
// The hook only runs when shouldRun returns true.
func ConditionalPreprocess(shouldRun TurnPredicate, hook PreprocessHook) PreprocessHook {
	return func(ctx context.Context, store StateStore, turn *Turn) error {
		if !shouldRun(turn) {
			return nil
		}
		return hook(ctx, store, turn)
	}
}

// ConditionalPostprocess wraps a PostprocessHook with a predicate.
func ConditionalPostprocess(shouldRun TurnPredicate, hook PostprocessHook) PostprocessHook {
	return func(ctx context.Context, store StateStore, turn *Turn, result *PipelineResult) error {
		if !shouldRun(turn) {
			return nil
		}
		return hook(ctx, store, turn, result)
	}
}

// ConditionalSupervisorHook wraps a SupervisorHook with a predicate.
func ConditionalSupervisorHook(shouldRun TurnPredicate, hook SupervisorHook) SupervisorHook {
	return func(ctx context.Context, store StateStore, turn *Turn, step int, phase Phase, reason string, usage *Usage) error {
		if !shouldRun(turn) {
			return nil
		}
		return hook(ctx, store, turn, step, phase, reason, usage)
	}
}

// ConditionalPostAgent wraps a PostAgentHook with a predicate.
func ConditionalPostAgent(shouldRun TurnPredicate, hook PostAgentHook) PostAgentHook {
	return func(ctx context.Context, view StateView, turn *Turn, result *NodeResult, phase string) error {
		if !shouldRun(turn) {
			return nil
		}
		return hook(ctx, view, turn, result, phase)
	}
}

// WithTimeout wraps a PreprocessHook with a context timeout.
func WithTimeout(timeout time.Duration, hook PreprocessHook) PreprocessHook {
	return func(ctx context.Context, store StateStore, turn *Turn) error {
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		return hook(ctx, store, turn)
	}
}

// WithPostprocessTimeout wraps a PostprocessHook with a context timeout.
func WithPostprocessTimeout(timeout time.Duration, hook PostprocessHook) PostprocessHook {
	return func(ctx context.Context, store StateStore, turn *Turn, result *PipelineResult) error {
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		return hook(ctx, store, turn, result)
	}
}

// ── Common predicates ───────────────────────────────────────────────

// WhenChannel returns a predicate that matches a specific channel.
func WhenChannel(channel string) TurnPredicate {
	return func(turn *Turn) bool {
		return turn.Channel == channel
	}
}

// WhenNotChannel returns a predicate that excludes a specific channel.
func WhenNotChannel(channel string) TurnPredicate {
	return func(turn *Turn) bool {
		return turn.Channel != channel
	}
}
