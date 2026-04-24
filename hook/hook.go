// Package hook provides composition helpers for orchestrator lifecycle hooks.
//
// Unlike the hook type signatures in the root package (PreprocessHook,
// PostprocessHook, etc.), these helpers are higher-order utilities: they wrap
// an existing hook with a predicate, timeout, or aggregation strategy.
//
// Typical usage:
//
//	builder.OnPreprocess(
//	    hook.ConditionalPreprocess(hook.WhenChannel("whatsapp"), whatsappPreprocess),
//	)
package hook

import (
	"context"
	"time"

	"github.com/baalamai/orchestrator"
)

// TurnPredicate is a simple predicate that inspects only the Turn.
// Usable with all Conditional* wrappers.
type TurnPredicate func(turn *orchestrator.Turn) bool

// ConditionalPreprocess wraps a PreprocessHook with a predicate.
// The hook only runs when shouldRun returns true.
func ConditionalPreprocess(shouldRun TurnPredicate, h orchestrator.PreprocessHook) orchestrator.PreprocessHook {
	return func(ctx context.Context, store orchestrator.StateStore, turn *orchestrator.Turn) error {
		if !shouldRun(turn) {
			return nil
		}
		return h(ctx, store, turn)
	}
}

// ConditionalPostprocess wraps a PostprocessHook with a predicate.
func ConditionalPostprocess(shouldRun TurnPredicate, h orchestrator.PostprocessHook) orchestrator.PostprocessHook {
	return func(ctx context.Context, store orchestrator.StateStore, turn *orchestrator.Turn, result *orchestrator.PipelineResult) error {
		if !shouldRun(turn) {
			return nil
		}
		return h(ctx, store, turn, result)
	}
}

// ConditionalSupervisor wraps a SupervisorHook with a predicate.
func ConditionalSupervisor(shouldRun TurnPredicate, h orchestrator.SupervisorHook) orchestrator.SupervisorHook {
	return func(ctx context.Context, store orchestrator.StateStore, turn *orchestrator.Turn, step int, phase orchestrator.Phase, reason string, usage *orchestrator.Usage) error {
		if !shouldRun(turn) {
			return nil
		}
		return h(ctx, store, turn, step, phase, reason, usage)
	}
}

// ConditionalPostAgent wraps a PostAgentHook with a predicate.
func ConditionalPostAgent(shouldRun TurnPredicate, h orchestrator.PostAgentHook) orchestrator.PostAgentHook {
	return func(ctx context.Context, view orchestrator.StateView, turn *orchestrator.Turn, result *orchestrator.NodeResult, phase string) error {
		if !shouldRun(turn) {
			return nil
		}
		return h(ctx, view, turn, result, phase)
	}
}

// WithTimeout wraps a PreprocessHook with a context timeout.
func WithTimeout(timeout time.Duration, h orchestrator.PreprocessHook) orchestrator.PreprocessHook {
	return func(ctx context.Context, store orchestrator.StateStore, turn *orchestrator.Turn) error {
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		return h(ctx, store, turn)
	}
}

// WithPostprocessTimeout wraps a PostprocessHook with a context timeout.
func WithPostprocessTimeout(timeout time.Duration, h orchestrator.PostprocessHook) orchestrator.PostprocessHook {
	return func(ctx context.Context, store orchestrator.StateStore, turn *orchestrator.Turn, result *orchestrator.PipelineResult) error {
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		return h(ctx, store, turn, result)
	}
}

// WhenChannel returns a predicate that matches a specific channel.
func WhenChannel(channel string) TurnPredicate {
	return func(turn *orchestrator.Turn) bool {
		return turn.Channel == channel
	}
}

// WhenNotChannel returns a predicate that excludes a specific channel.
func WhenNotChannel(channel string) TurnPredicate {
	return func(turn *orchestrator.Turn) bool {
		return turn.Channel != channel
	}
}
