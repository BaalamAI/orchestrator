package orchestrator

import "context"

// ── Hook signatures ──────────────────────────────────────────────────
// The Engine exposes several lifecycle hooks that callers can plug in via
// the PipelineBuilder. Hook type signatures live here; higher-order helpers
// to compose and guard these hooks live in the hook/ subpackage.

// PreprocessHook runs before the main execution loop.
// Errors are logged but do not stop the pipeline.
type PreprocessHook func(ctx context.Context, store StateStore, turn *Turn) error

// FatalPreprocessHook runs before the main loop. Errors stop the pipeline.
type FatalPreprocessHook func(ctx context.Context, store StateStore, turn *Turn) error

// PostprocessHook runs after the main execution loop.
type PostprocessHook func(ctx context.Context, store StateStore, turn *Turn, result *PipelineResult) error

// SupervisorHook runs after each supervisor decision in the execution loop.
// Errors are logged but do not stop the pipeline.
type SupervisorHook func(ctx context.Context, store StateStore, turn *Turn, step int, phase Phase, reason string, usage *Usage) error

// PreAgentHook runs before each agent in the execution loop.
type PreAgentHook func(ctx context.Context, view StateView, turn *Turn, phase string) error

// PostAgentHook runs after each agent in the execution loop.
type PostAgentHook func(ctx context.Context, view StateView, turn *Turn, result *NodeResult, phase string) error

// Logger is an optional structured logger for the engine.
type Logger interface {
	Info(ctx context.Context, msg string, args ...any)
	Error(ctx context.Context, msg string, args ...any)
	Warn(ctx context.Context, msg string, args ...any)
}
