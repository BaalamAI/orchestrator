package orchestrator

import (
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// NodeConfig declares a single phase node and its middleware.
type NodeConfig struct {
	// Phase is the name that identifies this node (e.g. "diagnostic", "payment").
	// The supervisor uses this key to route execution to the correct agent.
	Phase string
	// Description is a human-readable summary of what this node does.
	// Useful for logging, debugging, and self-documenting pipeline configs.
	Description string
	// Fn is the pure-function agent that executes when this phase is selected.
	// It receives a read-only StateView and returns a NodeResult with deltas.
	Fn NodeFunc
	// Middleware wraps Fn with pre/post behavior (capture, RAG, logging).
	// Executed in order: first middleware is the outermost wrapper.
	Middleware []AgentMiddleware
	// Concurrent marks this node as safe for parallel execution.
	// NOTE: Only used when the supervisor implements ParallelSupervisor.
	Concurrent bool
	// Repeatable allows this phase to be re-entered across turns
	// without requiring EventPhaseComplete from the previous execution.
	Repeatable bool
}

// HookConfig groups all lifecycle hooks for a pipeline.
// WARN: Slice order matters — hooks execute sequentially in the order provided.
type HookConfig struct {
	// FatalPreprocessHooks run before the first supervisor call.
	// WARN: errors here stop the pipeline immediately — use for mandatory guards.
	FatalPreprocessHooks []FatalPreprocessHook
	// PreprocessHooks run before the first supervisor call.
	// Errors are logged but do not stop the pipeline (best-effort).
	PreprocessHooks []PreprocessHook
	// SupervisorHooks run after each supervisor decision in the execution loop.
	SupervisorHooks []SupervisorHook
	// PreAgentHooks run before each agent execution in the loop.
	PreAgentHooks []PreAgentHook
	// PostAgentHooks run after each agent execution in the loop.
	PostAgentHooks []PostAgentHook
	// PostprocessHooks run after the final agent result, before returning to the caller.
	PostprocessHooks []PostprocessHook
}

// PipelineConfig is a declarative description of a pipeline.
// The services layer fills in concrete Supervisor and hook implementations;
// shared/orchestrator only holds interfaces, preserving hexagonal boundaries.
type PipelineConfig struct {
	// MaxSteps caps the supervisor→agent cascade depth per turn, preventing runaway loops.
	MaxSteps int
	// Supervisor decides which phase to execute next in each iteration of the engine loop.
	Supervisor Supervisor
	// Logger receives structured log output from the engine (info, warn, error).
	Logger Logger
	// Classifier maps provider-specific errors to retry categories. Optional;
	// defaults to noopClassifier (treats every error as permanent).
	Classifier ErrorClassifier
	// Cost computes USD cost from usage for MaxCostUSD budget enforcement.
	// Optional; defaults to noopCostCalculator (always 0).
	Cost CostCalculator
	// Tracer emits OpenTelemetry spans for turn, supervisor, node, and attempt.
	// Optional; defaults to a noop tracer (zero overhead).
	Tracer trace.Tracer
	// Meter emits counters and histograms for tokens, cost, phase duration, retries.
	// Optional; defaults to a noop meter (zero overhead).
	Meter metric.Meter
	// Checkpoints enables mid-turn durability: the engine saves after each phase
	// and resumes from the last saved point on Run() if a checkpoint exists for
	// the Turn.TurnID. Optional; nil disables checkpointing.
	//
	// Idempotency contract: phases replayed after resume must be idempotent.
	// Tools with visible side-effects (send message, create payment link) must
	// deduplicate internally by natural key — the engine does NOT track which
	// tools already ran pre-crash. See lib/orchestrator/checkpoint.go docs.
	Checkpoints CheckpointStore
	// Nodes is the ordered list of phase nodes registered in the pipeline.
	// Each node maps a phase name to a pure-function agent with optional middleware.
	Nodes []NodeConfig
	// Hooks contains all lifecycle hooks grouped by execution stage.
	Hooks HookConfig
}

// BuildFromConfig constructs an Engine from a declarative PipelineConfig.
// NOTE: This is a thin translation layer — all validation is delegated to Build().
func BuildFromConfig(cfg PipelineConfig) (*Engine, error) {
	b := NewPipelineBuilder()
	if cfg.MaxSteps > 0 {
		b.MaxSteps(cfg.MaxSteps)
	}
	if cfg.Supervisor != nil {
		b.WithSupervisor(cfg.Supervisor)
	}
	if cfg.Logger != nil {
		b.WithLogger(cfg.Logger)
	}
	if cfg.Classifier != nil {
		b.WithClassifier(cfg.Classifier)
	}
	if cfg.Cost != nil {
		b.WithCostCalculator(cfg.Cost)
	}
	if cfg.Tracer != nil {
		b.WithTracer(cfg.Tracer)
	}
	if cfg.Meter != nil {
		b.WithMeter(cfg.Meter)
	}
	if cfg.Checkpoints != nil {
		b.WithCheckpoints(cfg.Checkpoints)
	}

	var repeatables []string
	for _, n := range cfg.Nodes {
		if n.Concurrent {
			b.RegisterConcurrentNode(n.Phase, n.Fn, n.Middleware...)
		} else {
			b.RegisterNode(n.Phase, n.Fn, n.Middleware...)
		}
		if n.Repeatable {
			repeatables = append(repeatables, n.Phase)
		}
	}
	b.Repeatable(repeatables...)

	b.OnFatalPreprocess(cfg.Hooks.FatalPreprocessHooks...)
	b.OnPreprocess(cfg.Hooks.PreprocessHooks...)
	b.OnSupervisorDecision(cfg.Hooks.SupervisorHooks...)
	b.OnPreAgent(cfg.Hooks.PreAgentHooks...)
	b.OnPostAgent(cfg.Hooks.PostAgentHooks...)
	b.OnPostprocess(cfg.Hooks.PostprocessHooks...)

	return b.Build()
}
