package orchestrator

import (
	"fmt"

	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

// PipelineBuilder provides a fluent API for constructing a pipeline engine.
type PipelineBuilder struct {
	nodeRegistry         map[string]registeredNode
	supervisor           Supervisor
	maxSteps             int
	repeatablePhases     map[string]bool
	retryPolicy          RetryPolicy
	budget               BudgetConfig
	fatalPreprocessHooks []FatalPreprocessHook
	preprocessHooks      []PreprocessHook
	postprocessHooks     []PostprocessHook
	supervisorHooks      []SupervisorHook
	preAgentHooks        []PreAgentHook
	postAgentHooks       []PostAgentHook
	logger               Logger
	classifier           ErrorClassifier
	cost                 CostCalculator
	tracer               trace.Tracer
	meter                metric.Meter
	checkpoints          CheckpointStore
}

// NewPipelineBuilder creates a new PipelineBuilder with sensible defaults (maxSteps=3).
func NewPipelineBuilder() *PipelineBuilder {
	return &PipelineBuilder{
		nodeRegistry:     make(map[string]registeredNode),
		repeatablePhases: make(map[string]bool),
		maxSteps:         3,
	}
}

// MaxSteps sets the maximum number of supervisor iterations per turn.
func (b *PipelineBuilder) MaxSteps(n int) *PipelineBuilder {
	b.maxSteps = n
	return b
}

// WithSupervisor sets the supervisor that decides the next phase.
func (b *PipelineBuilder) WithSupervisor(s Supervisor) *PipelineBuilder {
	b.supervisor = s
	return b
}

// WithLogger sets an optional structured logger.
func (b *PipelineBuilder) WithLogger(l Logger) *PipelineBuilder {
	b.logger = l
	return b
}

// WithClassifier plugs in a concrete ErrorClassifier. Without one, the engine
// falls back to noopClassifier which treats every error as CategoryPermanent.
func (b *PipelineBuilder) WithClassifier(c ErrorClassifier) *PipelineBuilder {
	b.classifier = c
	return b
}

// WithCostCalculator plugs in a concrete CostCalculator for MaxCostUSD budget
// enforcement. Without one, the engine falls back to noopCostCalculator which
// always returns 0 — MaxCostUSD limits never trip.
func (b *PipelineBuilder) WithCostCalculator(c CostCalculator) *PipelineBuilder {
	b.cost = c
	return b
}

// WithTracer plugs in an OpenTelemetry tracer for span emission.
// Without one, a noop tracer is used and no spans are recorded.
func (b *PipelineBuilder) WithTracer(t trace.Tracer) *PipelineBuilder {
	b.tracer = t
	return b
}

// WithMeter plugs in an OpenTelemetry meter for counter and histogram emission.
// Without one, a noop meter is used and no metrics are recorded.
func (b *PipelineBuilder) WithMeter(m metric.Meter) *PipelineBuilder {
	b.meter = m
	return b
}

// WithCheckpoints enables mid-turn durability. See CheckpointStore for the
// idempotency contract callers must honor for tools with side-effects.
func (b *PipelineBuilder) WithCheckpoints(s CheckpointStore) *PipelineBuilder {
	b.checkpoints = s
	return b
}

// WithRetry configures a retry policy applied to all agent executions.
// By default no retries occur (MaxAttempts=0 is treated as 1).
func (b *PipelineBuilder) WithRetry(policy RetryPolicy) *PipelineBuilder {
	b.retryPolicy = policy
	return b
}

// WithBudget sets token and/or cost limits for the pipeline turn.
// The engine stops the loop gracefully when either limit is exceeded.
func (b *PipelineBuilder) WithBudget(budget BudgetConfig) *PipelineBuilder {
	b.budget = budget
	return b
}

// Repeatable marks phases that can re-enter the loop without EventPhaseComplete.
func (b *PipelineBuilder) Repeatable(phases ...string) *PipelineBuilder {
	for _, p := range phases {
		b.repeatablePhases[p] = true
	}
	return b
}

// OnFatalPreprocess adds preprocess hooks that stop the pipeline on error.
func (b *PipelineBuilder) OnFatalPreprocess(hooks ...FatalPreprocessHook) *PipelineBuilder {
	b.fatalPreprocessHooks = append(b.fatalPreprocessHooks, hooks...)
	return b
}

// OnPreprocess adds one or more best-effort preprocess hooks.
func (b *PipelineBuilder) OnPreprocess(hooks ...PreprocessHook) *PipelineBuilder {
	b.preprocessHooks = append(b.preprocessHooks, hooks...)
	return b
}

// OnPostprocess adds one or more postprocess hooks.
func (b *PipelineBuilder) OnPostprocess(hooks ...PostprocessHook) *PipelineBuilder {
	b.postprocessHooks = append(b.postprocessHooks, hooks...)
	return b
}

// RegisterNode maps a phase to a pure-function agent with optional middleware.
// Nodes receive a read-only StateView and return only deltas.
func (b *PipelineBuilder) RegisterNode(phase string, fn NodeFunc, middleware ...AgentMiddleware) *PipelineBuilder {
	b.nodeRegistry[phase] = registeredNode{handler: fn, middleware: middleware}
	return b
}

// RegisterConcurrentNode is like RegisterNode but marks the phase as safe for
// concurrent execution. When the supervisor implements ParallelSupervisor, the
// engine may run multiple concurrent-safe phases in parallel.
func (b *PipelineBuilder) RegisterConcurrentNode(phase string, fn NodeFunc, middleware ...AgentMiddleware) *PipelineBuilder {
	b.nodeRegistry[phase] = registeredNode{handler: fn, middleware: middleware, concurrencySafe: true}
	return b
}

// OnSupervisorDecision adds hooks that run after each supervisor decision.
func (b *PipelineBuilder) OnSupervisorDecision(hooks ...SupervisorHook) *PipelineBuilder {
	b.supervisorHooks = append(b.supervisorHooks, hooks...)
	return b
}

// OnPreAgent adds hooks that run before each pure-function agent.
func (b *PipelineBuilder) OnPreAgent(hooks ...PreAgentHook) *PipelineBuilder {
	b.preAgentHooks = append(b.preAgentHooks, hooks...)
	return b
}

// OnPostAgent adds hooks that run after each pure-function agent.
func (b *PipelineBuilder) OnPostAgent(hooks ...PostAgentHook) *PipelineBuilder {
	b.postAgentHooks = append(b.postAgentHooks, hooks...)
	return b
}

// MustBuild validates the configuration and returns an engine, panicking on error.
func (b *PipelineBuilder) MustBuild() *Engine {
	eng, err := b.Build()
	if err != nil {
		panic(err)
	}
	return eng
}

// Build validates the configuration and returns an engine.
func (b *PipelineBuilder) Build() (*Engine, error) {
	if b.supervisor == nil {
		return nil, fmt.Errorf("orchestrator: supervisor is required")
	}
	if len(b.nodeRegistry) == 0 {
		return nil, fmt.Errorf("orchestrator: at least one agent must be registered")
	}

	hasPhase := func(phase string) bool {
		_, ok := b.nodeRegistry[phase]
		return ok
	}

	// Validate supervisor phases exist in either registry
	if sm, ok := b.supervisor.(*StateMachineSupervisor); ok {
		for _, t := range sm.Transitions {
			if !hasPhase(t.To) {
				return nil, fmt.Errorf("orchestrator: transition target phase %q has no registered agent", t.To)
			}
		}
		for _, ct := range sm.ConditionalTransitions {
			if !hasPhase(ct.To) {
				return nil, fmt.Errorf("orchestrator: conditional transition target phase %q has no registered agent", ct.To)
			}
		}
	}
	if ls, ok := b.supervisor.(*LinearSupervisor); ok {
		for _, p := range ls.phases {
			if !hasPhase(p) {
				return nil, fmt.Errorf("orchestrator: linear phase %q has no registered agent", p)
			}
		}
	}

	classifier := b.classifier
	if classifier == nil {
		classifier = noopClassifier{}
	}
	cost := b.cost
	if cost == nil {
		cost = noopCostCalculator{}
	}
	tracer := b.tracer
	if tracer == nil {
		tracer = tracenoop.NewTracerProvider().Tracer("")
	}
	meter := b.meter
	if meter == nil {
		meter = noop.NewMeterProvider().Meter("")
	}

	instruments, err := newInstruments(meter)
	if err != nil {
		return nil, fmt.Errorf("orchestrator: failed to init metrics: %w", err)
	}

	return &Engine{
		nodeRegistry:         b.nodeRegistry,
		supervisor:           b.supervisor,
		maxSteps:             b.maxSteps,
		repeatablePhases:     b.repeatablePhases,
		retryPolicy:          b.retryPolicy,
		budget:               b.budget,
		fatalPreprocessHooks: b.fatalPreprocessHooks,
		preprocessHooks:      b.preprocessHooks,
		postprocessHooks:     b.postprocessHooks,
		supervisorHooks:      b.supervisorHooks,
		preAgentHooks:        b.preAgentHooks,
		postAgentHooks:       b.postAgentHooks,
		logger:               b.logger,
		classifier:           classifier,
		cost:                 cost,
		tracer:               tracer,
		meter:                meter,
		instruments:          instruments,
		checkpoints:          b.checkpoints,
	}, nil
}
