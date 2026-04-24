package orchestrator

import (
	"context"
	"fmt"
	"sync"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// ── Engine Plumbing ──────────────────────────────────────────────────
// Hook and Logger types used exclusively by the Engine.
// They are not domain ports — they are extension points of the execution loop.

// PreprocessHook runs before the main execution loop.
// Errors are logged but do not stop the pipeline.
type PreprocessHook func(ctx context.Context, store StateStore, turn *Turn) error

// FatalPreprocessHook runs before the main loop. Errors stop the pipeline.
type FatalPreprocessHook func(ctx context.Context, store StateStore, turn *Turn) error

// PostprocessHook runs after the main execution loop.
type PostprocessHook func(ctx context.Context, store StateStore, turn *Turn, result *PipelineResult) error

// Logger is an optional structured logger for the engine.
type Logger interface {
	Info(ctx context.Context, msg string, args ...any)
	Error(ctx context.Context, msg string, args ...any)
	Warn(ctx context.Context, msg string, args ...any)
}

// RetryPolicy defines how the engine retries a failed agent.
// Zero value (MaxAttempts=0) means no retry — the agent runs exactly once.
type RetryPolicy struct {
	// MaxAttempts is the total number of attempts (1 = no retry, 2 = one retry, etc.).
	// Zero or negative values are treated as 1.
	MaxAttempts int
	// ShouldRetry is an optional predicate controlling whether an error is retryable.
	// If nil, all errors trigger a retry up to MaxAttempts.
	ShouldRetry func(error) bool
	// OnRetryExhausted is called when all retry attempts fail. It receives the
	// failed phase and last error, and can return a fallback phase to redirect
	// execution instead of failing. Return ("", err) to propagate the original error.
	OnRetryExhausted func(phase Phase, lastErr error) (fallbackPhase Phase, err error)
}

// BudgetConfig defines token and cost limits for a pipeline turn.
// Zero values mean no limit for that dimension.
type BudgetConfig struct {
	// MaxTokens is the maximum total tokens (prompt + completion) allowed per turn.
	MaxTokens int32
	// MaxCostUSD is the maximum estimated cost in USD allowed per turn.
	MaxCostUSD float64
	// MaxTransientErrors is the maximum number of transient errors accumulated
	// across all phases before the pipeline stops. Zero means no limit.
	MaxTransientErrors int
}

// errorLedger tracks accumulated errors by category during a single Run().
// Thread-safe: record and transientCount can be called from concurrent goroutines.
type errorLedger struct {
	mu         sync.Mutex
	counts     map[ErrorCategory]int
	classifier ErrorClassifier
}

func newErrorLedger(classifier ErrorClassifier) *errorLedger {
	return &errorLedger{counts: make(map[ErrorCategory]int), classifier: classifier}
}

func (l *errorLedger) record(err error) {
	cat := l.classifier.Classify(err)
	l.mu.Lock()
	l.counts[cat]++
	l.mu.Unlock()
}

func (l *errorLedger) transientCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.counts[CategoryTransient] + l.counts[CategoryRateLimit]
}

// snapshot returns a copy of the counts map keyed by ErrorCategory string name,
// suitable for persisting in a Checkpoint. Nil counts map returns nil.
func (l *errorLedger) snapshot() map[string]int {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.counts) == 0 {
		return nil
	}
	out := make(map[string]int, len(l.counts))
	for cat, n := range l.counts {
		out[cat.String()] = n
	}
	return out
}

// restore rehydrates ledger counts from a Checkpoint snapshot.
// Unknown category names are silently dropped.
func (l *errorLedger) restore(snap map[string]int) {
	if snap == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	for name, n := range snap {
		switch name {
		case "permanent":
			l.counts[CategoryPermanent] = n
		case "transient":
			l.counts[CategoryTransient] = n
		case "rate_limit":
			l.counts[CategoryRateLimit] = n
		case "unknown":
			l.counts[CategoryUnknown] = n
		}
	}
}

// Engine orchestrates a multi-phase agent pipeline: the supervisor decides the
// next phase, the engine executes it, applies state deltas, and loops until
// the agent signals wait-for-user, the budget is exhausted, or maxSteps is reached.
type Engine struct {
	nodeRegistry         map[string]registeredNode // pure-function agents
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
	instruments          *instruments
	checkpoints          CheckpointStore
}

// registeredNode holds a pure-function agent and its middleware chain.
type registeredNode struct {
	handler         NodeFunc
	middleware      []AgentMiddleware
	concurrencySafe bool // if true, this node can run in parallel with other safe nodes
}

// runConfig is an immutable snapshot of the engine configuration captured at
// the start of Run(). It prevents drift if the Engine is modified concurrently.
type runConfig struct {
	maxSteps         int
	retryPolicy      RetryPolicy
	budget           BudgetConfig
	repeatablePhases map[string]bool
}

// Run executes the full pipeline: preprocess → loop → postprocess.
func (e *Engine) Run(ctx context.Context, store StateStore, turn *Turn) (*PipelineResult, error) {
	ctx, span := e.tracer.Start(ctx, "orchestrator.turn",
		trace.WithAttributes(
			attribute.String("conversation.id", turn.ConversationID),
			attribute.String("org.id", turn.OrgID),
			attribute.String("org.name", turn.OrgName),
			attribute.String("channel", turn.Channel),
			attribute.String("message.type", turn.MessageType),
		),
	)
	defer span.End()

	// 1a. Fatal preprocess hooks — errors stop the pipeline
	for _, hook := range e.fatalPreprocessHooks {
		if err := hook(ctx, store, turn); err != nil {
			e.logError(ctx, "Fatal preprocess hook error", "error", err)
			span.RecordError(err)
			span.SetStatus(codes.Error, "fatal preprocess")
			return nil, fmt.Errorf("fatal preprocess hook: %w", err)
		}
	}

	// 1b. Best-effort preprocess hooks — errors are logged but don't stop the pipeline
	for _, hook := range e.preprocessHooks {
		if err := hook(ctx, store, turn); err != nil {
			e.logError(ctx, "Preprocess hook error", "error", err)
		}
	}

	// 2. Snapshot immutable config for this turn (prevents drift mid-request)
	repeatables := make(map[string]bool, len(e.repeatablePhases))
	for k, v := range e.repeatablePhases {
		repeatables[k] = v
	}
	cfg := runConfig{
		maxSteps:         e.maxSteps,
		retryPolicy:      e.retryPolicy,
		budget:           e.budget,
		repeatablePhases: repeatables,
	}

	// 3. Resume from checkpoint if one exists — otherwise record the user message.
	//    AddMessage is skipped on resume because the pre-crash run already wrote it.
	//    Preprocess hooks are re-run on resume (they must be idempotent) so that
	//    turn.Metadata enrichment (fatal preprocess auth, channel-specific fields)
	//    remains consistent post-crash.
	result := &PipelineResult{}
	ls, resumed := e.loadOrInit(ctx, turn, result, span)
	if !resumed {
		if err := store.AddMessage("user", turn.Text); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, "add user message")
			return nil, fmt.Errorf("failed to add user message: %w", err)
		}
	}

	// 4. Core execution loop
	if err := e.executeLoop(ctx, store, turn, result, cfg, ls); err != nil {
		e.logError(ctx, "Execute loop error", "error", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, "execute loop")
		return result, fmt.Errorf("execute loop: %w", err)
	}

	// 5. Postprocess hooks
	for _, hook := range e.postprocessHooks {
		if err := hook(ctx, store, turn, result); err != nil {
			e.logError(ctx, "Postprocess hook error", "error", err)
		}
	}

	if result.Usage != nil {
		span.SetAttributes(
			attribute.Int64("usage.prompt_tokens", int64(result.Usage.PromptTokens)),
			attribute.Int64("usage.completion_tokens", int64(result.Usage.CompletionTokens)),
			attribute.Int64("usage.total_tokens", int64(result.Usage.TotalTokens)),
		)
	}
	if phase, ok := result.Metadata[MetaFinalPhase].(string); ok {
		span.SetAttributes(attribute.String("final.phase", phase))
	}
	if event, ok := result.Metadata[MetaFinalEvent].(string); ok {
		span.SetAttributes(attribute.String("final.event", event))
	}

	// 6. Normal completion: clear checkpoint so next turn starts fresh.
	//    On context cancel or fatal error we DO NOT clear — lets the next retry
	//    of the same TurnID resume where we left off.
	if e.checkpoints != nil && turn.TurnID != "" {
		if err := e.checkpoints.Clear(ctx, turn.TurnID); err != nil {
			e.logWarn(ctx, "checkpoint clear failed", "turn_id", turn.TurnID, "error", err)
		} else {
			e.instruments.checkpoint.Add(ctx, 1, metric.WithAttributes(attribute.String("event", "clear")))
		}
	}
	return result, nil
}

// loadOrInit returns the loopState for this turn. On resume it rehydrates from
// a checkpoint; otherwise it initializes a fresh state. The resumed bool signals
// to callers that AddMessage should be skipped.
func (e *Engine) loadOrInit(ctx context.Context, turn *Turn, result *PipelineResult, span trace.Span) (*loopState, bool) {
	ls := &loopState{
		ledger:        newErrorLedger(e.classifier),
		sharedContext: make(map[string]any),
	}
	if e.checkpoints == nil || turn.TurnID == "" {
		return ls, false
	}

	ckpt, err := e.checkpoints.Load(ctx, turn.TurnID)
	if err != nil {
		e.logWarn(ctx, "checkpoint load failed", "turn_id", turn.TurnID, "error", err)
		e.instruments.checkpoint.Add(ctx, 1, metric.WithAttributes(attribute.String("event", "error")))
		return ls, false
	}
	if ckpt == nil {
		e.instruments.checkpoint.Add(ctx, 1, metric.WithAttributes(attribute.String("event", "miss")))
		return ls, false
	}

	ls.phase = ckpt.Phase
	ls.lastEvent = ckpt.LastEvent
	ls.steps = ckpt.Step
	ls.ledger.restore(ckpt.ErrorCounts)
	if ckpt.SharedContext != nil {
		ls.sharedContext = ckpt.SharedContext
	}
	if ckpt.Usage != nil {
		u := *ckpt.Usage
		result.Usage = &u
	}

	span.SetAttributes(
		attribute.Bool("resumed", true),
		attribute.Int("resumed.step", ckpt.Step),
		attribute.String("resumed.phase", string(ckpt.Phase)),
	)
	e.instruments.checkpoint.Add(ctx, 1, metric.WithAttributes(attribute.String("event", "resume")))
	e.logInfo(ctx, "resumed from checkpoint", "turn_id", turn.TurnID, "step", ckpt.Step, "phase", ckpt.Phase)
	return ls, true
}

func (e *Engine) accumulateUsage(result *PipelineResult, usage *Usage) {
	if result.Usage == nil {
		result.Usage = &Usage{}
	}
	result.Usage.Add(usage)
}

func (e *Engine) logInfo(ctx context.Context, msg string, args ...any) {
	if e.logger != nil {
		e.logger.Info(ctx, msg, args...)
	}
}

func (e *Engine) logError(ctx context.Context, msg string, args ...any) {
	if e.logger != nil {
		e.logger.Error(ctx, msg, args...)
	}
}

func (e *Engine) logWarn(ctx context.Context, msg string, args ...any) {
	if e.logger != nil {
		e.logger.Warn(ctx, msg, args...)
	}
}

// toolLoopMeter wraps the Engine's instruments for injection into NodeFunc context.
// Returns nil when instruments are uninitialized (should not happen post-Build).
func (e *Engine) toolLoopMeter() *toolLoopMeter {
	if e.instruments == nil {
		return nil
	}
	return &toolLoopMeter{
		llmDuration:  e.instruments.llmDuration,
		toolDuration: e.instruments.toolDuration,
		toolCalls:    e.instruments.toolCalls,
	}
}
