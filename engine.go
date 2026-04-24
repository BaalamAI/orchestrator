package orchestrator

import (
	"context"
	"fmt"
	"sync"
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
	// 1a. Fatal preprocess hooks — errors stop the pipeline
	for _, hook := range e.fatalPreprocessHooks {
		if err := hook(ctx, store, turn); err != nil {
			e.logError(ctx, "Fatal preprocess hook error", "error", err)
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

	// 3. Add user message
	if err := store.AddMessage("user", turn.Text); err != nil {
		return nil, fmt.Errorf("failed to add user message: %w", err)
	}

	// 4. Core execution loop
	result := &PipelineResult{}
	if err := e.executeLoop(ctx, store, turn, result, cfg); err != nil {
		e.logError(ctx, "Execute loop error", "error", err)
		return result, fmt.Errorf("execute loop: %w", err)
	}

	// 5. Postprocess hooks
	for _, hook := range e.postprocessHooks {
		if err := hook(ctx, store, turn, result); err != nil {
			e.logError(ctx, "Postprocess hook error", "error", err)
		}
	}

	return result, nil
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
