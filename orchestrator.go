package orchestrator

import (
	"context"
	"sync"

	"github.com/baalamai/orchestrator/obs"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// Engine orchestrates a multi-phase agent pipeline: the supervisor decides the
// next phase, the engine executes it, applies state deltas, and loops until
// the agent signals wait-for-user, the budget is exhausted, or maxSteps is reached.
//
// Use PipelineBuilder to construct an Engine — the zero value is not usable.
type Engine struct {
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
	instruments          *instruments
	checkpoints          CheckpointStore
}

// RetryPolicy defines how the engine retries a failed agent.
// Zero value (MaxAttempts=0) means no retry — the agent runs exactly once.
type RetryPolicy struct {
	// MaxAttempts is the total number of attempts (1 = no retry, 2 = one retry, etc.).
	MaxAttempts int
	// ShouldRetry is an optional predicate controlling whether an error is retryable.
	ShouldRetry func(error) bool
	// OnRetryExhausted is called when all retry attempts fail. It can redirect
	// execution to a fallback phase instead of propagating the error.
	OnRetryExhausted func(phase Phase, lastErr error) (fallbackPhase Phase, err error)
}

// registeredNode holds a pure-function agent and its middleware chain.
type registeredNode struct {
	handler         NodeFunc
	middleware      []AgentMiddleware
	concurrencySafe bool
}

// runConfig is an immutable snapshot of the engine configuration captured at
// the start of Run(). It prevents drift if the Engine is modified concurrently.
type runConfig struct {
	maxSteps         int
	retryPolicy      RetryPolicy
	budget           BudgetConfig
	repeatablePhases map[string]bool
}

// ── errorLedger ──────────────────────────────────────────────────────
// Tracks accumulated errors by category during a single Run(). Thread-safe.

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

// ── Logger helpers ──────────────────────────────────────────────────

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

// toolLoopObservability builds the obs.Observability bag from the engine's
// instruments. The tool loop reads it via obs.FromCtx (injected in computeNode)
// so it can emit spans/metrics without a direct Engine reference.
func (e *Engine) toolLoopObservability() *obs.Observability {
	if e.instruments == nil {
		return nil
	}
	return &obs.Observability{
		Tracer:       e.tracer,
		LLMDuration:  e.instruments.llmDuration,
		ToolDuration: e.instruments.toolDuration,
		ToolCalls:    e.instruments.toolCalls,
	}
}

// accumulateUsage adds a delta Usage into the pipeline result, allocating on first call.
func (e *Engine) accumulateUsage(result *PipelineResult, usage *Usage) {
	if result.Usage == nil {
		result.Usage = &Usage{}
	}
	result.Usage.Add(usage)
}
