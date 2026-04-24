package orchestrator

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// loopState holds mutable state that evolves across iterations of executeLoop.
type loopState struct {
	phase     Phase
	lastEvent EventType
	steps     int
	ledger    *errorLedger
	sharedContext map[string]any
}

// executeLoop runs the supervisor → agent cycle up to maxSteps times.
// The loop terminates when: (1) EventWaitUser — agent needs user input,
// (2) budget exceeded — token or cost limit reached, (3) error threshold —
// too many transient errors, or (4) maxSteps reached.
//
// The ls parameter is the mutable loop state; callers pre-fill it from a
// checkpoint for resume, or pass a fresh state for a new turn.
func (e *Engine) executeLoop(ctx context.Context, store StateStore, turn *Turn, result *PipelineResult, cfg runConfig, ls *loopState) error {
	for step := ls.steps; step < cfg.maxSteps; step++ {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		phases, _, err := e.decidePhases(ctx, store, turn, result, ls, step)
		if err != nil {
			return err
		}

		// Anti-stutter: prevent re-entering the same phase unless it's repeatable
		if phases[0] == ls.phase && ls.lastEvent != EventPhaseComplete {
			if !cfg.repeatablePhases[phases[0]] {
				break
			}
		}
		ls.phase = phases[0]

		nodeRes, err := e.runPhases(ctx, store, turn, phases, cfg, ls)
		if err != nil {
			return fmt.Errorf("error in %s agent: %w", ls.phase, err)
		}

		e.propagateNodeOutput(ctx, result, turn, nodeRes, ls)
		ls.steps = step + 1

		e.saveCheckpoint(ctx, store, turn, result, ls)

		if event, stop := e.stopEvent(ctx, result, nodeRes, cfg, ls); stop {
			ls.lastEvent = event
			break
		}
	}

	e.writeDebugMeta(result, ls)
	return nil
}

// saveCheckpoint persists loop state after each step if a CheckpointStore is configured.
// Non-fatal — failure to checkpoint logs a warning and the pipeline continues.
func (e *Engine) saveCheckpoint(ctx context.Context, store StateStore, turn *Turn, result *PipelineResult, ls *loopState) {
	if e.checkpoints == nil || turn.TurnID == "" {
		return
	}
	ckpt := &Checkpoint{
		TurnID:        turn.TurnID,
		Step:          ls.steps,
		Phase:         ls.phase,
		LastEvent:     ls.lastEvent,
		State:         store.State(),
		Messages:      store.Messages(),
		Usage:         result.Usage,
		ErrorCounts:   ls.ledger.snapshot(),
		SharedContext: ls.sharedContext,
		CreatedAt:     time.Now(),
	}
	if err := e.checkpoints.Save(ctx, ckpt); err != nil {
		e.logWarn(ctx, "checkpoint save failed", "turn_id", turn.TurnID, "step", ls.steps, "error", err)
		e.instruments.checkpoint.Add(ctx, 1, metric.WithAttributes(attribute.String("event", "error")))
		return
	}
	e.instruments.checkpoint.Add(ctx, 1, metric.WithAttributes(attribute.String("event", "save")))
}

// decidePhases asks the supervisor for the next phase(s) and fires hooks.
func (e *Engine) decidePhases(ctx context.Context, store StateStore, turn *Turn, result *PipelineResult, ls *loopState, step int) ([]Phase, string, error) {
	ctx, span := e.tracer.Start(ctx, "orchestrator.supervisor",
		trace.WithAttributes(
			attribute.Int("step", step),
			attribute.String("current.phase", string(ls.phase)),
			attribute.String("last.event", string(ls.lastEvent)),
		),
	)
	defer span.End()

	var phases []Phase
	var reason string
	var err error

	if ps, ok := e.supervisor.(ParallelSupervisor); ok {
		phases, reason, err = ps.DecideNextSteps(ctx, store, turn.Text, ls.phase, ls.lastEvent)
	} else {
		var next Phase
		next, reason, err = e.supervisor.DecideNextStep(ctx, store, turn.Text, ls.phase, ls.lastEvent)
		phases = []Phase{next}
	}
	if err != nil {
		e.logError(ctx, "Supervisor error", "error", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, "supervisor error")
		return nil, "", fmt.Errorf("supervisor error: %w", err)
	}

	e.logInfo(ctx, "Supervisor decision", "phases", phases, "reason", reason)

	strPhases := make([]string, len(phases))
	for i, p := range phases {
		strPhases[i] = string(p)
	}
	span.SetAttributes(
		attribute.StringSlice("decided.phases", strPhases),
		attribute.String("reason", reason),
	)

	usage := e.supervisor.Usage()
	if usage != nil {
		e.accumulateUsage(result, usage)
		e.recordUsageMetrics(ctx, usage, "_supervisor")
		span.SetAttributes(attribute.Int64("usage.total_tokens", int64(usage.TotalTokens)))
	}

	// Fire hooks (errors are logged, not fatal)
	for _, hook := range e.supervisorHooks {
		if err := hook(ctx, store, turn, step, phases[0], reason, usage); err != nil {
			e.logError(ctx, "Supervisor hook error", "error", err)
		}
	}

	return phases, reason, nil
}

// runPhases dispatches to parallel or single-phase execution.
func (e *Engine) runPhases(ctx context.Context, store StateStore, turn *Turn, phases []Phase, cfg runConfig, ls *loopState) (*NodeResult, error) {
	parallel := len(phases) > 1
	ctx, span := e.tracer.Start(ctx, "orchestrator.node",
		trace.WithAttributes(
			attribute.String("phase", string(ls.phase)),
			attribute.Bool("parallel", parallel),
		),
	)
	defer span.End()

	start := time.Now()
	var nodeRes *NodeResult
	var err error

	if parallel {
		nodeRes, err = e.executeParallelPhases(ctx, store, turn, phases, cfg.retryPolicy, ls.ledger, ls.sharedContext)
	} else {
		nodeRes, err = e.retryAndCommit(ctx, store, turn, ls.phase, cfg.retryPolicy, ls.ledger, ls.sharedContext)
	}

	durMs := float64(time.Since(start).Milliseconds())
	outcome := "ok"
	if err != nil {
		outcome = "error"
	} else if nodeRes != nil {
		switch nodeRes.Event {
		case EventBudgetExceeded:
			outcome = "budget"
		case EventErrorThreshold:
			outcome = "error_threshold"
		}
	}
	e.instruments.phaseDuration.Record(ctx, durMs, metric.WithAttributes(
		attribute.String("phase", string(ls.phase)),
		attribute.String("outcome", outcome),
	))

	if err != nil {
		e.logError(ctx, "Agent error", "phase", ls.phase, "error", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, "agent error")
		return nil, err
	}
	if nodeRes != nil {
		span.SetAttributes(attribute.String("event", string(nodeRes.Event)))
		if nodeRes.Usage != nil {
			span.SetAttributes(attribute.Int64("usage.total_tokens", int64(nodeRes.Usage.TotalTokens)))
		}
	}

	return nodeRes, nil
}

// propagateNodeOutput distributes agent output (answer, usage, metadata, hints)
// to the pipeline result and loop state.
func (e *Engine) propagateNodeOutput(ctx context.Context, result *PipelineResult, turn *Turn, nodeRes *NodeResult, ls *loopState) {
	for k, v := range nodeRes.SharedContext {
		ls.sharedContext[k] = v
	}

	if nodeRes.Answer != "" {
		result.Answer = nodeRes.Answer
		if turn.Metadata == nil {
			turn.Metadata = make(map[string]any)
		}
		turn.Metadata[MetaPriorAnswer] = nodeRes.Answer
	}

	if nodeRes.Usage != nil {
		e.accumulateUsage(result, nodeRes.Usage)
		e.recordUsageMetrics(ctx, nodeRes.Usage, string(ls.phase))
	}

	for k, v := range nodeRes.Metadata {
		result.SetMeta(k, v)
	}

	ls.lastEvent = nodeRes.Event
}

// stopEvent evaluates error threshold, wait-for-user, and budget limits.
// Returns the termination event and true if the loop should break.
func (e *Engine) stopEvent(ctx context.Context, result *PipelineResult, nodeRes *NodeResult, cfg runConfig, ls *loopState) (EventType, bool) {
	// Error threshold — evaluated first so it takes precedence even when the
	// agent eventually succeeded after retries.
	if cfg.budget.MaxTransientErrors > 0 && ls.ledger.transientCount() >= cfg.budget.MaxTransientErrors {
		e.logWarn(ctx, "Error threshold exceeded, stopping pipeline",
			"transient_errors", ls.ledger.transientCount(),
			"max_transient_errors", cfg.budget.MaxTransientErrors,
		)
		return EventErrorThreshold, true
	}

	if nodeRes.Event == EventWaitUser {
		return nodeRes.Event, true
	}

	if e.budgetExceeded(result, cfg.budget) {
		e.logWarn(ctx, "Budget exceeded, stopping pipeline",
			"total_tokens", result.Usage.TotalTokens,
			"max_tokens", cfg.budget.MaxTokens,
			"max_cost_usd", cfg.budget.MaxCostUSD,
		)
		return EventBudgetExceeded, true
	}

	return "", false
}

// writeDebugMeta writes final metadata used by postprocess hooks (agent tree).
func (e *Engine) writeDebugMeta(result *PipelineResult, ls *loopState) {
	result.SetMeta(MetaFinalPhase, string(ls.phase))
	result.SetMeta(MetaFinalEvent, string(ls.lastEvent))
	result.SetMeta(MetaTotalSteps, ls.steps)
	result.SetMeta(MetaTotalCostUSD, e.estimateCost(result))

	if ls.lastEvent == EventBudgetExceeded {
		result.SetMeta(MetaBudgetExceeded, true)
	}
	if ls.lastEvent == EventErrorThreshold {
		result.SetMeta(MetaErrorThreshold, true)
	}

	if len(ls.ledger.counts) > 0 {
		counts := make(map[string]int, len(ls.ledger.counts))
		for cat, n := range ls.ledger.counts {
			counts[cat.String()] = n
		}
		result.SetMeta(MetaErrorLedger, counts)
	}
}
