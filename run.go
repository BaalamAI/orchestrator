package orchestrator

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

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

	// 1b. Best-effort preprocess hooks
	for _, hook := range e.preprocessHooks {
		if err := hook(ctx, store, turn); err != nil {
			e.logError(ctx, "Preprocess hook error", "error", err)
		}
	}

	// 2. Snapshot immutable config for this turn
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

	// 6. Clear checkpoint on normal completion.
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
