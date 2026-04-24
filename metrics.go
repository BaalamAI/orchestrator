package orchestrator

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// instruments bundles all OTel metric instruments used by the engine.
// All instruments are no-op when Meter is the default noop meter.
type instruments struct {
	tokens         metric.Int64Counter
	costUSD        metric.Float64Counter
	phaseDuration  metric.Float64Histogram
	llmDuration    metric.Float64Histogram
	toolDuration   metric.Float64Histogram
	toolCalls      metric.Int64Counter
	retries        metric.Int64Counter
	checkpoint     metric.Int64Counter
}

func newInstruments(m metric.Meter) (*instruments, error) {
	tokens, err := m.Int64Counter(
		"orchestrator.tokens",
		metric.WithDescription("Tokens consumed per phase, split by model/provider/kind"),
		metric.WithUnit("{token}"),
	)
	if err != nil {
		return nil, fmt.Errorf("tokens counter: %w", err)
	}
	costUSD, err := m.Float64Counter(
		"orchestrator.cost_usd",
		metric.WithDescription("Accumulated cost in USD per phase"),
		metric.WithUnit("USD"),
	)
	if err != nil {
		return nil, fmt.Errorf("cost counter: %w", err)
	}
	phaseDur, err := m.Float64Histogram(
		"orchestrator.phase.duration_ms",
		metric.WithDescription("Phase execution duration"),
		metric.WithUnit("ms"),
	)
	if err != nil {
		return nil, fmt.Errorf("phase duration histogram: %w", err)
	}
	llmDur, err := m.Float64Histogram(
		"orchestrator.llm.duration_ms",
		metric.WithDescription("LLM completion call duration"),
		metric.WithUnit("ms"),
	)
	if err != nil {
		return nil, fmt.Errorf("llm duration histogram: %w", err)
	}
	toolDur, err := m.Float64Histogram(
		"orchestrator.tool.duration_ms",
		metric.WithDescription("Tool invocation duration"),
		metric.WithUnit("ms"),
	)
	if err != nil {
		return nil, fmt.Errorf("tool duration histogram: %w", err)
	}
	toolCalls, err := m.Int64Counter(
		"orchestrator.tool.calls",
		metric.WithDescription("Tool invocations by outcome"),
	)
	if err != nil {
		return nil, fmt.Errorf("tool calls counter: %w", err)
	}
	retries, err := m.Int64Counter(
		"orchestrator.retries",
		metric.WithDescription("Retry attempts by phase and error category"),
	)
	if err != nil {
		return nil, fmt.Errorf("retries counter: %w", err)
	}
	checkpoint, err := m.Int64Counter(
		"orchestrator.checkpoint",
		metric.WithDescription("Checkpoint events: save, load, resume, clear, miss, error"),
	)
	if err != nil {
		return nil, fmt.Errorf("checkpoint counter: %w", err)
	}
	return &instruments{
		tokens:        tokens,
		costUSD:       costUSD,
		phaseDuration: phaseDur,
		llmDuration:   llmDur,
		toolDuration:  toolDur,
		toolCalls:     toolCalls,
		retries:       retries,
		checkpoint:    checkpoint,
	}, nil
}

// recordUsageMetrics emits per-model breakdowns from a Usage into the tokens counter.
// The phase attribute tags which pipeline phase consumed the tokens.
func (e *Engine) recordUsageMetrics(ctx context.Context, usage *Usage, phase string) {
	if usage == nil || e.instruments == nil {
		return
	}
	for _, mu := range usage.Breakdown {
		attrs := []attribute.KeyValue{
			attribute.String("phase", phase),
			attribute.String("model", mu.Model),
			attribute.String("provider", mu.Provider),
		}
		e.instruments.tokens.Add(ctx, int64(mu.PromptTokens),
			metric.WithAttributes(append(attrs, attribute.String("kind", "prompt"))...))
		e.instruments.tokens.Add(ctx, int64(mu.CompletionTokens),
			metric.WithAttributes(append(attrs, attribute.String("kind", "completion"))...))

		if cost := e.cost.Calculate(mu.Model, int(mu.PromptTokens), int(mu.CompletionTokens)); cost > 0 {
			e.instruments.costUSD.Add(ctx, cost, metric.WithAttributes(
				attribute.String("phase", phase),
				attribute.String("model", mu.Model),
			))
		}
	}
}
