// Package obs carries the orchestrator's OpenTelemetry wiring in a neutral
// place that both the root package and the llm/ subpackage can import without
// creating a cycle (root → llm → root).
//
// The Engine constructs an [Observability] bag from its own instruments and
// injects it into the context via [WithObservability]. The llm.NewToolLoopNode
// adapter reads it back via [FromCtx] when it emits spans and metrics.
package obs

import (
	"context"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// Observability bundles the OTel instruments used by the tool loop.
type Observability struct {
	Tracer       trace.Tracer
	LLMDuration  metric.Float64Histogram
	ToolDuration metric.Float64Histogram
	ToolCalls    metric.Int64Counter
}

type ctxKey int

const ctxKeyObs ctxKey = 0

// WithObservability attaches obs to ctx. Pass nil to disable.
func WithObservability(ctx context.Context, o *Observability) context.Context {
	return context.WithValue(ctx, ctxKeyObs, o)
}

// FromCtx returns the Observability attached to ctx, or nil if none.
func FromCtx(ctx context.Context) *Observability {
	if o, ok := ctx.Value(ctxKeyObs).(*Observability); ok {
		return o
	}
	return nil
}

// TracerFromCtx returns the tracer in ctx, or a noop tracer if none.
func TracerFromCtx(ctx context.Context) trace.Tracer {
	if o := FromCtx(ctx); o != nil && o.Tracer != nil {
		return o.Tracer
	}
	return trace.NewNoopTracerProvider().Tracer("")
}

// RecordLLMDuration is a no-op when the histogram is nil.
func (o *Observability) RecordLLMDuration(ctx context.Context, ms float64, model string) {
	if o == nil || o.LLMDuration == nil {
		return
	}
	o.LLMDuration.Record(ctx, ms, metric.WithAttributes(attribute.String("model", model)))
}

// RecordToolDuration is a no-op when the histogram is nil.
func (o *Observability) RecordToolDuration(ctx context.Context, ms float64, tool string) {
	if o == nil || o.ToolDuration == nil {
		return
	}
	o.ToolDuration.Record(ctx, ms, metric.WithAttributes(attribute.String("tool", tool)))
}

// RecordToolCall is a no-op when the counter is nil.
func (o *Observability) RecordToolCall(ctx context.Context, tool, outcome string) {
	if o == nil || o.ToolCalls == nil {
		return
	}
	o.ToolCalls.Add(ctx, 1, metric.WithAttributes(
		attribute.String("tool", tool),
		attribute.String("outcome", outcome),
	))
}
