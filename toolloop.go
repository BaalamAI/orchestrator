package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

// ToolErrorPolicy controls how tool execution errors propagate in the ReAct loop.
type ToolErrorPolicy int

const (
	// ToolErrorReportToLLM (default) converts the error to a tool result
	// with IsError=true and feeds it back to the LLM as the tool response.
	// The LLM can then decide to retry, try a different tool, or give up.
	ToolErrorReportToLLM ToolErrorPolicy = iota
	// ToolErrorPropagate surfaces the error to the engine, which applies
	// the configured RetryPolicy / classifier. Use for truly fatal tool errors.
	ToolErrorPropagate
)

// ToolLoopOptions configures a NewToolLoopNode.
type ToolLoopOptions struct {
	// Model is the provider-specific model identifier passed to LLMClient.
	Model string
	// SystemPrompt returns the system prompt for this phase given the current state.
	// Called once per NodeFunc invocation (at the start of the loop).
	SystemPrompt func(view StateView, turn *Turn) string
	// Tools is the list of tools exposed to the LLM in every request.
	Tools []Tool
	// MaxIterations caps the ReAct inner loop. Default: 8. Aborts with an error
	// if the LLM keeps requesting tools past this limit.
	MaxIterations int
	// Temperature for LLM calls. Default: 0.
	Temperature float64
	// MaxTokens caps each LLM completion. Default: 0 (provider default).
	MaxTokens int32
	// EventOnComplete is the NodeResult.Event when the loop ends with end_turn.
	// Default: EventWaitUser.
	EventOnComplete EventType
	// OnToolError controls tool-error propagation. Default: ToolErrorReportToLLM.
	OnToolError ToolErrorPolicy
	// InitialMessages is an optional hook to build the starting message list.
	// Default: converts view.Messages() to ChatMessages.
	InitialMessages func(view StateView, turn *Turn) []ChatMessage
	// OnLLMCall is invoked after each LLM completion (even on error).
	// Useful for wiring AgentTree / custom observability.
	OnLLMCall func(ctx context.Context, req CompletionRequest, resp *CompletionResponse, dur time.Duration, err error)
	// OnToolCall is invoked after each tool invocation (even on error).
	// Useful for wiring AgentTree / custom observability.
	OnToolCall func(ctx context.Context, call ToolCall, result *ToolResult, dur time.Duration, err error)
}

// ErrToolLoopMaxIterations is returned when the LLM keeps requesting tools past
// ToolLoopOptions.MaxIterations. Classifiers may map this to CategoryPermanent.
var ErrToolLoopMaxIterations = errors.New("tool loop: max iterations exceeded")

// NewToolLoopNode returns a NodeFunc that implements a ReAct-style loop:
//
//  1. Call the LLM with the current messages and registered tools.
//  2. If StopReason is "tool_use", invoke each requested tool, append the tool
//     results as new messages, and loop back to step 1.
//  3. If StopReason is "end_turn" (or any terminal reason), return a NodeResult
//     with the assistant's content and Event = EventOnComplete.
//
// Each iteration accumulates token usage in the returned NodeResult. State deltas
// returned by tools are merged into NodeResult.Delta and applied by the engine
// after the node completes.
func NewToolLoopNode(client LLMClient, opts ToolLoopOptions) NodeFunc {
	if client == nil {
		panic("orchestrator: NewToolLoopNode requires a non-nil LLMClient")
	}
	max := opts.MaxIterations
	if max <= 0 {
		max = 8
	}
	completionEvent := opts.EventOnComplete
	if completionEvent == "" {
		completionEvent = EventWaitUser
	}

	toolMap := make(map[string]Tool, len(opts.Tools))
	toolDefs := make([]ToolDefinition, 0, len(opts.Tools))
	for _, t := range opts.Tools {
		def := t.Definition()
		toolMap[def.Name] = t
		toolDefs = append(toolDefs, def)
	}

	sysPrompt := opts.SystemPrompt
	if sysPrompt == nil {
		sysPrompt = func(StateView, *Turn) string { return "" }
	}
	buildInitial := opts.InitialMessages
	if buildInitial == nil {
		buildInitial = defaultInitialMessages
	}

	return func(ctx context.Context, view StateView, turn *Turn, input *NodeInput) (*NodeResult, error) {
		tracer := tracerFromCtx(ctx)
		meter := metricsFromCtx(ctx)

		result := &NodeResult{}
		msgs := buildInitial(view, turn)
		if input != nil && input.RAGContext != "" {
			msgs = append(msgs, ChatMessage{Role: "user", Content: "### Contexto:\n" + input.RAGContext})
		}

		for iter := 0; iter < max; iter++ {
			if err := ctx.Err(); err != nil {
				return nil, err
			}

			req := CompletionRequest{
				Model:       opts.Model,
				System:      sysPrompt(view, turn),
				Messages:    msgs,
				Tools:       toolDefs,
				Temperature: opts.Temperature,
				MaxTokens:   opts.MaxTokens,
			}

			resp, err := callLLM(ctx, tracer, meter, client, req, opts.OnLLMCall, iter, opts.Model)
			if err != nil {
				return nil, fmt.Errorf("tool loop: llm call: %w", err)
			}
			accumulateToolLoopUsage(result, resp.Usage)

			if resp.StopReason != StopReasonToolUse || len(resp.ToolCalls) == 0 {
				result.Answer = resp.Content
				result.Event = completionEvent
				return result, nil
			}

			// Append the assistant message with its tool calls to the history.
			msgs = append(msgs, ChatMessage{
				Role:      "assistant",
				Content:   resp.Content,
				ToolCalls: resp.ToolCalls,
			})

			for _, call := range resp.ToolCalls {
				toolMsg, toolErr := invokeToolCall(ctx, tracer, meter, toolMap, call, view, turn, result, opts.OnToolCall)
				if toolErr != nil && opts.OnToolError == ToolErrorPropagate {
					return nil, toolErr
				}
				msgs = append(msgs, toolMsg)
			}
		}

		return nil, fmt.Errorf("%w (phase=tool_loop, model=%s)", ErrToolLoopMaxIterations, opts.Model)
	}
}

// callLLM runs a single LLM completion with span + metrics + hook.
func callLLM(
	ctx context.Context,
	tracer trace.Tracer,
	meter *toolLoopMeter,
	client LLMClient,
	req CompletionRequest,
	hook func(context.Context, CompletionRequest, *CompletionResponse, time.Duration, error),
	iter int,
	model string,
) (*CompletionResponse, error) {
	ctx, span := tracer.Start(ctx, "orchestrator.llm",
		trace.WithAttributes(
			attribute.String("model", model),
			attribute.Int("iteration", iter),
			attribute.Int("messages", len(req.Messages)),
			attribute.Int("tools", len(req.Tools)),
		),
	)
	defer span.End()

	start := time.Now()
	resp, err := client.Complete(ctx, req)
	dur := time.Since(start)

	if meter != nil {
		meter.recordLLMDuration(ctx, dur.Seconds()*1000, model)
	}
	if hook != nil {
		hook(ctx, req, resp, dur, err)
	}
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return nil, err
	}
	span.SetAttributes(
		attribute.String("stop_reason", resp.StopReason),
		attribute.Int("tool_calls", len(resp.ToolCalls)),
	)
	if resp.Usage != nil {
		span.SetAttributes(
			attribute.Int64("usage.prompt_tokens", int64(resp.Usage.PromptTokens)),
			attribute.Int64("usage.completion_tokens", int64(resp.Usage.CompletionTokens)),
		)
	}
	return resp, nil
}

// invokeToolCall runs one tool call and returns the message to append to history.
// The tool's delta is merged into result.Delta. The bool indicates a fatal error
// when ToolErrorPropagate is set by caller.
func invokeToolCall(
	ctx context.Context,
	tracer trace.Tracer,
	meter *toolLoopMeter,
	toolMap map[string]Tool,
	call ToolCall,
	view StateView,
	turn *Turn,
	result *NodeResult,
	hook func(context.Context, ToolCall, *ToolResult, time.Duration, error),
) (ChatMessage, error) {
	ctx, span := tracer.Start(ctx, "orchestrator.tool",
		trace.WithAttributes(
			attribute.String("tool.name", call.Name),
			attribute.String("tool.call_id", call.ID),
		),
	)
	defer span.End()

	tool, ok := toolMap[call.Name]
	if !ok {
		msg := fmt.Sprintf("unknown tool: %s", call.Name)
		span.SetStatus(codes.Error, msg)
		if meter != nil {
			meter.recordToolCall(ctx, call.Name, "unknown")
		}
		if hook != nil {
			hook(ctx, call, &ToolResult{Content: msg, IsError: true}, 0, nil)
		}
		return ChatMessage{Role: "tool", ToolCallID: call.ID, Content: msg}, nil
	}
	span.SetAttributes(attribute.Bool("tool.idempotent", tool.Definition().Idempotent))

	start := time.Now()
	toolRes, err := tool.Invoke(ctx, call, view, turn)
	dur := time.Since(start)

	if meter != nil {
		meter.recordToolDuration(ctx, dur.Seconds()*1000, call.Name)
	}

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		if meter != nil {
			meter.recordToolCall(ctx, call.Name, "error")
		}
		if hook != nil {
			hook(ctx, call, nil, dur, err)
		}
		return ChatMessage{
			Role:       "tool",
			ToolCallID: call.ID,
			Content:    fmt.Sprintf("tool %s failed: %v", call.Name, err),
		}, err
	}

	outcome := "ok"
	if toolRes.IsError {
		outcome = "error"
	}
	if meter != nil {
		meter.recordToolCall(ctx, call.Name, outcome)
	}
	if hook != nil {
		hook(ctx, call, &toolRes, dur, nil)
	}

	if toolRes.Delta != nil {
		result.Delta.Merge(toolRes.Delta)
	}
	accumulateToolLoopUsage(result, toolRes.Usage)

	return ChatMessage{
		Role:       "tool",
		ToolCallID: call.ID,
		Content:    toolRes.Content,
	}, nil
}

// defaultInitialMessages converts the view's conversation history to ChatMessages.
func defaultInitialMessages(view StateView, turn *Turn) []ChatMessage {
	msgs := view.Messages()
	out := make([]ChatMessage, 0, len(msgs)+1)
	for _, m := range msgs {
		role := m.Role
		if role == "model" {
			role = "assistant"
		}
		out = append(out, ChatMessage{Role: role, Content: m.Text})
	}
	if turn != nil && turn.Text != "" {
		out = append(out, ChatMessage{Role: "user", Content: turn.Text})
	}
	return out
}

func accumulateToolLoopUsage(result *NodeResult, src *Usage) {
	if src == nil {
		return
	}
	if result.Usage == nil {
		result.Usage = &Usage{}
	}
	result.Usage.Add(src)
}

// ── OTel helpers scoped to the ToolLoopNode ──────────────────────────
// These exist because NodeFunc doesn't have access to the Engine's tracer
// or instruments. The engine propagates them via context; when unset, noop is used.

type toolLoopMeter struct {
	llmDuration  metric.Float64Histogram
	toolDuration metric.Float64Histogram
	toolCalls    metric.Int64Counter
}

func (m *toolLoopMeter) recordLLMDuration(ctx context.Context, ms float64, model string) {
	m.llmDuration.Record(ctx, ms, metric.WithAttributes(attribute.String("model", model)))
}

func (m *toolLoopMeter) recordToolDuration(ctx context.Context, ms float64, tool string) {
	m.toolDuration.Record(ctx, ms, metric.WithAttributes(attribute.String("tool", tool)))
}

func (m *toolLoopMeter) recordToolCall(ctx context.Context, tool, outcome string) {
	m.toolCalls.Add(ctx, 1, metric.WithAttributes(
		attribute.String("tool", tool),
		attribute.String("outcome", outcome),
	))
}

type ctxKey int

const (
	ctxKeyTracer ctxKey = iota
	ctxKeyMeter
)

// withToolLoopObservability is used by the engine to inject its tracer/meter
// so that NewToolLoopNode closures can emit spans/metrics without needing a
// direct reference to the Engine.
func withToolLoopObservability(ctx context.Context, tracer trace.Tracer, meter *toolLoopMeter) context.Context {
	ctx = context.WithValue(ctx, ctxKeyTracer, tracer)
	ctx = context.WithValue(ctx, ctxKeyMeter, meter)
	return ctx
}

func tracerFromCtx(ctx context.Context) trace.Tracer {
	if t, ok := ctx.Value(ctxKeyTracer).(trace.Tracer); ok && t != nil {
		return t
	}
	return trace.NewNoopTracerProvider().Tracer("")
}

func metricsFromCtx(ctx context.Context) *toolLoopMeter {
	if m, ok := ctx.Value(ctxKeyMeter).(*toolLoopMeter); ok {
		return m
	}
	return nil
}
