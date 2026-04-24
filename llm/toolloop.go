package llm

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/baalamai/orchestrator"
	"github.com/baalamai/orchestrator/obs"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

// ToolErrorPolicy controls how tool execution errors propagate in the ReAct loop.
type ToolErrorPolicy int

const (
	// ToolErrorReport (default) converts the error to a tool result with IsError=true
	// and feeds it back to the LLM as the tool response. The LLM can then decide to
	// retry, try a different tool, or give up.
	ToolErrorReport ToolErrorPolicy = iota
	// ToolErrorPropagate surfaces the error to the engine, which applies the
	// configured RetryPolicy / classifier. Use for truly fatal tool errors.
	ToolErrorPropagate
)

// ToolLoopOptions configures a NewToolLoopNode.
type ToolLoopOptions struct {
	// Model is the provider-specific model identifier passed to Client.
	Model string
	// SystemPrompt returns the system prompt for this phase given the current state.
	// Called once per NodeFunc invocation (at the start of the loop).
	SystemPrompt func(view orchestrator.StateView, turn *orchestrator.Turn) string
	// Tools is the list of tools exposed to the LLM in every request.
	Tools []Tool
	// MaxIterations caps the ReAct inner loop. Default: 8.
	MaxIterations int
	// Temperature for LLM calls. Default: 0.
	Temperature float64
	// MaxTokens caps each LLM completion. Default: 0 (provider default).
	MaxTokens int32
	// EventOnComplete is the NodeResult.Event when the loop ends with end_turn.
	// Default: EventWaitUser.
	EventOnComplete orchestrator.EventType
	// OnToolError controls tool-error propagation. Default: ToolErrorReport.
	OnToolError ToolErrorPolicy
	// InitialMessages is an optional hook to build the starting message list.
	// Default: converts view.Messages() to ChatMessages.
	InitialMessages func(view orchestrator.StateView, turn *orchestrator.Turn) []ChatMessage
	// OnLLMCall is invoked after each LLM completion (even on error).
	OnLLMCall func(ctx context.Context, req CompletionRequest, resp *CompletionResponse, dur time.Duration, err error)
	// OnToolCall is invoked after each tool invocation (even on error).
	OnToolCall func(ctx context.Context, call ToolCall, result *ToolResult, dur time.Duration, err error)
}

// ErrToolLoopMaxIterations is returned when the LLM keeps requesting tools past
// ToolLoopOptions.MaxIterations. Classifiers may map this to CategoryPermanent.
var ErrToolLoopMaxIterations = errors.New("tool loop: max iterations exceeded")

// NewToolLoopNode returns an orchestrator.NodeFunc that implements a ReAct-style loop:
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
func NewToolLoopNode(client Client, opts ToolLoopOptions) orchestrator.NodeFunc {
	if client == nil {
		panic("orchestrator/llm: NewToolLoopNode requires a non-nil Client")
	}
	max := opts.MaxIterations
	if max <= 0 {
		max = 8
	}
	completionEvent := opts.EventOnComplete
	if completionEvent == "" {
		completionEvent = orchestrator.EventWaitUser
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
		sysPrompt = func(orchestrator.StateView, *orchestrator.Turn) string { return "" }
	}
	buildInitial := opts.InitialMessages
	if buildInitial == nil {
		buildInitial = defaultInitialMessages
	}

	return func(ctx context.Context, view orchestrator.StateView, turn *orchestrator.Turn, input *orchestrator.NodeInput) (*orchestrator.NodeResult, error) {
		tracer := obs.TracerFromCtx(ctx)
		o := obs.FromCtx(ctx)

		result := &orchestrator.NodeResult{}
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

			resp, err := callLLM(ctx, tracer, o, client, req, opts.OnLLMCall, iter, opts.Model)
			if err != nil {
				return nil, fmt.Errorf("tool loop: llm call: %w", err)
			}
			accumulateUsage(result, resp.Usage)

			if resp.StopReason != StopReasonToolUse || len(resp.ToolCalls) == 0 {
				result.Answer = resp.Content
				result.Event = completionEvent
				return result, nil
			}

			msgs = append(msgs, ChatMessage{
				Role:      "assistant",
				Content:   resp.Content,
				ToolCalls: resp.ToolCalls,
			})

			for _, call := range resp.ToolCalls {
				toolMsg, toolErr := invokeToolCall(ctx, tracer, o, toolMap, call, view, turn, result, opts.OnToolCall)
				if toolErr != nil && opts.OnToolError == ToolErrorPropagate {
					return nil, toolErr
				}
				msgs = append(msgs, toolMsg)
			}
		}

		return nil, fmt.Errorf("%w (phase=tool_loop, model=%s)", ErrToolLoopMaxIterations, opts.Model)
	}
}

func callLLM(
	ctx context.Context,
	tracer trace.Tracer,
	o *obs.Observability,
	client Client,
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

	o.RecordLLMDuration(ctx, dur.Seconds()*1000, model)
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

func invokeToolCall(
	ctx context.Context,
	tracer trace.Tracer,
	o *obs.Observability,
	toolMap map[string]Tool,
	call ToolCall,
	view orchestrator.StateView,
	turn *orchestrator.Turn,
	result *orchestrator.NodeResult,
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
		o.RecordToolCall(ctx, call.Name, "unknown")
		if hook != nil {
			hook(ctx, call, &ToolResult{Content: msg, IsError: true}, 0, nil)
		}
		return ChatMessage{Role: "tool", ToolCallID: call.ID, Content: msg}, nil
	}
	span.SetAttributes(attribute.Bool("tool.idempotent", tool.Definition().Idempotent))

	start := time.Now()
	toolRes, err := tool.Invoke(ctx, call, view, turn)
	dur := time.Since(start)

	o.RecordToolDuration(ctx, dur.Seconds()*1000, call.Name)

	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		o.RecordToolCall(ctx, call.Name, "error")
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
	o.RecordToolCall(ctx, call.Name, outcome)
	if hook != nil {
		hook(ctx, call, &toolRes, dur, nil)
	}

	if toolRes.Delta != nil {
		result.Delta.Merge(toolRes.Delta)
	}
	accumulateUsage(result, toolRes.Usage)

	return ChatMessage{
		Role:       "tool",
		ToolCallID: call.ID,
		Content:    toolRes.Content,
	}, nil
}

// defaultInitialMessages converts the view's conversation history to ChatMessages.
func defaultInitialMessages(view orchestrator.StateView, turn *orchestrator.Turn) []ChatMessage {
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

func accumulateUsage(result *orchestrator.NodeResult, src *orchestrator.Usage) {
	if src == nil {
		return
	}
	if result.Usage == nil {
		result.Usage = &orchestrator.Usage{}
	}
	result.Usage.Add(src)
}
