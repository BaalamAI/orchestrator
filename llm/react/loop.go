package react

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

// ToolErrorPolicy controls how tool execution errors propagate in RunLoop.
type ToolErrorPolicy int

const (
	// ToolErrorReport (default) converts the error to a tool result with
	// IsError=true and feeds it back to the LLM. The LLM can then decide to
	// retry, try a different tool, or give up.
	ToolErrorReport ToolErrorPolicy = iota
	// ToolErrorPropagate surfaces the error to the caller. Use for truly
	// fatal tool errors.
	ToolErrorPropagate
)

// LoopOptions configures a single RunLoop invocation.
type LoopOptions struct {
	// Client is the LLM adapter used for every completion call. Required.
	Client Client
	// Tools is the set of tools exposed to the LLM in every request.
	Tools []Tool
	// Model is the provider-specific model identifier forwarded to Client.
	Model string
	// System is the system prompt; ignored when StableInstruction is set.
	System string
	// StableInstruction is the cacheable, turn-invariant system prompt.
	// When non-empty, it supersedes System and (subject to Cache) is marked
	// cacheable by the adapter.
	StableInstruction string
	// DynamicContext is the per-turn, never-cached prefix injected by the
	// adapter as a user-role message prepended to Messages.
	DynamicContext string
	// Cache controls prompt-cache retention; CacheNone disables caching.
	Cache CacheHint
	// Thinking enables extended thinking; nil disables it.
	Thinking *ThinkingHint
	// Messages is the initial conversation passed to the LLM on the first
	// iteration. RunLoop appends assistant turns and tool result turns to its
	// internal copy.
	Messages []ChatMessage
	// Temperature controls sampling; 0 means deterministic.
	Temperature float64
	// MaxTokens caps each LLM completion; 0 means provider default.
	MaxTokens int32
	// MaxIterations caps the ReAct inner loop. Default: 8.
	MaxIterations int
	// OnToolError selects how tool failures propagate. Default: ToolErrorReport.
	OnToolError ToolErrorPolicy
	// OnLLMCall is invoked after each LLM completion (even on error).
	OnLLMCall func(ctx context.Context, req CompletionRequest, resp *CompletionResponse, dur time.Duration, err error)
	// OnToolCall is invoked after each tool invocation (even on error).
	OnToolCall func(ctx context.Context, call ToolCall, result *ToolResult, dur time.Duration, err error)
}

// ToolInvocation records a single tool execution emitted during RunLoop.
// Wrappers consume the list to merge deltas, read metadata, or build
// breakdown telemetry.
type ToolInvocation struct {
	Call   ToolCall
	Result ToolResult
	Err    error
}

// LoopResult is the outcome of a RunLoop call.
type LoopResult struct {
	// Answer is the assistant's final text once the loop terminates.
	Answer string
	// Messages is the full conversation produced by the loop: the initial
	// messages followed by every assistant/tool turn, in order.
	Messages []ChatMessage
	// Usage is the sum of token usage from every LLM call AND every tool
	// invocation that reported Usage.
	Usage *orchestrator.Usage
	// ToolResults lists every tool invocation in execution order.
	ToolResults []ToolInvocation
}

// ErrMaxIterations is returned when the LLM keeps requesting tools past
// LoopOptions.MaxIterations.
var ErrMaxIterations = errors.New("react: max iterations exceeded")

// RunLoop drives a ReAct-style tool-calling loop until the LLM returns a
// terminal stop reason or MaxIterations is exceeded.
func RunLoop(ctx context.Context, opts LoopOptions) (*LoopResult, error) {
	if opts.Client == nil {
		return nil, errors.New("react: RunLoop requires a non-nil Client")
	}
	max := opts.MaxIterations
	if max <= 0 {
		max = 8
	}

	toolMap := make(map[string]Tool, len(opts.Tools))
	toolDefs := make([]ToolDefinition, 0, len(opts.Tools))
	for _, t := range opts.Tools {
		def := t.Definition()
		toolMap[def.Name] = t
		toolDefs = append(toolDefs, def)
	}

	tracer := obs.TracerFromCtx(ctx)
	o := obs.FromCtx(ctx)

	msgs := append([]ChatMessage(nil), opts.Messages...)
	result := &LoopResult{}

	for iter := 0; iter < max; iter++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		req := CompletionRequest{
			Model:             opts.Model,
			System:            opts.System,
			StableInstruction: opts.StableInstruction,
			DynamicContext:    opts.DynamicContext,
			Cache:             opts.Cache,
			Thinking:          opts.Thinking,
			Messages:          msgs,
			Tools:             toolDefs,
			Temperature:       opts.Temperature,
			MaxTokens:         opts.MaxTokens,
		}

		resp, err := callLLM(ctx, tracer, o, opts.Client, req, opts.OnLLMCall, iter, opts.Model)
		if err != nil {
			return nil, fmt.Errorf("react: llm call: %w", err)
		}
		accumulateUsage(result, resp.Usage)

		if resp.StopReason != StopReasonToolUse || len(resp.ToolCalls) == 0 {
			result.Answer = resp.Content
			result.Messages = msgs
			return result, nil
		}

		msgs = append(msgs, ChatMessage{
			Role:      "assistant",
			Content:   resp.Content,
			ToolCalls: resp.ToolCalls,
		})

		for _, call := range resp.ToolCalls {
			toolMsg, inv, toolErr := invokeToolCall(ctx, tracer, o, toolMap, call, opts.OnToolCall)
			result.ToolResults = append(result.ToolResults, inv)
			accumulateUsage(result, inv.Result.Usage)
			if toolErr != nil && opts.OnToolError == ToolErrorPropagate {
				result.Messages = msgs
				return nil, toolErr
			}
			msgs = append(msgs, toolMsg)
		}
	}

	return nil, fmt.Errorf("%w (model=%s)", ErrMaxIterations, opts.Model)
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
	hook func(context.Context, ToolCall, *ToolResult, time.Duration, error),
) (ChatMessage, ToolInvocation, error) {
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
		errResult := ToolResult{Content: msg, IsError: true}
		if hook != nil {
			hook(ctx, call, &errResult, 0, nil)
		}
		return ChatMessage{
				Role:       "tool",
				ToolCallID: call.ID,
				ToolName:   call.Name,
				Content:    msg,
			},
			ToolInvocation{Call: call, Result: errResult},
			nil
	}
	span.SetAttributes(attribute.Bool("tool.idempotent", tool.Definition().Idempotent))

	start := time.Now()
	toolRes, err := tool.Invoke(ctx, call)
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
			},
			ToolInvocation{Call: call, Err: err},
			err
	}

	outcome := "ok"
	if toolRes.IsError {
		outcome = "error"
	}
	o.RecordToolCall(ctx, call.Name, outcome)
	if hook != nil {
		hook(ctx, call, &toolRes, dur, nil)
	}

	return ChatMessage{
			Role:       "tool",
			ToolCallID: call.ID,
			ToolName:   call.Name,
			Content:    toolRes.Content,
		},
		ToolInvocation{Call: call, Result: toolRes},
		nil
}

func accumulateUsage(result *LoopResult, src *orchestrator.Usage) {
	if src == nil {
		return
	}
	if result.Usage == nil {
		result.Usage = &orchestrator.Usage{}
	}
	result.Usage.Add(src)
}
