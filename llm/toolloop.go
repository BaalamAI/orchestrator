package llm

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/baalamai/orchestrator"
	"github.com/baalamai/orchestrator/llm/react"
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

// ── Context plumbing for view/turn ────────────────────────────────────
//
// llm.Tool.Invoke takes orchestrator.StateView and *orchestrator.Turn but
// react.Tool.Invoke does not. NewToolLoopNode injects view/turn through
// context before invoking react.RunLoop; the llm→react Tool adapter recovers
// them and forwards them to the underlying llm.Tool. This keeps the react
// package free of orchestrator FSM types.

type viewCtxKey struct{}
type turnCtxKey struct{}

// ViewFromContext recovers the orchestrator.StateView that NewToolLoopNode
// injected into ctx before invoking react.RunLoop. Returns (nil, false) when
// the loop was driven directly via react.RunLoop without the wrapper.
func ViewFromContext(ctx context.Context) (orchestrator.StateView, bool) {
	v, ok := ctx.Value(viewCtxKey{}).(orchestrator.StateView)
	return v, ok
}

// TurnFromContext recovers the orchestrator.Turn that NewToolLoopNode injected
// into ctx before invoking react.RunLoop. Returns (nil, false) when the loop
// was driven directly via react.RunLoop without the wrapper.
func TurnFromContext(ctx context.Context) (*orchestrator.Turn, bool) {
	t, ok := ctx.Value(turnCtxKey{}).(*orchestrator.Turn)
	return t, ok
}

func withViewTurn(ctx context.Context, view orchestrator.StateView, turn *orchestrator.Turn) context.Context {
	ctx = context.WithValue(ctx, viewCtxKey{}, view)
	ctx = context.WithValue(ctx, turnCtxKey{}, turn)
	return ctx
}

// llmToolAdapter wraps a llm.Tool as a react.Tool, recovering view/turn from
// ctx that the NewToolLoopNode wrapper injected.
type llmToolAdapter struct{ inner Tool }

func (a llmToolAdapter) Definition() ToolDefinition { return a.inner.Definition() }

func (a llmToolAdapter) Invoke(ctx context.Context, call ToolCall) (ToolResult, error) {
	view, _ := ViewFromContext(ctx)
	turn, _ := TurnFromContext(ctx)
	return a.inner.Invoke(ctx, call, view, turn)
}

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
//
// Internally this is a thin adapter over react.RunLoop: the ReAct loop, OTel
// spans (orchestrator.llm, orchestrator.tool), and observability metrics live
// in package react. NewToolLoopNode injects view/turn through context so the
// llm.Tool.Invoke signature stays unchanged for existing tools.
func NewToolLoopNode(client Client, opts ToolLoopOptions) orchestrator.NodeFunc {
	if client == nil {
		panic("orchestrator/llm: NewToolLoopNode requires a non-nil Client")
	}

	completionEvent := opts.EventOnComplete
	if completionEvent == "" {
		completionEvent = orchestrator.EventWaitUser
	}

	tools := make([]react.Tool, 0, len(opts.Tools))
	for _, t := range opts.Tools {
		tools = append(tools, llmToolAdapter{inner: t})
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
		msgs := buildInitial(view, turn)
		if input != nil && input.RAGContext != "" {
			msgs = append(msgs, ChatMessage{Role: "user", Content: "### Contexto:\n" + input.RAGContext})
		}

		loopRes, err := react.RunLoop(withViewTurn(ctx, view, turn), react.LoopOptions{
			Client:        client,
			Tools:         tools,
			Model:         opts.Model,
			System:        sysPrompt(view, turn),
			Messages:      msgs,
			Temperature:   opts.Temperature,
			MaxTokens:     opts.MaxTokens,
			MaxIterations: opts.MaxIterations,
			OnToolError:   react.ToolErrorPolicy(opts.OnToolError),
			OnLLMCall:     opts.OnLLMCall,
			OnToolCall:    opts.OnToolCall,
		})
		if err != nil {
			if errors.Is(err, react.ErrMaxIterations) {
				return nil, fmt.Errorf("%w (phase=tool_loop, model=%s)", ErrToolLoopMaxIterations, opts.Model)
			}
			return nil, fmt.Errorf("tool loop: %w", err)
		}

		result := &orchestrator.NodeResult{
			Answer: loopRes.Answer,
			Usage:  loopRes.Usage,
			Event:  completionEvent,
		}
		for _, inv := range loopRes.ToolResults {
			if inv.Result.Delta != nil {
				result.Delta.Merge(inv.Result.Delta)
			}
		}
		return result, nil
	}
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
