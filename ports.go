package orchestrator

import (
	"context"
	"encoding/json"
)

// ── LLM + Tool ports ──────────────────────────────────────────────────
// Outbound ports used by NewToolLoopNode to implement a ReAct-style agent.
// Adapters (Anthropic, OpenAI, Gemini) live in consumer services.

// LLMClient is the outbound port for chat-completion calls with optional tool use.
// Adapters wrap provider SDKs and translate tool_use / tool_calls to the canonical
// CompletionResponse format.
type LLMClient interface {
	Complete(ctx context.Context, req CompletionRequest) (*CompletionResponse, error)
}

// CompletionRequest is the canonical input to an LLM completion call.
type CompletionRequest struct {
	// Model is the provider-specific model identifier (e.g. "claude-opus-4-7").
	Model string
	// System is the system prompt; empty string means no system prompt.
	System string
	// Messages is the conversation history plus tool results.
	Messages []ChatMessage
	// Tools is the list of tools the model may invoke.
	Tools []ToolDefinition
	// Temperature controls sampling; 0 means deterministic.
	Temperature float64
	// MaxTokens caps the completion length; 0 means provider default.
	MaxTokens int32
}

// ChatMessage is a single message in the conversation passed to the LLM.
type ChatMessage struct {
	// Role is one of: "user", "assistant", "tool".
	Role string
	// Content is the text content (may be empty if ToolCalls is set).
	Content string
	// ToolCalls is set when Role=="assistant" and the model requested tools.
	ToolCalls []ToolCall
	// ToolCallID is set when Role=="tool" and identifies which call this answers.
	ToolCallID string
}

// CompletionResponse is the canonical LLM response.
type CompletionResponse struct {
	// Content is the text portion of the assistant response.
	Content string
	// ToolCalls is set when StopReason == "tool_use".
	ToolCalls []ToolCall
	// StopReason is one of: "end_turn", "tool_use", "max_tokens", "stop_sequence".
	StopReason string
	// Usage holds tokens consumed by this call.
	Usage *Usage
}

// StopReason constants used by the ToolLoopNode to drive its state machine.
const (
	StopReasonEndTurn   = "end_turn"
	StopReasonToolUse   = "tool_use"
	StopReasonMaxTokens = "max_tokens"
)

// ToolCall is a single tool invocation requested by the LLM.
type ToolCall struct {
	// ID uniquely identifies this call within the response.
	ID string
	// Name is the tool name (must match a registered Tool.Definition().Name).
	Name string
	// Input is the JSON-encoded arguments for the tool.
	Input json.RawMessage
}

// Tool is a callable exposed to the LLM by a ToolLoopNode.
type Tool interface {
	// Definition returns the schema and metadata used to advertise this tool to the LLM.
	Definition() ToolDefinition
	// Invoke executes the tool with the call's arguments and returns a result.
	// The view is a read-only snapshot; tools that need to mutate state must
	// return the mutations via ToolResult.Delta.
	Invoke(ctx context.Context, call ToolCall, view StateView, turn *Turn) (ToolResult, error)
}

// ToolDefinition describes a tool to the LLM.
type ToolDefinition struct {
	// Name is the tool identifier; must be unique within a ToolLoopNode.
	Name string
	// Description is the human-readable description shown to the LLM.
	Description string
	// InputSchema is the JSON Schema for tool arguments.
	InputSchema json.RawMessage
	// Idempotent indicates that re-invoking this tool with the same args is safe.
	// Checkpoint replay may re-execute phases mid-turn; tools with visible side-effects
	// (send message, create payment link, charge money) must be Idempotent=true and
	// deduplicate internally by natural key, or callers must accept double-execution risk.
	Idempotent bool
}

// ToolResult is the output of a Tool.Invoke.
type ToolResult struct {
	// Content is the text returned to the LLM as the tool's output.
	Content string
	// Delta contains optional state mutations to apply after the tool runs.
	Delta *StateDelta
	// Usage is optional token usage consumed by this tool (e.g. RAG embedding calls).
	Usage *Usage
	// IsError marks the content as an error message to be reported to the LLM.
	IsError bool
}

// ErrorClassifier maps provider-specific errors to orchestrator retry categories.
// Implementations inspect errors returned by LLM SDKs (OpenAI, Anthropic, Google, …)
// or downstream libraries and return the category that best describes them,
// letting the engine decide whether to retry.
//
// Callers wire a concrete classifier via WithClassifier. Without one, the engine
// uses noopClassifier, which treats every non-nil error as CategoryTransient —
// making RetryPolicy retry unknown failures by default. Pass a real classifier
// (or a custom ShouldRetry predicate) to short-circuit specific error types.
type ErrorClassifier interface {
	Classify(err error) ErrorCategory
}

// CostCalculator computes USD cost from per-call token usage for budget enforcement.
// Implementations own the pricing table for the models they support.
//
// Callers wire a concrete calculator via WithCostCalculator. Without one, the
// engine uses noopCostCalculator, which returns 0 — cost-based budget limits
// (BudgetConfig.MaxCostUSD) never trip unless a real calculator is supplied.
type CostCalculator interface {
	Calculate(model string, promptTokens, completionTokens int) float64
}

type noopClassifier struct{}

func (noopClassifier) Classify(err error) ErrorCategory {
	if err == nil {
		return CategoryUnknown
	}
	return CategoryTransient
}

type noopCostCalculator struct{}

func (noopCostCalculator) Calculate(string, int, int) float64 { return 0 }
