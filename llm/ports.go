// Package llm contains the LLM subsystem: outbound ports (Client, Tool) and the
// ReAct-style tool-loop adapter (NewToolLoopNode) that wraps them into an
// orchestrator.NodeFunc.
//
// These ports are local to the tool-loop subsystem — the core Engine does not
// depend on them. Concrete Client adapters (Anthropic, OpenAI, Gemini) live in
// the consumer services.
package llm

import (
	"context"
	"encoding/json"

	"github.com/baalamai/orchestrator"
)

// Client is the outbound port for chat-completion calls with optional tool use.
// Adapters wrap provider SDKs and translate tool_use / tool_calls to the canonical
// CompletionResponse format.
type Client interface {
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
	Usage *orchestrator.Usage
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
	Invoke(ctx context.Context, call ToolCall, view orchestrator.StateView, turn *orchestrator.Turn) (ToolResult, error)
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
	Idempotent bool
}

// ToolResult is the output of a Tool.Invoke.
type ToolResult struct {
	// Content is the text returned to the LLM as the tool's output.
	Content string
	// Delta contains optional state mutations to apply after the tool runs.
	Delta *orchestrator.StateDelta
	// Usage is optional token usage consumed by this tool (e.g. RAG embedding calls).
	Usage *orchestrator.Usage
	// IsError marks the content as an error message to be reported to the LLM.
	IsError bool
}
