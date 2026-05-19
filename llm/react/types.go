package react

import (
	"context"
	"encoding/json"

	"github.com/baalamai/orchestrator"
)

// Client is the outbound port for chat-completion calls with optional tool use.
// Adapters wrap provider SDKs and translate tool_use / tool_calls to the
// canonical CompletionResponse format. The same interface is re-exported from
// package llm as llm.Client; adapters in lib/llm-gemini, lib/llm-anthropic, …
// can satisfy either name interchangeably.
type Client interface {
	Complete(ctx context.Context, req CompletionRequest) (*CompletionResponse, error)
}

// CompletionRequest is the canonical input to an LLM completion call.
//
// System / StableInstruction split:
//
//   - When StableInstruction is set, it is treated as the system prompt AND
//     marked cacheable (subject to Cache); DynamicContext is prepended as a
//     user-role message so cache markers stay valid across turns. System is
//     ignored.
//   - When StableInstruction is empty, System is used as the system prompt
//     with no cache marker.
//
// Mutual exclusivity is enforced by precedence (StableInstruction wins);
// supplying both is allowed but the adapter will not concatenate them.
//
// Cache and Thinking are zero-value-no-op so callers that do not opt in see
// no behavior change.
type CompletionRequest struct {
	// Model is the provider-specific model identifier (e.g. "claude-opus-4-7").
	Model string
	// System is the system prompt; empty string means no system prompt. Used
	// only when StableInstruction is empty.
	System string
	// Messages is the conversation history plus tool results.
	Messages []ChatMessage
	// Tools is the list of tools the model may invoke.
	Tools []ToolDefinition
	// Temperature controls sampling; 0 means deterministic.
	Temperature float64
	// MaxTokens caps the completion length; 0 means provider default.
	MaxTokens int32
	// StableInstruction is the cacheable, turn-invariant portion of the system
	// prompt. When non-empty, the adapter uses this AS the system prompt and
	// (if Cache != CacheNone) applies the provider's cache marker to it.
	StableInstruction string
	// DynamicContext is the per-turn, never-cached prefix. Adapters inject it
	// as a user-role message prepended to Messages so cache markers on the
	// stable prefix remain valid across turns.
	DynamicContext string
	// Cache controls prompt-cache retention.
	Cache CacheHint
	// Thinking enables extended thinking / reasoning. nil disables it.
	Thinking *ThinkingHint
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
	// ToolName is the name of the tool whose result this message carries.
	// Set when Role=="tool" by the loop. Required by providers (Gemini, OpenAI)
	// that bind tool responses by name rather than by ID alone; adapters for
	// ID-only providers (Anthropic) may ignore it.
	ToolName string
}

// CompletionResponse is the canonical LLM response.
//
// Adapters MUST normalize StopReason to one of the StopReason* constants below
// — the ReAct loop treats any value other than StopReasonToolUse as terminal,
// so leaving a raw provider string here will silently break the loop.
//
// Provider → canonical mapping reference:
//
//	Anthropic  end_turn      → StopReasonEndTurn
//	           tool_use      → StopReasonToolUse
//	           max_tokens    → StopReasonMaxTokens
//	           stop_sequence → StopReasonEndTurn (no native equivalent)
//
//	OpenAI     stop          → StopReasonEndTurn
//	           tool_calls    → StopReasonToolUse
//	           length        → StopReasonMaxTokens
//	           function_call → StopReasonToolUse (legacy API)
//
//	Gemini     STOP                  → StopReasonEndTurn
//	           MAX_TOKENS            → StopReasonMaxTokens
//	           any candidate with    → StopReasonToolUse
//	           FunctionCall parts
type CompletionResponse struct {
	// Content is the text portion of the assistant response.
	Content string
	// ToolCalls is set when StopReason == StopReasonToolUse.
	//
	// Some providers (Gemini) do not assign IDs to tool calls natively.
	// Adapters MUST synthesize stable, request-unique IDs in that case so the
	// loop can match tool results back to the originating call via ToolCallID.
	ToolCalls []ToolCall
	// StopReason is the canonical termination reason — see the mapping table
	// in the CompletionResponse doc comment.
	StopReason string
	// Usage holds tokens consumed by this call.
	Usage *orchestrator.Usage
}

// StopReason constants used by the ReAct loop to drive its state machine.
// Adapters translate provider-specific strings into these canonical values.
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
	// ProviderData is an opaque per-call blob the adapter round-trips through
	// the loop. Used by providers that require call-bound state to be echoed
	// back when the assistant message is replayed in the next request (e.g.
	// Gemini 2.5 thought signatures on FunctionCall parts). The loop never
	// inspects this field; adapters that do not need it leave it nil.
	ProviderData []byte
}

// ToolDefinition describes a tool to the LLM.
type ToolDefinition struct {
	// Name is the tool identifier; must be unique within a loop.
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
	// Metadata carries opaque per-invocation data the loop does not interpret.
	// Tools use it to surface fields hooks may want (e.g. "file_names",
	// "vector_hits", "auto_enrich") without bloating the canonical result
	// shape. Keys and value types are agreed between the tool implementation
	// and the hook reader; the loop forwards Metadata unchanged.
	Metadata map[string]any
}

// CacheHint expresses the caller's caching preference. Adapters map it to the
// provider's nearest TTL bucket; CacheNone disables caching entirely.
type CacheHint int

const (
	// CacheNone disables prompt caching for this call.
	CacheNone CacheHint = iota
	// CacheShort prefers the provider's short-TTL bucket.
	// Anthropic: ~5min ephemeral. Gemini: default TTL.
	CacheShort
	// CacheLong prefers the provider's long-TTL bucket.
	// Anthropic: 1h ephemeral. Gemini: extended TTL.
	CacheLong
)

// ThinkingHint enables extended thinking / reasoning. Budget is the token
// allowance dedicated to internal reasoning; Level is a provider-specific tier
// hint (e.g. "LOW", "HIGH"). Either may be zero; the adapter applies what its
// provider supports and ignores the rest.
type ThinkingHint struct {
	// Budget is the maximum tokens spent on internal reasoning. <=0 disables thinking.
	Budget int32
	// Level is an optional provider-specific tier hint (Gemini: "LOW", "HIGH").
	Level string
}
