package llm

import (
	"context"
	"encoding/json"

	"github.com/baalamai/orchestrator"
	"github.com/baalamai/orchestrator/llm/react"
)

// ── Re-exports from the react subpackage ──────────────────────────────
//
// These aliases preserve the existing llm.X API while the canonical
// definitions live in react/. Adapters and consumers can use either name.

// Client is the outbound port for chat-completion calls with optional tool
// use. See react.Client for the full doc.
type Client = react.Client

// CompletionRequest is the canonical input to an LLM completion call. See
// react.CompletionRequest for the full doc, including the
// StableInstruction / DynamicContext / Cache / Thinking semantics.
type CompletionRequest = react.CompletionRequest

// ChatMessage is a single message in the conversation passed to the LLM.
type ChatMessage = react.ChatMessage

// CompletionResponse is the canonical LLM response. See react.CompletionResponse.
type CompletionResponse = react.CompletionResponse

// ToolCall is a single tool invocation requested by the LLM.
type ToolCall = react.ToolCall

// ToolDefinition describes a tool to the LLM.
type ToolDefinition = react.ToolDefinition

// ToolResult is the output of a Tool.Invoke.
type ToolResult = react.ToolResult

// CacheHint expresses the caller's caching preference.
type CacheHint = react.CacheHint

// ThinkingHint enables extended thinking / reasoning.
type ThinkingHint = react.ThinkingHint

// StopReason constants used by the ReAct loop. Adapters translate
// provider-specific strings into these canonical values.
const (
	StopReasonEndTurn   = react.StopReasonEndTurn
	StopReasonToolUse   = react.StopReasonToolUse
	StopReasonMaxTokens = react.StopReasonMaxTokens
)

// CacheHint values. CacheNone disables prompt caching entirely (zero value).
const (
	CacheNone  = react.CacheNone
	CacheShort = react.CacheShort
	CacheLong  = react.CacheLong
)

// ── Tool (orchestrator-aware variant) ─────────────────────────────────

// Tool is a callable exposed to the LLM by NewToolLoopNode. Unlike
// react.Tool, this variant takes orchestrator.StateView and *orchestrator.Turn
// so tools that need access to per-turn state can read them directly.
//
// NewToolLoopNode wraps every llm.Tool as a react.Tool internally, injecting
// view and turn through context.
type Tool interface {
	// Definition returns the schema and metadata used to advertise this tool
	// to the LLM.
	Definition() ToolDefinition
	// Invoke executes the tool with the call's arguments and returns a result.
	Invoke(ctx context.Context, call ToolCall, view orchestrator.StateView, turn *orchestrator.Turn) (ToolResult, error)
}

// ── Embedder ──────────────────────────────────────────────────────────

// Embedder is an outbound port for text embedding. It is intentionally
// separate from Client: not every provider exposes embeddings (Anthropic does
// not), and consumers that only need embeddings (vector indexing, semantic
// search) should not be forced to depend on a chat-completion adapter.
//
// Adapters that support both Client and Embedder typically expose them as two
// distinct constructors (e.g. NewClient / NewEmbedder in lib/llm-gemini)
// backed by a shared HTTP client.
type Embedder interface {
	Embed(ctx context.Context, req EmbedRequest) (*EmbedResponse, error)
}

// EmbedRequest is the canonical input to an embedding call.
type EmbedRequest struct {
	// Model is the provider-specific embedding model id (e.g. "text-embedding-004").
	Model string
	// Texts is the batch of inputs to embed; adapters preserve order in the output.
	Texts []string
	// TaskType is an optional hint for asymmetric embedding models. Common values:
	// "RETRIEVAL_QUERY", "RETRIEVAL_DOCUMENT", "SEMANTIC_SIMILARITY",
	// "CLASSIFICATION", "CLUSTERING". Adapters ignore unsupported values.
	TaskType string
}

// EmbedResponse is the canonical embedding response.
type EmbedResponse struct {
	// Vectors holds one embedding per input text, in the same order as the request.
	Vectors [][]float32
	// Usage is the token usage reported by the provider (may be nil if unsupported).
	Usage *orchestrator.Usage
}

// ── StructuredClient ──────────────────────────────────────────────────

// StructuredClient is the outbound port for 1-shot completion calls with
// optional structured output (JSON schema), prompt caching, and extended
// thinking.
//
// A StructuredClient is also a Client — implementers expose both surfaces
// because most consumers (form agents, capture extractors) want to fall back
// to plain Complete when they don't need a schema. The two-method split keeps
// Client minimal for tool-loop callers that never need schemas or caching.
//
// Use StructuredClient when:
//   - You need the model to return JSON matching a known schema.
//   - You want to take advantage of provider-side prompt caching
//     (StableInstruction + Cache hint).
//   - You need extended thinking budgets (Anthropic) or thinking levels
//     (Gemini).
//
// Use the plain Client when:
//   - You're running a ReAct tool loop (NewToolLoopNode requires Client, not
//     StructuredClient — schemas conflict with native tool calling).
//   - You only need a plain text response.
type StructuredClient interface {
	Client
	CompleteStructured(ctx context.Context, req StructuredRequest) (*StructuredResponse, error)
}

// StructuredRequest is the canonical input to a structured completion call.
//
// System / StableInstruction split:
//
//   - When StableInstruction is set, it is treated as the system prompt AND
//     marked cacheable (subject to Cache); DynamicContext is prepended as a
//     user-role message so cache markers stay valid across turns. System is
//     ignored.
//   - When StableInstruction is empty, System is used as the system prompt with
//     no cache marker.
//
// Mutual exclusivity is enforced by precedence (StableInstruction wins);
// supplying both is allowed but the adapter will not concatenate them.
type StructuredRequest struct {
	// Model is the provider-specific model identifier.
	Model string
	// Messages is the conversation history. Tool-use messages are not supported
	// here — use Client.Complete for tool loops.
	Messages []ChatMessage
	// Temperature controls sampling; 0 means deterministic.
	Temperature float64
	// MaxTokens caps the completion length; 0 means provider default.
	MaxTokens int32
	// TopP enables nucleus sampling; 0 means provider default.
	TopP float64
	// TopK enables top-k sampling; 0 means provider default.
	TopK int32

	// System is the system prompt. Used only when StableInstruction is empty.
	System string

	// StableInstruction is the cacheable, turn-invariant portion of the system
	// prompt. When non-empty, the adapter uses this AS the system prompt and
	// (if Cache != CacheNone) applies the provider's cache marker to it.
	StableInstruction string

	// DynamicContext is the per-turn, never-cached prefix. Adapters inject it
	// as a user-role message prepended to Messages so cache markers on the
	// stable prefix remain valid across turns.
	DynamicContext string

	// ResponseSchema enforces structured output. The adapter translates this
	// to the provider's preferred mechanism (genai responseSchema, Anthropic
	// synthetic tool, OpenAI response_format).
	ResponseSchema json.RawMessage

	// ResponseMIMEType is an optional output format hint (e.g. "application/json").
	// Honored by providers with native MIME selection; ignored elsewhere.
	ResponseMIMEType string

	// Cache controls prompt-cache retention. CacheNone disables caching entirely.
	Cache CacheHint

	// Thinking enables extended thinking. nil disables it.
	Thinking *ThinkingHint
}

// StructuredResponse is the canonical structured-completion response.
//
// Unlike CompletionResponse, this carries no ToolCalls — structured calls are
// single-shot and use the response Content (typically JSON when
// ResponseSchema is set). Use Client.Complete for tool-calling flows.
type StructuredResponse struct {
	// Content is the model's response text (the JSON when ResponseSchema is set).
	Content string
	// StopReason is the canonical termination reason; see the mapping table
	// on CompletionResponse.
	StopReason string
	// Usage is the token usage for this call.
	Usage *orchestrator.Usage
	// Thinking holds the model's chain-of-thought when extended thinking is
	// enabled.
	Thinking string
	// CacheCreated is the number of input tokens written to the prompt cache
	// by this call (cost of seeding the cache).
	CacheCreated int32
	// CacheRead is the number of input tokens served from the prompt cache
	// by this call (savings).
	CacheRead int32
}
