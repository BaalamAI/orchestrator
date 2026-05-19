// Package llm is the orchestrator's LLM subsystem. It defines the outbound
// ports (Client, StructuredClient, Embedder, Tool) and the ReAct-style adapter
// (NewToolLoopNode) that turns a Client + a set of Tools into an
// orchestrator.NodeFunc.
//
// The core orchestrator Engine does NOT depend on this package. That keeps the
// dependency tree small for callers that wire their own node functions.
//
// # Architecture
//
//	your provider SDK     ┐
//	(anthropic-sdk-go,    ├──► your *Client (implements llm.Client)
//	 google.golang.org/   ┘                  │
//	 genai, openai-go)                       │
//	                                         ▼
//	                                NewToolLoopNode  ──► orchestrator.NodeFunc
//	                                         ▲
//	                                         │
//	                              []llm.Tool ┘
//
// One Client adapter per provider, one Tool per capability exposed to the
// model. The loop, retry, observability and state delta merging are handled
// by NewToolLoopNode; adapters do nothing but translate to and from the
// provider's wire format.
//
// # Implementing a Client adapter
//
// A minimal adapter is ~150 lines: a constructor that holds the SDK client,
// a Complete method that translates CompletionRequest to the provider's
// request type, calls the SDK, and translates the response back. Two things
// adapter authors must get right:
//
//  1. Normalize the StopReason. The ReAct loop branches on it; raw provider
//     strings break the loop silently. See the mapping table in
//     CompletionResponse's doc comment.
//
//  2. Synthesize tool-call IDs if the provider does not assign them (Gemini).
//     The loop matches tool results to calls via ToolCall.ID, so IDs must be
//     stable and unique within a single response.
//
// Reference adapters in the BaalamAI ecosystem:
//
//   - github.com/baalamai/llm-gemini   — google.golang.org/genai
//   - github.com/baalamai/llm-anthropic — github.com/anthropics/anthropic-sdk-go
//
// # Package layout
//
// The canonical types for the ReAct loop (Client, CompletionRequest,
// CompletionResponse, ChatMessage, ToolCall, ToolDefinition, ToolResult,
// StopReason*, CacheHint, ThinkingHint) live in subpackage
// github.com/baalamai/orchestrator/llm/react along with RunLoop itself.
// Package llm re-exports those types via aliases so existing adapters and
// consumers keep using the llm.X names they already know — both names refer
// to the same underlying definitions.
//
// Types that belong only to this package:
//
//   - llm.Tool — variant of react.Tool whose Invoke takes the orchestrator
//     StateView and Turn. NewToolLoopNode wraps llm.Tool as react.Tool by
//     injecting view/turn through context.
//   - llm.Embedder — the embeddings port.
//   - llm.StructuredClient — the 1-shot structured-output port.
//
// # Testing
//
// The llmtest subpackage provides ScriptedClient, FakeClient, RecordingClient
// and FakeTool — sufficient to unit-test any node that wraps NewToolLoopNode
// without touching a real provider.
//
// # Embeddings
//
// The Embedder port is intentionally separate from Client. Not every provider
// exposes embeddings (Anthropic does not); consumers that only embed should
// not depend on a chat-completion adapter. Adapters that support both
// typically expose two constructors (NewClient / NewEmbedder).
//
// # Structured output and caching
//
// StructuredClient extends Client with a second method (CompleteStructured)
// for one-shot calls that need JSON schema enforcement, prompt caching, or
// extended thinking. Use it for form-filling, extraction, classification —
// any flow where the response shape is known up front and tool calling is
// not involved.
//
// The two-method split is deliberate: NewToolLoopNode only needs Client, so
// adapters can implement that surface without committing to the schema /
// caching / thinking translations.
//
// # Concurrency
//
// Client and Embedder implementations must be safe for concurrent use — the
// Engine runs concurrent phases against the same client when configured with
// a ParallelSupervisor. Adapter authors should share the underlying SDK
// client (which is itself thread-safe in all major SDKs) across goroutines
// rather than constructing per-call clients.
package llm
