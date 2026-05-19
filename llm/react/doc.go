// Package react contains the orchestrator-agnostic core of the ReAct
// (tool-calling) loop. It is the foundation shared by:
//
//   - llm.NewToolLoopNode — the orchestrator.NodeFunc adapter in package llm.
//   - lib/loom/supervisor — the RAG supervisor that does not depend on the
//     orchestrator FSM.
//
// The split exists so consumers outside the orchestrator package (loom and
// future ones) can build ReAct loops without importing the FSM types
// (StateView, Turn, NodeFunc, NodeInput, StateDelta).
//
// Wiring of view/turn for tools that need them is the wrapper's
// responsibility: llm.NewToolLoopNode injects view and turn through context
// before invoking the loop; loom supervisors pass per-invocation data through
// their own Plugin contract instead.
//
// # Threading model
//
// RunLoop is synchronous; tool fan-out (e.g. auto-enrichment) is the tool's
// responsibility, not the loop's. Each LLM call is wrapped in an OTel span
// named "orchestrator.llm"; each tool invocation in a span named
// "orchestrator.tool". Naming is preserved across the package split so
// existing dashboards keep working.
//
// # Caching and thinking
//
// Cache and Thinking hints in LoopOptions are forwarded verbatim to
// llm.CompletionRequest. Whether the underlying adapter honors them depends
// on the provider; zero values are no-op.
package react
