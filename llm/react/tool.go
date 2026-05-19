package react

import (
	"context"
)

// Tool is a callable exposed to the LLM by RunLoop.
//
// Unlike llm.Tool (the orchestrator-aware variant in package llm), this
// interface does NOT take orchestrator.StateView or orchestrator.Turn so
// consumers outside the orchestrator FSM (lib/loom and other future
// supervisors) can implement Tool without depending on the FSM types.
// Consumers that need view/turn (like llm.NewToolLoopNode) wrap a llm.Tool by
// injecting view and turn through context before calling Invoke.
type Tool interface {
	// Definition returns the schema and metadata used to advertise this tool
	// to the LLM.
	Definition() ToolDefinition
	// Invoke executes the tool with the call's arguments and returns a result.
	Invoke(ctx context.Context, call ToolCall) (ToolResult, error)
}
