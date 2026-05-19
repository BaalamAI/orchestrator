package llm_test

import (
	"context"
	"encoding/json"
	"fmt"

	orch "github.com/baalamai/orchestrator"
	"github.com/baalamai/orchestrator/llm"
	"github.com/baalamai/orchestrator/llm/llmtest"
	"github.com/baalamai/orchestrator/store"
	"github.com/baalamai/orchestrator/supervisor"
)

// ExampleNewToolLoopNode shows the full end-to-end wiring: a scripted Client
// drives a single tool call, the FakeTool returns a result, and the model
// produces a final answer on the second turn.
//
// In production the ScriptedClient is replaced by a real adapter
// (lib/llm-gemini, lib/llm-anthropic, …) and the FakeTool by an
// implementation that hits your backend.
func ExampleNewToolLoopNode() {
	// 1. Scripted model: first reply asks for a tool, second reply ends the turn.
	client := &llmtest.ScriptedClient{
		Responses: []llm.CompletionResponse{
			{
				StopReason: llm.StopReasonToolUse,
				ToolCalls: []llm.ToolCall{{
					ID:    "call_1",
					Name:  "search",
					Input: json.RawMessage(`{"query":"cafe"}`),
				}},
				Usage: &orch.Usage{TotalTokens: 10},
			},
			{
				Content:    "Encontré 3 productos para café.",
				StopReason: llm.StopReasonEndTurn,
				Usage:      &orch.Usage{TotalTokens: 8},
			},
		},
	}

	// 2. One tool, with a canned result.
	search := &llmtest.FakeTool{
		Name:        "search",
		Description: "search the product catalog",
		Result:      llm.ToolResult{Content: "found 3 products"},
	}

	// 3. NewToolLoopNode wraps client + tools into an orchestrator.NodeFunc.
	answer := llm.NewToolLoopNode(client, llm.ToolLoopOptions{
		Model: "test-model",
		Tools: []llm.Tool{search},
		SystemPrompt: func(_ orch.StateView, _ *orch.Turn) string {
			return "You are a product catalog assistant."
		},
	})

	// 4. Standard pipeline build — the node is just a NodeFunc.
	engine, err := orch.BuildFromConfig(orch.PipelineConfig{
		MaxSteps:   2,
		Supervisor: supervisor.NewLinear("answer"),
		Nodes:      []orch.NodeConfig{{Phase: "answer", Fn: answer}},
	})
	if err != nil {
		panic(err)
	}

	result, err := engine.Run(context.Background(), store.NewMemory(), orch.NewTurn("conv-1", "busca café"))
	if err != nil {
		panic(err)
	}

	fmt.Println(result.Answer)
	fmt.Println("tool calls:", len(search.Calls()))
	fmt.Println("llm calls:", len(client.Calls()))
	// Output:
	// Encontré 3 productos para café.
	// tool calls: 1
	// llm calls: 2
}
