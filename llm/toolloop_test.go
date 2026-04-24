package llm

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/baalamai/orchestrator"
)

// ── Test doubles ──────────────────────────────────────────────────────

// fakeLLM is a scripted Client: returns responses from a pre-built list in order.
type fakeLLM struct {
	responses []CompletionResponse
	calls     []CompletionRequest
	idx       int
}

func (f *fakeLLM) Complete(_ context.Context, req CompletionRequest) (*CompletionResponse, error) {
	f.calls = append(f.calls, req)
	if f.idx >= len(f.responses) {
		return nil, errors.New("fakeLLM: no more scripted responses")
	}
	resp := f.responses[f.idx]
	f.idx++
	return &resp, nil
}

// fakeTool records invocations and returns a canned result.
type fakeTool struct {
	name       string
	idempotent bool
	result     ToolResult
	err        error
	calls      []ToolCall
}

func (t *fakeTool) Definition() ToolDefinition {
	return ToolDefinition{
		Name:        t.name,
		Description: "fake tool for tests",
		InputSchema: json.RawMessage(`{"type":"object"}`),
		Idempotent:  t.idempotent,
	}
}

func (t *fakeTool) Invoke(_ context.Context, call ToolCall, _ orchestrator.StateView, _ *orchestrator.Turn) (ToolResult, error) {
	t.calls = append(t.calls, call)
	return t.result, t.err
}

// ── Tests ─────────────────────────────────────────────────────────────

// TestToolLoop_DirectAnswer: LLM returns end_turn on first call, no tools.
func TestToolLoop_DirectAnswer(t *testing.T) {
	llm := &fakeLLM{responses: []CompletionResponse{
		{Content: "hola", StopReason: StopReasonEndTurn, Usage: &orchestrator.Usage{TotalTokens: 42}},
	}}

	node := NewToolLoopNode(llm, ToolLoopOptions{Model: "test"})
	res, err := node(context.Background(), noopView{}, orchestrator.NewTurn("c1", "hi"), &orchestrator.NodeInput{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Answer != "hola" {
		t.Errorf("expected 'hola', got %q", res.Answer)
	}
	if res.Event != orchestrator.EventWaitUser {
		t.Errorf("expected EventWaitUser, got %q", res.Event)
	}
	if res.Usage == nil || res.Usage.TotalTokens != 42 {
		t.Errorf("expected usage total=42, got %+v", res.Usage)
	}
	if len(llm.calls) != 1 {
		t.Errorf("expected 1 LLM call, got %d", len(llm.calls))
	}
}

// TestToolLoop_SingleToolCall: LLM asks for one tool, then returns end_turn.
func TestToolLoop_SingleToolCall(t *testing.T) {
	search := &fakeTool{
		name:   "search",
		result: ToolResult{Content: "found 3 products"},
	}
	llm := &fakeLLM{responses: []CompletionResponse{
		{
			StopReason: StopReasonToolUse,
			ToolCalls:  []ToolCall{{ID: "call_1", Name: "search", Input: json.RawMessage(`{"q":"x"}`)}},
			Usage:      &orchestrator.Usage{TotalTokens: 10},
		},
		{Content: "aquí hay 3", StopReason: StopReasonEndTurn, Usage: &orchestrator.Usage{TotalTokens: 5}},
	}}

	node := NewToolLoopNode(llm, ToolLoopOptions{Model: "test", Tools: []Tool{search}})
	res, err := node(context.Background(), noopView{}, orchestrator.NewTurn("c1", "buscar"), &orchestrator.NodeInput{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Answer != "aquí hay 3" {
		t.Errorf("expected answer 'aquí hay 3', got %q", res.Answer)
	}
	if len(search.calls) != 1 {
		t.Fatalf("expected tool invoked once, got %d", len(search.calls))
	}
	if res.Usage.TotalTokens != 15 {
		t.Errorf("expected accumulated tokens=15, got %d", res.Usage.TotalTokens)
	}
	// Second LLM call must include the tool result in messages.
	if len(llm.calls) != 2 {
		t.Fatalf("expected 2 LLM calls, got %d", len(llm.calls))
	}
	lastReq := llm.calls[1]
	found := false
	for _, m := range lastReq.Messages {
		if m.Role == "tool" && m.ToolCallID == "call_1" && strings.Contains(m.Content, "found 3 products") {
			found = true
		}
	}
	if !found {
		t.Error("expected tool result message in second LLM call history")
	}
}

// TestToolLoop_MultiRound: LLM chains two tools, then returns end_turn.
func TestToolLoop_MultiRound(t *testing.T) {
	search := &fakeTool{name: "search", result: ToolResult{Content: "ok"}}
	lookup := &fakeTool{name: "lookup", result: ToolResult{Content: "details"}}

	llm := &fakeLLM{responses: []CompletionResponse{
		{StopReason: StopReasonToolUse, ToolCalls: []ToolCall{{ID: "c1", Name: "search"}}},
		{StopReason: StopReasonToolUse, ToolCalls: []ToolCall{{ID: "c2", Name: "lookup"}}},
		{Content: "done", StopReason: StopReasonEndTurn},
	}}

	node := NewToolLoopNode(llm, ToolLoopOptions{Model: "t", Tools: []Tool{search, lookup}})
	res, err := node(context.Background(), noopView{}, orchestrator.NewTurn("c1", ""), &orchestrator.NodeInput{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Answer != "done" {
		t.Errorf("expected 'done', got %q", res.Answer)
	}
	if len(search.calls) != 1 || len(lookup.calls) != 1 {
		t.Errorf("expected each tool called once, got search=%d lookup=%d", len(search.calls), len(lookup.calls))
	}
}

// TestToolLoop_ToolError_ReportToLLM: tool errors are fed back to the LLM.
func TestToolLoop_ToolError_ReportToLLM(t *testing.T) {
	failing := &fakeTool{name: "buggy", err: errors.New("boom")}
	llm := &fakeLLM{responses: []CompletionResponse{
		{StopReason: StopReasonToolUse, ToolCalls: []ToolCall{{ID: "x", Name: "buggy"}}},
		{Content: "recovered", StopReason: StopReasonEndTurn},
	}}

	node := NewToolLoopNode(llm, ToolLoopOptions{
		Model:       "t",
		Tools:       []Tool{failing},
		OnToolError: ToolErrorReport,
	})
	res, err := node(context.Background(), noopView{}, orchestrator.NewTurn("c1", ""), &orchestrator.NodeInput{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Answer != "recovered" {
		t.Errorf("expected 'recovered', got %q", res.Answer)
	}
	if len(llm.calls) != 2 {
		t.Errorf("expected 2 LLM calls, got %d", len(llm.calls))
	}
	// The tool error message should be in the second call's history.
	found := false
	for _, m := range llm.calls[1].Messages {
		if m.Role == "tool" && strings.Contains(m.Content, "boom") {
			found = true
		}
	}
	if !found {
		t.Error("expected tool error surfaced to LLM as tool message")
	}
}

// TestToolLoop_ToolError_Propagate: tool errors surface to the engine.
func TestToolLoop_ToolError_Propagate(t *testing.T) {
	failing := &fakeTool{name: "buggy", err: errors.New("boom")}
	llm := &fakeLLM{responses: []CompletionResponse{
		{StopReason: StopReasonToolUse, ToolCalls: []ToolCall{{ID: "x", Name: "buggy"}}},
	}}

	node := NewToolLoopNode(llm, ToolLoopOptions{
		Model:       "t",
		Tools:       []Tool{failing},
		OnToolError: ToolErrorPropagate,
	})
	_, err := node(context.Background(), noopView{}, orchestrator.NewTurn("c1", ""), &orchestrator.NodeInput{})
	if err == nil {
		t.Fatal("expected error to propagate")
	}
	if !strings.Contains(err.Error(), "boom") {
		t.Errorf("expected error to include 'boom', got %v", err)
	}
}

// TestToolLoop_MaxIterations: infinite tool_use should abort.
func TestToolLoop_MaxIterations(t *testing.T) {
	loopTool := &fakeTool{name: "loop", result: ToolResult{Content: "x"}}
	responses := make([]CompletionResponse, 12)
	for i := range responses {
		responses[i] = CompletionResponse{
			StopReason: StopReasonToolUse,
			ToolCalls:  []ToolCall{{ID: "c", Name: "loop"}},
		}
	}
	llm := &fakeLLM{responses: responses}

	node := NewToolLoopNode(llm, ToolLoopOptions{
		Model:         "t",
		Tools:         []Tool{loopTool},
		MaxIterations: 3,
	})
	_, err := node(context.Background(), noopView{}, orchestrator.NewTurn("c1", ""), &orchestrator.NodeInput{})
	if !errors.Is(err, ErrToolLoopMaxIterations) {
		t.Fatalf("expected ErrToolLoopMaxIterations, got %v", err)
	}
	if len(loopTool.calls) != 3 {
		t.Errorf("expected 3 tool invocations (bounded by MaxIterations), got %d", len(loopTool.calls))
	}
}

// TestToolLoop_UnknownTool: LLM requests a tool that is not registered.
func TestToolLoop_UnknownTool(t *testing.T) {
	llm := &fakeLLM{responses: []CompletionResponse{
		{StopReason: StopReasonToolUse, ToolCalls: []ToolCall{{ID: "x", Name: "ghost"}}},
		{Content: "ok", StopReason: StopReasonEndTurn},
	}}
	node := NewToolLoopNode(llm, ToolLoopOptions{Model: "t"})

	res, err := node(context.Background(), noopView{}, orchestrator.NewTurn("c1", ""), &orchestrator.NodeInput{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Answer != "ok" {
		t.Errorf("expected 'ok', got %q", res.Answer)
	}
	// Second call should contain the "unknown tool" message.
	found := false
	for _, m := range llm.calls[1].Messages {
		if m.Role == "tool" && strings.Contains(m.Content, "unknown tool") {
			found = true
		}
	}
	if !found {
		t.Error("expected unknown-tool error surfaced to LLM")
	}
}

// TestToolLoop_DeltaMerged: tool results' deltas are merged into NodeResult.
func TestToolLoop_DeltaMerged(t *testing.T) {
	tool := &fakeTool{
		name: "t1",
		result: ToolResult{
			Content: "done",
			Delta:   &orchestrator.StateDelta{Updates: map[string]any{"k": "v"}},
		},
	}
	llm := &fakeLLM{responses: []CompletionResponse{
		{StopReason: StopReasonToolUse, ToolCalls: []ToolCall{{ID: "x", Name: "t1"}}},
		{Content: "fin", StopReason: StopReasonEndTurn},
	}}
	node := NewToolLoopNode(llm, ToolLoopOptions{Model: "t", Tools: []Tool{tool}})

	res, err := node(context.Background(), noopView{}, orchestrator.NewTurn("c1", ""), &orchestrator.NodeInput{})
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if got := res.Delta.Updates["k"]; got != "v" {
		t.Errorf("expected delta k=v merged into result, got %v", got)
	}
}

// TestToolLoop_Hooks: OnLLMCall and OnToolCall are invoked.
func TestToolLoop_Hooks(t *testing.T) {
	tool := &fakeTool{name: "t", result: ToolResult{Content: "ok"}}
	llm := &fakeLLM{responses: []CompletionResponse{
		{StopReason: StopReasonToolUse, ToolCalls: []ToolCall{{ID: "x", Name: "t"}}},
		{Content: "fin", StopReason: StopReasonEndTurn},
	}}

	var llmCalls, toolCalls int
	node := NewToolLoopNode(llm, ToolLoopOptions{
		Model: "t", Tools: []Tool{tool},
		OnLLMCall: func(_ context.Context, _ CompletionRequest, _ *CompletionResponse, _ time.Duration, _ error) {
			llmCalls++
		},
		OnToolCall: func(_ context.Context, _ ToolCall, _ *ToolResult, _ time.Duration, _ error) {
			toolCalls++
		},
	})

	_, err := node(context.Background(), noopView{}, orchestrator.NewTurn("c1", ""), &orchestrator.NodeInput{})
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if llmCalls != 2 {
		t.Errorf("expected 2 OnLLMCall invocations, got %d", llmCalls)
	}
	if toolCalls != 1 {
		t.Errorf("expected 1 OnToolCall invocation, got %d", toolCalls)
	}
}

// ── Test helpers ──────────────────────────────────────────────────────

// noopView is a zero-value orchestrator.StateView for isolated tool-loop tests.
type noopView struct{}

func (noopView) Get(string) (any, bool)           { return nil, false }
func (noopView) GetString(string) string          { return "" }
func (noopView) GetBool(string) bool              { return false }
func (noopView) HasFlag(string) bool              { return false }
func (noopView) State() map[string]any            { return map[string]any{} }
func (noopView) Messages() []orchestrator.Message { return nil }
func (noopView) Memory() map[string]any           { return nil }
