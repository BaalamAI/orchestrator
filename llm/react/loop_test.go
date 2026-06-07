package react

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

// scriptedClient is a fake Client returning pre-built responses in order.
type scriptedClient struct {
	responses []CompletionResponse
	calls     []CompletionRequest
	idx       int
}

func (s *scriptedClient) Complete(_ context.Context, req CompletionRequest) (*CompletionResponse, error) {
	s.calls = append(s.calls, req)
	if s.idx >= len(s.responses) {
		return nil, errors.New("scriptedClient: no more scripted responses")
	}
	resp := s.responses[s.idx]
	s.idx++
	return &resp, nil
}

// scriptedTool records invocations and returns a canned result.
type scriptedTool struct {
	name       string
	idempotent bool
	result     ToolResult
	err        error
	calls      []ToolCall
}

func (t *scriptedTool) Definition() ToolDefinition {
	return ToolDefinition{
		Name:        t.name,
		Description: "scripted tool for tests",
		InputSchema: json.RawMessage(`{"type":"object"}`),
		Idempotent:  t.idempotent,
	}
}

func (t *scriptedTool) Invoke(_ context.Context, call ToolCall) (ToolResult, error) {
	t.calls = append(t.calls, call)
	return t.result, t.err
}

// ── Tests ─────────────────────────────────────────────────────────────

// TestRunLoop_DirectAnswer: LLM returns end_turn on first call, no tools.
func TestRunLoop_DirectAnswer(t *testing.T) {
	client := &scriptedClient{responses: []CompletionResponse{
		{Content: "hola", StopReason: StopReasonEndTurn, Usage: &orchestrator.Usage{TotalTokens: 42}},
	}}

	res, err := RunLoop(context.Background(), LoopOptions{
		Client:   client,
		Model:    "test",
		Messages: []ChatMessage{{Role: "user", Content: "hi"}},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Answer != "hola" {
		t.Errorf("expected 'hola', got %q", res.Answer)
	}
	if res.Usage == nil || res.Usage.TotalTokens != 42 {
		t.Errorf("expected usage total=42, got %+v", res.Usage)
	}
	if len(client.calls) != 1 {
		t.Errorf("expected 1 LLM call, got %d", len(client.calls))
	}
	if len(res.ToolResults) != 0 {
		t.Errorf("expected no tool invocations, got %d", len(res.ToolResults))
	}
}

// TestRunLoop_SingleToolCall: LLM asks for one tool, then returns end_turn.
func TestRunLoop_SingleToolCall(t *testing.T) {
	search := &scriptedTool{
		name:   "search",
		result: ToolResult{Content: "found 3 products"},
	}
	client := &scriptedClient{responses: []CompletionResponse{
		{
			StopReason: StopReasonToolUse,
			ToolCalls:  []ToolCall{{ID: "call_1", Name: "search", Input: json.RawMessage(`{"q":"x"}`)}},
			Usage:      &orchestrator.Usage{TotalTokens: 10},
		},
		{Content: "aquí hay 3", StopReason: StopReasonEndTurn, Usage: &orchestrator.Usage{TotalTokens: 5}},
	}}

	res, err := RunLoop(context.Background(), LoopOptions{
		Client:   client,
		Model:    "test",
		Tools:    []Tool{search},
		Messages: []ChatMessage{{Role: "user", Content: "buscar"}},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Answer != "aquí hay 3" {
		t.Errorf("expected 'aquí hay 3', got %q", res.Answer)
	}
	if len(search.calls) != 1 {
		t.Fatalf("expected tool invoked once, got %d", len(search.calls))
	}
	if res.Usage.TotalTokens != 15 {
		t.Errorf("expected accumulated tokens=15, got %d", res.Usage.TotalTokens)
	}
	if len(client.calls) != 2 {
		t.Fatalf("expected 2 LLM calls, got %d", len(client.calls))
	}
	lastReq := client.calls[1]
	found := false
	for _, m := range lastReq.Messages {
		if m.Role == "tool" && m.ToolCallID == "call_1" && strings.Contains(m.Content, "found 3 products") {
			found = true
		}
	}
	if !found {
		t.Error("expected tool result message in second LLM call history")
	}
	if len(res.ToolResults) != 1 {
		t.Fatalf("expected 1 tool invocation recorded, got %d", len(res.ToolResults))
	}
	if res.ToolResults[0].Result.Content != "found 3 products" {
		t.Errorf("expected ToolResults[0].Result.Content=%q, got %q", "found 3 products", res.ToolResults[0].Result.Content)
	}
}

// TestRunLoop_MultiRound: LLM chains two tools, then returns end_turn.
func TestRunLoop_MultiRound(t *testing.T) {
	search := &scriptedTool{name: "search", result: ToolResult{Content: "ok"}}
	lookup := &scriptedTool{name: "lookup", result: ToolResult{Content: "details"}}

	client := &scriptedClient{responses: []CompletionResponse{
		{StopReason: StopReasonToolUse, ToolCalls: []ToolCall{{ID: "c1", Name: "search"}}},
		{StopReason: StopReasonToolUse, ToolCalls: []ToolCall{{ID: "c2", Name: "lookup"}}},
		{Content: "done", StopReason: StopReasonEndTurn},
	}}

	res, err := RunLoop(context.Background(), LoopOptions{
		Client: client,
		Model:  "t",
		Tools:  []Tool{search, lookup},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Answer != "done" {
		t.Errorf("expected 'done', got %q", res.Answer)
	}
	if len(search.calls) != 1 || len(lookup.calls) != 1 {
		t.Errorf("expected each tool called once, got search=%d lookup=%d", len(search.calls), len(lookup.calls))
	}
	if len(res.ToolResults) != 2 {
		t.Errorf("expected 2 ToolResults, got %d", len(res.ToolResults))
	}
}

// TestRunLoop_ToolError_ReportToLLM: tool errors are fed back to the LLM.
func TestRunLoop_ToolError_ReportToLLM(t *testing.T) {
	failing := &scriptedTool{name: "buggy", err: errors.New("boom")}
	client := &scriptedClient{responses: []CompletionResponse{
		{StopReason: StopReasonToolUse, ToolCalls: []ToolCall{{ID: "x", Name: "buggy"}}},
		{Content: "recovered", StopReason: StopReasonEndTurn},
	}}

	res, err := RunLoop(context.Background(), LoopOptions{
		Client:      client,
		Model:       "t",
		Tools:       []Tool{failing},
		OnToolError: ToolErrorReport,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Answer != "recovered" {
		t.Errorf("expected 'recovered', got %q", res.Answer)
	}
	if len(client.calls) != 2 {
		t.Errorf("expected 2 LLM calls, got %d", len(client.calls))
	}
	found := false
	for _, m := range client.calls[1].Messages {
		if m.Role == "tool" && strings.Contains(m.Content, "boom") {
			found = true
		}
	}
	if !found {
		t.Error("expected tool error surfaced to LLM as tool message")
	}
	if len(res.ToolResults) != 1 || res.ToolResults[0].Err == nil {
		t.Errorf("expected one ToolResults entry with Err set, got %+v", res.ToolResults)
	}
}

// TestRunLoop_ToolError_Propagate: tool errors surface to the caller.
func TestRunLoop_ToolError_Propagate(t *testing.T) {
	failing := &scriptedTool{name: "buggy", err: errors.New("boom")}
	client := &scriptedClient{responses: []CompletionResponse{
		{StopReason: StopReasonToolUse, ToolCalls: []ToolCall{{ID: "x", Name: "buggy"}}},
	}}

	_, err := RunLoop(context.Background(), LoopOptions{
		Client:      client,
		Model:       "t",
		Tools:       []Tool{failing},
		OnToolError: ToolErrorPropagate,
	})
	if err == nil {
		t.Fatal("expected error to propagate")
	}
	if !strings.Contains(err.Error(), "boom") {
		t.Errorf("expected error to include 'boom', got %v", err)
	}
}

// TestRunLoop_MaxIterations: infinite tool_use aborts after MaxIterations.
func TestRunLoop_MaxIterations(t *testing.T) {
	loopTool := &scriptedTool{name: "loop", result: ToolResult{Content: "x"}}
	responses := make([]CompletionResponse, 12)
	for i := range responses {
		responses[i] = CompletionResponse{
			StopReason: StopReasonToolUse,
			ToolCalls:  []ToolCall{{ID: "c", Name: "loop"}},
		}
	}
	client := &scriptedClient{responses: responses}

	res, err := RunLoop(context.Background(), LoopOptions{
		Client:        client,
		Model:         "t",
		Tools:         []Tool{loopTool},
		MaxIterations: 3,
	})
	if !errors.Is(err, ErrMaxIterations) {
		t.Fatalf("expected ErrMaxIterations, got %v", err)
	}
	if len(loopTool.calls) != 3 {
		t.Errorf("expected 3 tool invocations (bounded by MaxIterations), got %d", len(loopTool.calls))
	}
	// On max-iterations RunLoop returns the partial result it gathered so
	// callers can salvage the work instead of discarding it.
	if res == nil {
		t.Fatal("expected partial result alongside ErrMaxIterations, got nil")
	}
	if len(res.ToolResults) != 3 {
		t.Errorf("expected 3 tool results in partial output, got %d", len(res.ToolResults))
	}
}

// TestRunLoop_UnknownTool: LLM requests a tool that is not registered.
func TestRunLoop_UnknownTool(t *testing.T) {
	client := &scriptedClient{responses: []CompletionResponse{
		{StopReason: StopReasonToolUse, ToolCalls: []ToolCall{{ID: "x", Name: "ghost"}}},
		{Content: "ok", StopReason: StopReasonEndTurn},
	}}
	res, err := RunLoop(context.Background(), LoopOptions{
		Client: client,
		Model:  "t",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.Answer != "ok" {
		t.Errorf("expected 'ok', got %q", res.Answer)
	}
	found := false
	for _, m := range client.calls[1].Messages {
		if m.Role == "tool" && strings.Contains(m.Content, "unknown tool") {
			found = true
		}
	}
	if !found {
		t.Error("expected unknown-tool error surfaced to LLM")
	}
	if len(res.ToolResults) != 1 || !res.ToolResults[0].Result.IsError {
		t.Errorf("expected ToolResults to record IsError=true entry, got %+v", res.ToolResults)
	}
}

// TestRunLoop_DeltaPreserved: tool results' deltas are surfaced via
// LoopResult.ToolResults so wrappers can merge them into orchestrator state.
func TestRunLoop_DeltaPreserved(t *testing.T) {
	tool := &scriptedTool{
		name: "t1",
		result: ToolResult{
			Content: "done",
			Delta:   &orchestrator.StateDelta{Updates: map[string]any{"k": "v"}},
		},
	}
	client := &scriptedClient{responses: []CompletionResponse{
		{StopReason: StopReasonToolUse, ToolCalls: []ToolCall{{ID: "x", Name: "t1"}}},
		{Content: "fin", StopReason: StopReasonEndTurn},
	}}
	res, err := RunLoop(context.Background(), LoopOptions{
		Client: client,
		Model:  "t",
		Tools:  []Tool{tool},
	})
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	if len(res.ToolResults) != 1 {
		t.Fatalf("expected 1 ToolResults entry, got %d", len(res.ToolResults))
	}
	d := res.ToolResults[0].Result.Delta
	if d == nil || d.Updates["k"] != "v" {
		t.Errorf("expected delta k=v preserved in ToolResults, got %+v", d)
	}
}

// TestRunLoop_MetadataPreserved: ToolResult.Metadata is surfaced unchanged.
func TestRunLoop_MetadataPreserved(t *testing.T) {
	tool := &scriptedTool{
		name: "t1",
		result: ToolResult{
			Content: "ok",
			Metadata: map[string]any{
				"file_names":  []string{"a.pdf"},
				"vector_hits": 7,
			},
		},
	}
	client := &scriptedClient{responses: []CompletionResponse{
		{StopReason: StopReasonToolUse, ToolCalls: []ToolCall{{ID: "x", Name: "t1"}}},
		{Content: "fin", StopReason: StopReasonEndTurn},
	}}
	res, err := RunLoop(context.Background(), LoopOptions{
		Client: client,
		Model:  "t",
		Tools:  []Tool{tool},
	})
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	md := res.ToolResults[0].Result.Metadata
	if md == nil || md["vector_hits"] != 7 {
		t.Errorf("expected metadata.vector_hits=7 preserved, got %+v", md)
	}
}

// TestRunLoop_Hooks: OnLLMCall and OnToolCall are invoked.
func TestRunLoop_Hooks(t *testing.T) {
	tool := &scriptedTool{name: "t", result: ToolResult{Content: "ok"}}
	client := &scriptedClient{responses: []CompletionResponse{
		{StopReason: StopReasonToolUse, ToolCalls: []ToolCall{{ID: "x", Name: "t"}}},
		{Content: "fin", StopReason: StopReasonEndTurn},
	}}

	var llmCalls, toolCalls int
	_, err := RunLoop(context.Background(), LoopOptions{
		Client: client,
		Model:  "t",
		Tools:  []Tool{tool},
		OnLLMCall: func(_ context.Context, _ CompletionRequest, _ *CompletionResponse, _ time.Duration, _ error) {
			llmCalls++
		},
		OnToolCall: func(_ context.Context, _ ToolCall, _ *ToolResult, _ time.Duration, _ error) {
			toolCalls++
		},
	})
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

// TestRunLoop_ForwardsCacheAndThinking: the new CompletionRequest fields are
// forwarded verbatim from LoopOptions to every Client.Complete call.
func TestRunLoop_ForwardsCacheAndThinking(t *testing.T) {
	client := &scriptedClient{responses: []CompletionResponse{
		{Content: "ok", StopReason: StopReasonEndTurn},
	}}
	thinking := &ThinkingHint{Budget: 2048, Level: "HIGH"}

	_, err := RunLoop(context.Background(), LoopOptions{
		Client:            client,
		Model:             "t",
		StableInstruction: "stable system",
		DynamicContext:    "per-turn",
		Cache:             CacheLong,
		Thinking:          thinking,
	})
	if err != nil {
		t.Fatalf("unexpected: %v", err)
	}
	req := client.calls[0]
	if req.StableInstruction != "stable system" {
		t.Errorf("StableInstruction not forwarded: %q", req.StableInstruction)
	}
	if req.DynamicContext != "per-turn" {
		t.Errorf("DynamicContext not forwarded: %q", req.DynamicContext)
	}
	if req.Cache != CacheLong {
		t.Errorf("Cache not forwarded: %v", req.Cache)
	}
	if req.Thinking != thinking {
		t.Errorf("Thinking not forwarded (want %p, got %p)", thinking, req.Thinking)
	}
}

// TestRunLoop_NilClient: returns a clear error instead of panicking.
func TestRunLoop_NilClient(t *testing.T) {
	_, err := RunLoop(context.Background(), LoopOptions{Model: "t"})
	if err == nil || !strings.Contains(err.Error(), "non-nil Client") {
		t.Fatalf("expected non-nil Client error, got %v", err)
	}
}

// TestRunLoop_RespectsContextCancellation: a cancelled ctx aborts before the
// next LLM call.
func TestRunLoop_RespectsContextCancellation(t *testing.T) {
	tool := &scriptedTool{name: "t", result: ToolResult{Content: "ok"}}
	client := &scriptedClient{responses: []CompletionResponse{
		{StopReason: StopReasonToolUse, ToolCalls: []ToolCall{{ID: "x", Name: "t"}}},
		{Content: "should not be reached", StopReason: StopReasonEndTurn},
	}}
	ctx, cancel := context.WithCancel(context.Background())

	_, err := RunLoop(ctx, LoopOptions{
		Client: client,
		Model:  "t",
		Tools:  []Tool{tool},
		OnToolCall: func(_ context.Context, _ ToolCall, _ *ToolResult, _ time.Duration, _ error) {
			cancel()
		},
	})
	if err == nil {
		t.Fatal("expected ctx.Err() to abort the loop")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

// toolMsgContent returns the Content of the tool-result message for callID in
// req's history, or "" if absent.
func toolMsgContent(req CompletionRequest, callID string) string {
	for _, m := range req.Messages {
		if m.Role == "tool" && m.ToolCallID == callID {
			return m.Content
		}
	}
	return ""
}

// TestRunLoop_EmptyToolContent_DefaultBackstop: a tool returning blank Content
// without an error must NOT feed an empty message back to the LLM (which would
// trigger a re-fetch loop). The loop substitutes the default placeholder.
func TestRunLoop_EmptyToolContent_DefaultBackstop(t *testing.T) {
	blank := &scriptedTool{name: "blank", result: ToolResult{Content: ""}}
	client := &scriptedClient{responses: []CompletionResponse{
		{StopReason: StopReasonToolUse, ToolCalls: []ToolCall{{ID: "c1", Name: "blank"}}},
		{Content: "done", StopReason: StopReasonEndTurn},
	}}

	res, err := RunLoop(context.Background(), LoopOptions{
		Client: client,
		Model:  "t",
		Tools:  []Tool{blank},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// The tool is invoked exactly once — the backstop terminates the loop
	// instead of letting the LLM re-call it.
	if len(blank.calls) != 1 {
		t.Fatalf("expected tool invoked once, got %d", len(blank.calls))
	}
	// The second LLM call must see the placeholder, never an empty tool message.
	got := toolMsgContent(client.calls[1], "c1")
	if got != defaultEmptyToolResultText {
		t.Errorf("expected tool message %q, got %q", defaultEmptyToolResultText, got)
	}
	// The recorded invocation carries the substituted content, not blank.
	if res.ToolResults[0].Result.Content != defaultEmptyToolResultText {
		t.Errorf("expected ToolResults[0].Content=%q, got %q", defaultEmptyToolResultText, res.ToolResults[0].Result.Content)
	}
}

// TestRunLoop_EmptyToolContent_CustomText: EmptyToolResultText overrides the
// default placeholder.
func TestRunLoop_EmptyToolContent_CustomText(t *testing.T) {
	const custom = "No se encontró información para esta consulta."
	blank := &scriptedTool{name: "blank", result: ToolResult{Content: "   "}} // whitespace counts as blank
	client := &scriptedClient{responses: []CompletionResponse{
		{StopReason: StopReasonToolUse, ToolCalls: []ToolCall{{ID: "c1", Name: "blank"}}},
		{Content: "done", StopReason: StopReasonEndTurn},
	}}

	_, err := RunLoop(context.Background(), LoopOptions{
		Client:              client,
		Model:               "t",
		Tools:               []Tool{blank},
		EmptyToolResultText: custom,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got := toolMsgContent(client.calls[1], "c1"); got != custom {
		t.Errorf("expected tool message %q, got %q", custom, got)
	}
}

// TestRunLoop_ErrorResultNotBackstopped: an IsError result with blank Content
// is NOT substituted — error reporting owns that path.
func TestRunLoop_ErrorResultNotBackstopped(t *testing.T) {
	errTool := &scriptedTool{name: "bad", result: ToolResult{Content: "boom", IsError: true}}
	client := &scriptedClient{responses: []CompletionResponse{
		{StopReason: StopReasonToolUse, ToolCalls: []ToolCall{{ID: "c1", Name: "bad"}}},
		{Content: "done", StopReason: StopReasonEndTurn},
	}}

	res, err := RunLoop(context.Background(), LoopOptions{
		Client: client,
		Model:  "t",
		Tools:  []Tool{errTool},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if res.ToolResults[0].Result.Content != "boom" {
		t.Errorf("error content must be preserved verbatim, got %q", res.ToolResults[0].Result.Content)
	}
}
