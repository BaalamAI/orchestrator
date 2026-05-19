// Package llmtest provides reusable test doubles for the llm.Client,
// llm.StructuredClient and llm.Tool ports. Adapter authors (lib/llm-gemini,
// lib/llm-anthropic, …) and consumers of NewToolLoopNode / CompleteStructured
// use these to write fast, hermetic unit tests without hitting a real provider.
//
// Client doubles (for tool-loop tests):
//
//   - ScriptedClient — returns a fixed list of CompletionResponses in order.
//     Use when the test knows the exact sequence of model replies it wants
//     to drive.
//   - RecordingClient — wraps another Client and records every request /
//     response pair. Use to assert what was sent to the model.
//   - FakeClient — a single canned response, useful for quick smoke tests.
//
// StructuredClient doubles (for one-shot JSON / form-extractor tests):
//
//   - ScriptedStructuredClient — returns a fixed list of StructuredResponses
//     in order. Also satisfies llm.Client by returning the same payload as a
//     CompletionResponse so it can be passed wherever either interface is
//     expected.
//   - FakeStructuredClient — single canned StructuredResponse.
//
// FakeTool is the matching Tool double: it returns a canned ToolResult and
// records every invocation.
package llmtest

import (
	"context"
	"encoding/json"
	"errors"
	"sync"

	"github.com/baalamai/orchestrator"
	"github.com/baalamai/orchestrator/llm"
)

// ── Client doubles ────────────────────────────────────────────────────

// ScriptedClient implements llm.Client by returning Responses in order.
// Calls past the end of the script return ErrScriptExhausted.
//
// It is safe for concurrent use; mu protects idx and the calls slice.
type ScriptedClient struct {
	Responses []llm.CompletionResponse

	mu    sync.Mutex
	idx   int
	calls []llm.CompletionRequest
}

// ErrScriptExhausted is returned when Complete is called more times than
// the ScriptedClient has scripted responses for.
var ErrScriptExhausted = errors.New("llmtest: scripted client exhausted")

// Complete returns the next scripted response, or ErrScriptExhausted.
func (s *ScriptedClient) Complete(_ context.Context, req llm.CompletionRequest) (*llm.CompletionResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls = append(s.calls, req)
	if s.idx >= len(s.Responses) {
		return nil, ErrScriptExhausted
	}
	resp := s.Responses[s.idx]
	s.idx++
	return &resp, nil
}

// Calls returns a copy of every request received so far. Tests use this to
// assert what was sent to the model (messages, tool list, model id).
func (s *ScriptedClient) Calls() []llm.CompletionRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]llm.CompletionRequest, len(s.calls))
	copy(out, s.calls)
	return out
}

// FakeClient implements llm.Client by always returning the same Response.
// Useful when the test only cares that *some* completion happened.
type FakeClient struct {
	Response llm.CompletionResponse
	Err      error
}

// Complete returns the canned Response (or Err).
func (f *FakeClient) Complete(_ context.Context, _ llm.CompletionRequest) (*llm.CompletionResponse, error) {
	if f.Err != nil {
		return nil, f.Err
	}
	resp := f.Response
	return &resp, nil
}

// RecordingClient wraps an inner llm.Client and records every (req, resp, err)
// tuple. Tests use it to assert headers, message order, or tool definitions
// passed to a real or scripted client.
type RecordingClient struct {
	Inner llm.Client

	mu    sync.Mutex
	calls []RecordedCall
}

// RecordedCall is a single Complete invocation captured by RecordingClient.
type RecordedCall struct {
	Request  llm.CompletionRequest
	Response *llm.CompletionResponse
	Err      error
}

// Complete forwards to Inner and records the call.
func (r *RecordingClient) Complete(ctx context.Context, req llm.CompletionRequest) (*llm.CompletionResponse, error) {
	resp, err := r.Inner.Complete(ctx, req)
	r.mu.Lock()
	r.calls = append(r.calls, RecordedCall{Request: req, Response: resp, Err: err})
	r.mu.Unlock()
	return resp, err
}

// Calls returns a copy of every recorded call.
func (r *RecordingClient) Calls() []RecordedCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]RecordedCall, len(r.calls))
	copy(out, r.calls)
	return out
}

// ── StructuredClient doubles ──────────────────────────────────────────

// ScriptedStructuredClient implements llm.StructuredClient by returning
// Responses in order. Calls past the end of the script return
// ErrScriptExhausted.
//
// It also implements llm.Client: Complete returns the same scripted payload
// projected onto a CompletionResponse so tests that need either interface
// can use a single double.
//
// Safe for concurrent use; mu protects idx and the calls slices.
type ScriptedStructuredClient struct {
	// Responses drive CompleteStructured. When the same instance is also used
	// as a Client, Complete walks the same slice (projecting each response
	// onto a CompletionResponse).
	Responses []llm.StructuredResponse
	// Err, when non-nil, is returned from every call instead of consuming a
	// scripted response. Use for one-shot error simulation.
	Err error

	mu                sync.Mutex
	idx               int
	structuredCalls   []llm.StructuredRequest
	completionCalls   []llm.CompletionRequest
}

// CompleteStructured returns the next scripted response (or Err).
func (s *ScriptedStructuredClient) CompleteStructured(_ context.Context, req llm.StructuredRequest) (*llm.StructuredResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Err != nil {
		return nil, s.Err
	}
	s.structuredCalls = append(s.structuredCalls, req)
	if s.idx >= len(s.Responses) {
		return nil, ErrScriptExhausted
	}
	resp := s.Responses[s.idx]
	s.idx++
	return &resp, nil
}

// Complete projects the next scripted StructuredResponse onto a CompletionResponse.
// Tool calls cannot round-trip through this path — they are not part of the
// structured shape. Use ScriptedClient when you need to drive tool loops.
func (s *ScriptedStructuredClient) Complete(_ context.Context, req llm.CompletionRequest) (*llm.CompletionResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.Err != nil {
		return nil, s.Err
	}
	s.completionCalls = append(s.completionCalls, req)
	if s.idx >= len(s.Responses) {
		return nil, ErrScriptExhausted
	}
	sr := s.Responses[s.idx]
	s.idx++
	return &llm.CompletionResponse{
		Content:    sr.Content,
		StopReason: sr.StopReason,
		Usage:      sr.Usage,
	}, nil
}

// Calls returns a copy of every CompleteStructured request received so far.
func (s *ScriptedStructuredClient) Calls() []llm.StructuredRequest {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]llm.StructuredRequest, len(s.structuredCalls))
	copy(out, s.structuredCalls)
	return out
}

// FakeStructuredClient implements llm.StructuredClient by always returning
// the same Response (and also satisfies llm.Client).
type FakeStructuredClient struct {
	Response llm.StructuredResponse
	Err      error
}

// CompleteStructured returns the canned Response (or Err).
func (f *FakeStructuredClient) CompleteStructured(_ context.Context, _ llm.StructuredRequest) (*llm.StructuredResponse, error) {
	if f.Err != nil {
		return nil, f.Err
	}
	resp := f.Response
	return &resp, nil
}

// Complete returns the canned Response projected onto a CompletionResponse.
func (f *FakeStructuredClient) Complete(_ context.Context, _ llm.CompletionRequest) (*llm.CompletionResponse, error) {
	if f.Err != nil {
		return nil, f.Err
	}
	return &llm.CompletionResponse{
		Content:    f.Response.Content,
		StopReason: f.Response.StopReason,
		Usage:      f.Response.Usage,
	}, nil
}

// ── Tool double ───────────────────────────────────────────────────────

// FakeTool implements llm.Tool with a canned ToolResult. It records every
// invocation in Calls for test assertions.
type FakeTool struct {
	Name        string
	Description string
	InputSchema json.RawMessage
	Idempotent  bool
	Result      llm.ToolResult
	Err         error

	mu    sync.Mutex
	calls []llm.ToolCall
}

// Definition implements llm.Tool.
func (t *FakeTool) Definition() llm.ToolDefinition {
	desc := t.Description
	if desc == "" {
		desc = "fake tool for tests"
	}
	schema := t.InputSchema
	if len(schema) == 0 {
		schema = json.RawMessage(`{"type":"object"}`)
	}
	return llm.ToolDefinition{
		Name:        t.Name,
		Description: desc,
		InputSchema: schema,
		Idempotent:  t.Idempotent,
	}
}

// Invoke records the call and returns the canned Result (or Err).
func (t *FakeTool) Invoke(_ context.Context, call llm.ToolCall, _ orchestrator.StateView, _ *orchestrator.Turn) (llm.ToolResult, error) {
	t.mu.Lock()
	t.calls = append(t.calls, call)
	t.mu.Unlock()
	if t.Err != nil {
		return llm.ToolResult{}, t.Err
	}
	return t.Result, nil
}

// Calls returns a copy of every tool invocation received.
func (t *FakeTool) Calls() []llm.ToolCall {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]llm.ToolCall, len(t.calls))
	copy(out, t.calls)
	return out
}
