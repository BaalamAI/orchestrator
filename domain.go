package orchestrator

import (
	"context"
	"fmt"
	"strings"
)

// ── Domain Types ─────────────────────────────────────────────────────
// Port interfaces (StateStore, Supervisor, ParallelSupervisor, IntentRouter,
// ErrorClassifier, CostCalculator, SupervisorValidator) live in ports.go.

// Metadata keys for transient data passed between agents and hooks.
const (
	MetaRAGContext  = "_rag_context"  // RAG context string for quality gate evaluation
	MetaPriorAnswer = "prior_answer"  // previous agent answer for cascade context
	MetaFinalPhase     = "final_phase"      // last phase executed (set after loop)
	MetaFinalEvent     = "final_event"      // last event type (set after loop)
	MetaTotalSteps     = "total_steps"      // number of steps executed (set after loop)
	MetaTotalCostUSD   = "total_cost_usd"   // accumulated cost in USD (set after loop)
	MetaBudgetExceeded  = "budget_exceeded"   // true if pipeline stopped due to budget (set after loop)
	MetaErrorThreshold  = "error_threshold"   // true if pipeline stopped due to error threshold (set after loop)
	MetaErrorLedger     = "error_ledger"      // map[ErrorCategory]int with accumulated error counts (set after loop)
)

// EventType controls the flow of the execution loop.
type EventType string

const (
	EventStepSuccess      EventType = "step_success"      // Agent finished a step, pipeline continues
	EventWaitUser         EventType = "wait_user"         // Agent needs user input, pipeline stops
	EventPhaseComplete    EventType = "phase_complete"    // Agent finished its phase, pipeline cascades
	EventBudgetExceeded   EventType = "budget_exceeded"   // Token or cost budget exceeded, pipeline stops
	EventErrorThreshold   EventType = "error_threshold"   // Accumulated transient errors exceeded threshold, pipeline stops
)

// Phase represents a named stage in the pipeline.
type Phase = string

// Turn is a channel-agnostic representation of a user message entering the pipeline.
type Turn struct {
	// TurnID uniquely identifies this turn for idempotent retries and checkpoint resume.
	// Callers should set a stable ID (e.g. an InteractionID or UUID) before calling Run.
	// When empty, checkpointing is disabled for this turn even if a CheckpointStore is configured.
	TurnID string
	// ConversationID uniquely identifies the conversation this message belongs to.
	ConversationID string
	// UserID identifies the end-user who sent the message.
	UserID string
	// OrgID is the organization's internal identifier (MongoDB _id).
	OrgID string
	// OrgName is the organization's human-readable name, used for config lookups.
	OrgName string
	// Channel is the origin channel (e.g. "whatsapp", "web", "api").
	Channel string
	// Text is the raw user message content.
	Text string
	// MessageType distinguishes text, image, audio, etc.
	MessageType string
	// Metadata carries channel-specific or request-specific key-value pairs.
	Metadata map[string]any
}

// Message represents a single message in the conversation history.
type Message struct {
	Role string
	Text string
}

// FormatMessages returns the last maxMessages messages formatted as "[role]: text" lines.
// If maxMessages <= 0, all messages are included.
func FormatMessages(msgs []Message, maxMessages int) string {
	start := 0
	if maxMessages > 0 && len(msgs) > maxMessages {
		start = len(msgs) - maxMessages
	}

	var sb strings.Builder
	for _, m := range msgs[start:] {
		fmt.Fprintf(&sb, "[%s]: %s\n", m.Role, m.Text)
	}
	return sb.String()
}

// Usage holds token usage information for analytics.
type Usage struct {
	PromptTokens     int32        `json:"prompt_tokens"`
	CompletionTokens int32        `json:"completion_tokens"`
	TotalTokens      int32        `json:"total_tokens"`
	Breakdown        []ModelUsage `json:"breakdown,omitempty"`
}

// Add accumulates usage from another source.
func (u *Usage) Add(other *Usage) {
	if other == nil {
		return
	}
	u.PromptTokens += other.PromptTokens
	u.CompletionTokens += other.CompletionTokens
	u.TotalTokens += other.TotalTokens
	u.Breakdown = append(u.Breakdown, other.Breakdown...)
}

// ModelUsage holds token usage for a specific model/provider.
type ModelUsage struct {
	Agent            string           `json:"agent"`
	Model            string           `json:"model"`
	Provider         string           `json:"provider"`
	PromptTokens     int32            `json:"prompt_tokens"`
	CompletionTokens int32            `json:"completion_tokens"`
	TotalTokens      int32            `json:"total_tokens"`
	ContextDetails   map[string]int32 `json:"context_details,omitempty"`
}

// PipelineResult encapsulates the final result of the pipeline execution.
type PipelineResult struct {
	// Answer is the final text response to send back to the user.
	Answer string
	// Usage holds accumulated token usage across all agents and supervisors.
	Usage *Usage
	// Metadata carries transient data from agents, available to postprocess hooks.
	Metadata map[string]any
}

// SetMeta sets a key in the Metadata map, initializing it if nil.
func (r *PipelineResult) SetMeta(key string, val any) {
	if r.Metadata == nil {
		r.Metadata = make(map[string]any)
	}
	r.Metadata[key] = val
}

// --- Constructors ---

// NewTurn creates a Turn with the minimum required fields.
func NewTurn(conversationID, text string) *Turn {
	return &Turn{
		ConversationID: conversationID,
		Text:           text,
	}
}

// ── Pure-Function Agent Types ────────────────────────────────────────
// These types support the LangGraph-style pattern where agents are pure
// functions: they receive a read-only state snapshot and return only deltas.

// NodeProvider defines an interface for components that provide data to an agent
// before it executes (e.g. RAG context, field extraction).
type NodeProvider interface {
	Provide(ctx context.Context, view StateView, turn *Turn) (*ProviderResult, error)
}

// ProviderResult holds the data returned by a NodeProvider.
type ProviderResult struct {
	// InputKey is the key under which Value is stored in NodeInput.Extra (or a specialized field).
	InputKey string
	// Value is the data to inject into the agent's input (e.g. RAG context, captured fields).
	Value any
	// Delta contains optional state updates to apply before the agent runs.
	Delta *StateDelta
	// Usage holds optional token usage consumed by this provider.
	Usage *Usage
	// Metadata carries optional transient key-value pairs forwarded to hooks.
	Metadata map[string]any
}

// StateView is a read-only snapshot of the current state.
// Agents receive this instead of StateStore — they cannot mutate state.
type StateView interface {
	// Get returns a value and whether it exists.
	Get(key string) (any, bool)
	// GetString returns the string at key, or "".
	GetString(key string) string
	// GetBool returns the bool at key, or false.
	GetBool(key string) bool
	// HasFlag returns true if key exists and is bool(true).
	HasFlag(key string) bool
	// State returns a shallow copy of the full state map.
	State() map[string]any
	// Messages returns a copy of the conversation history.
	Messages() []Message
	// Memory returns a copy of the cross-session persistent memory map.
	// Returns nil if no memory is available.
	Memory() map[string]any
}

// StateDelta represents the mutations an agent wants to apply.
// The engine is the only component that applies deltas to the store.
type StateDelta struct {
	// Updates contains keys to set or overwrite in the state.
	Updates map[string]any
	// Deletes lists keys to remove from the state.
	Deletes []string
}

// Merge combines another delta into this one (other wins on conflict).
func (d *StateDelta) Merge(other *StateDelta) {
	if other == nil {
		return
	}
	if d.Updates == nil && len(other.Updates) > 0 {
		d.Updates = make(map[string]any, len(other.Updates))
	}
	for k, v := range other.Updates {
		d.Updates[k] = v
	}
	d.Deletes = append(d.Deletes, other.Deletes...)
}

// NodeFunc is the pure-function agent signature.
// It receives a read-only state view and pre-computed input from middleware,
// and returns only a result with deltas — no direct state mutation.
type NodeFunc func(ctx context.Context, view StateView, turn *Turn, input *NodeInput) (*NodeResult, error)

// NodeInput carries context pre-computed by middleware (capture, RAG, etc.).
type NodeInput struct {
	// CapturedFields holds structured fields extracted by capture middleware (e.g. name, phone).
	CapturedFields map[string]any
	// RAGContext is the knowledge context string injected by RAG middleware.
	RAGContext string
	// Extra is an extensible key-value bag for provider data that doesn't fit specialized fields.
	Extra map[string]any
	// SharedContext carries context accumulated from prior phases (e.g. discovered products).
	SharedContext map[string]any
}

// NodeResult is the return type of a pure-function agent.
type NodeResult struct {
	// Event controls the flow: EventWaitUser stops, EventPhaseComplete cascades, EventStepSuccess continues.
	Event EventType
	// Answer is the agent's text response for this step.
	Answer string
	// Delta contains state mutations the engine will apply after this step.
	Delta StateDelta
	// Usage holds token usage consumed by this agent execution.
	Usage *Usage
	// Metadata carries transient key-value pairs forwarded to postprocess hooks.
	Metadata map[string]any
	// SharedContext carries optional context for subsequent phases, accumulated in NodeInput.SharedContext.
	SharedContext map[string]any
}

// AgentMiddleware wraps a NodeFunc, enabling pre/post behavior (capture, logging, etc.).
type AgentMiddleware func(next NodeFunc) NodeFunc

