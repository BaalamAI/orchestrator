// Package store provides in-memory adapter implementations of the orchestrator
// StateStore and CheckpointStore ports. Production adapters (Redis, Mongo)
// live outside this module in the consumer services.
package store

import (
	"sync"

	"github.com/baalamai/orchestrator"
)

// Memory is an in-memory implementation of orchestrator.StateStore.
// Useful for tests and pipelines that don't need persistence.
type Memory struct {
	mu       sync.Mutex
	state    map[string]any
	messages []orchestrator.Message
	memory   map[string]any
}

// NewMemory creates a new in-memory StateStore.
func NewMemory() *Memory {
	return &Memory{
		state: make(map[string]any),
	}
}

// State returns a shallow copy of the full state map.
func (m *Memory) State() map[string]any {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make(map[string]any, len(m.state))
	for k, v := range m.state {
		cp[k] = v
	}
	return cp
}

// SetState sets a single key-value pair in the state map.
func (m *Memory) SetState(key string, value any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state[key] = value
}

// DeleteState removes a key from the state map.
func (m *Memory) DeleteState(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.state, key)
}

// HasFlag returns true if key exists and its value is bool(true).
func (m *Memory) HasFlag(key string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	v, ok := m.state[key]
	if !ok {
		return false
	}
	b, ok := v.(bool)
	return ok && b
}

// Messages returns a copy of the conversation history.
func (m *Memory) Messages() []orchestrator.Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]orchestrator.Message, len(m.messages))
	copy(cp, m.messages)
	return cp
}

// AddMessage appends a message to the in-memory conversation history.
func (m *Memory) AddMessage(role, text string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.messages = append(m.messages, orchestrator.Message{Role: role, Text: text})
	return nil
}

// Restore replaces state and messages with checkpointed copies.
func (m *Memory) Restore(state map[string]any, messages []orchestrator.Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if state == nil {
		m.state = make(map[string]any)
	} else {
		m.state = make(map[string]any, len(state))
		for k, v := range state {
			m.state[k] = v
		}
	}

	if messages == nil {
		m.messages = nil
	} else {
		m.messages = make([]orchestrator.Message, len(messages))
		copy(m.messages, messages)
	}

	return nil
}

// Save is a no-op for the in-memory store.
func (m *Memory) Save() error {
	return nil
}

// SetMemory sets the memory map. Satisfies orchestrator.MemoryProvider.
func (m *Memory) SetMemory(mem map[string]any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.memory = mem
}

// Memory returns a copy of the memory map. Satisfies orchestrator.MemoryProvider.
func (m *Memory) Memory() map[string]any {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.memory == nil {
		return nil
	}
	cp := make(map[string]any, len(m.memory))
	for k, v := range m.memory {
		cp[k] = v
	}
	return cp
}
