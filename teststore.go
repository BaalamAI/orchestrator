package orchestrator

import "sync"

// MemoryStore is an in-memory implementation of StateStore.
// Useful for tests and pipelines that don't need persistence.
type MemoryStore struct {
	mu       sync.Mutex
	state    map[string]any
	messages []Message
	memory   map[string]any
}

// NewMemoryStore creates a new in-memory StateStore.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		state: make(map[string]any),
	}
}

// State returns a shallow copy of the full state map.
func (m *MemoryStore) State() map[string]any {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make(map[string]any, len(m.state))
	for k, v := range m.state {
		cp[k] = v
	}
	return cp
}

// SetState sets a single key-value pair in the state map.
func (m *MemoryStore) SetState(key string, value any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state[key] = value
}

// HasFlag returns true if key exists in the state and its value is bool(true).
func (m *MemoryStore) HasFlag(key string) bool {
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
func (m *MemoryStore) Messages() []Message {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]Message, len(m.messages))
	copy(cp, m.messages)
	return cp
}

// AddMessage appends a message to the in-memory conversation history.
func (m *MemoryStore) AddMessage(role, text string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.messages = append(m.messages, Message{Role: role, Text: text})
	return nil
}

// Save is a no-op for the in-memory store.
func (m *MemoryStore) Save() error {
	return nil
}

// SetMemory sets the memory map for testing. Satisfies MemoryProvider.
func (m *MemoryStore) SetMemory(mem map[string]any) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.memory = mem
}

// Memory returns a copy of the memory map. Satisfies MemoryProvider.
func (m *MemoryStore) Memory() map[string]any {
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
