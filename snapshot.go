package orchestrator

// stateSnapshot implements StateView as a frozen, read-only copy of a StateStore.
type stateSnapshot struct {
	state    map[string]any
	messages []Message
	memory   map[string]any
}

// MemoryProvider is an optional interface that StateStore implementations can
// satisfy to expose cross-session persistent memory.
type MemoryProvider interface {
	Memory() map[string]any
}

// NewSnapshot creates a read-only StateView from the current StateStore.
// It takes shallow copies so the snapshot is isolated from future mutations.
// If the store implements MemoryProvider, memory is included in the snapshot.
func NewSnapshot(store StateStore) StateView {
	var memory map[string]any
	if mp, ok := store.(MemoryProvider); ok {
		memory = mp.Memory()
	}
	return &stateSnapshot{
		state:    store.State(),    // State() already returns a copy (MemoryStore & RedisStateAdapter)
		messages: store.Messages(), // Messages() already returns a copy
		memory:   memory,
	}
}

func (s *stateSnapshot) Get(key string) (any, bool) {
	v, ok := s.state[key]
	return v, ok
}

func (s *stateSnapshot) GetString(key string) string {
	v, ok := s.state[key]
	if !ok {
		return ""
	}
	str, _ := v.(string)
	return str
}

func (s *stateSnapshot) GetBool(key string) bool {
	v, ok := s.state[key]
	if !ok {
		return false
	}
	b, _ := v.(bool)
	return b
}

func (s *stateSnapshot) HasFlag(key string) bool {
	return s.GetBool(key)
}

func (s *stateSnapshot) State() map[string]any {
	cp := make(map[string]any, len(s.state))
	for k, v := range s.state {
		cp[k] = v
	}
	return cp
}

func (s *stateSnapshot) Messages() []Message {
	cp := make([]Message, len(s.messages))
	copy(cp, s.messages)
	return cp
}

func (s *stateSnapshot) Memory() map[string]any {
	if s.memory == nil {
		return nil
	}
	cp := make(map[string]any, len(s.memory))
	for k, v := range s.memory {
		cp[k] = v
	}
	return cp
}
