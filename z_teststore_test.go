package orchestrator

import (
	"sync"
	"testing"
)

func TestMemoryStoreInitialState(t *testing.T) {
	ms := NewMemoryStore()
	state := ms.State()
	if len(state) != 0 {
		t.Errorf("expected empty state, got %d entries", len(state))
	}
	msgs := ms.Messages()
	if len(msgs) != 0 {
		t.Errorf("expected no messages, got %d", len(msgs))
	}
}

func TestMemoryStoreSetState(t *testing.T) {
	ms := NewMemoryStore()
	ms.SetState("name", "test")
	ms.SetState("count", 42)

	state := ms.State()
	if state["name"] != "test" {
		t.Errorf("expected name=test, got %v", state["name"])
	}
	if state["count"] != 42 {
		t.Errorf("expected count=42, got %v", state["count"])
	}
}

func TestMemoryStoreStateReturnsCopy(t *testing.T) {
	ms := NewMemoryStore()
	ms.SetState("key", "original")

	state := ms.State()
	state["key"] = "modified"

	state2 := ms.State()
	if state2["key"] != "original" {
		t.Error("State should return a copy, not a reference")
	}
}

func TestMemoryStoreHasFlag(t *testing.T) {
	ms := NewMemoryStore()

	// Missing key
	if ms.HasFlag("missing") {
		t.Error("expected false for missing key")
	}

	// Non-bool value
	ms.SetState("string_val", "yes")
	if ms.HasFlag("string_val") {
		t.Error("expected false for non-bool value")
	}

	// Bool false
	ms.SetState("disabled", false)
	if ms.HasFlag("disabled") {
		t.Error("expected false for false value")
	}

	// Bool true
	ms.SetState("enabled", true)
	if !ms.HasFlag("enabled") {
		t.Error("expected true for true value")
	}
}

func TestMemoryStoreMessages(t *testing.T) {
	ms := NewMemoryStore()

	if err := ms.AddMessage("user", "hola"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := ms.AddMessage("model", "respuesta"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	msgs := ms.Messages()
	if len(msgs) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(msgs))
	}
	if msgs[0].Role != "user" || msgs[0].Text != "hola" {
		t.Errorf("unexpected first message: %+v", msgs[0])
	}
	if msgs[1].Role != "model" || msgs[1].Text != "respuesta" {
		t.Errorf("unexpected second message: %+v", msgs[1])
	}
}

func TestMemoryStoreMessagesReturnsCopy(t *testing.T) {
	ms := NewMemoryStore()
	ms.AddMessage("user", "original")

	msgs := ms.Messages()
	msgs[0].Text = "modified"

	msgs2 := ms.Messages()
	if msgs2[0].Text != "original" {
		t.Error("Messages should return a copy")
	}
}

func TestMemoryStoreSave(t *testing.T) {
	ms := NewMemoryStore()
	if err := ms.Save(); err != nil {
		t.Errorf("Save should be no-op, got error: %v", err)
	}
}

func TestMemoryStoreConcurrency(t *testing.T) {
	ms := NewMemoryStore()
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(3)
		go func(i int) {
			defer wg.Done()
			ms.SetState("key", i)
		}(i)
		go func() {
			defer wg.Done()
			ms.State()
		}()
		go func(i int) {
			defer wg.Done()
			ms.AddMessage("user", "msg")
		}(i)
	}
	wg.Wait()

	// Just verify no panics/races occurred and messages were added
	msgs := ms.Messages()
	if len(msgs) != 100 {
		t.Errorf("expected 100 messages, got %d", len(msgs))
	}
}
