package orchestrator

import "testing"

func TestNewTurn(t *testing.T) {
	turn := NewTurn("conv-1", "hola")
	if turn.ConversationID != "conv-1" {
		t.Errorf("expected ConversationID conv-1, got %s", turn.ConversationID)
	}
	if turn.Text != "hola" {
		t.Errorf("expected Text hola, got %s", turn.Text)
	}
	if turn.UserID != "" || turn.OrgID != "" || turn.Channel != "" {
		t.Error("expected empty optional fields")
	}
}

func TestUsageAdd(t *testing.T) {
	u := &Usage{PromptTokens: 10, CompletionTokens: 5, TotalTokens: 15}

	// Add nil — should be no-op
	u.Add(nil)
	if u.PromptTokens != 10 {
		t.Error("Add(nil) should be no-op")
	}

	other := &Usage{
		PromptTokens:     20,
		CompletionTokens: 10,
		TotalTokens:      30,
		Breakdown: []ModelUsage{
			{Agent: "router", Model: "gemini", PromptTokens: 20, CompletionTokens: 10, TotalTokens: 30},
		},
	}
	u.Add(other)

	if u.PromptTokens != 30 {
		t.Errorf("expected PromptTokens 30, got %d", u.PromptTokens)
	}
	if u.CompletionTokens != 15 {
		t.Errorf("expected CompletionTokens 15, got %d", u.CompletionTokens)
	}
	if u.TotalTokens != 45 {
		t.Errorf("expected TotalTokens 45, got %d", u.TotalTokens)
	}
	if len(u.Breakdown) != 1 {
		t.Errorf("expected 1 breakdown entry, got %d", len(u.Breakdown))
	}
}

func TestFormatMessages(t *testing.T) {
	msgs := []Message{
		{Role: "user", Text: "hola"},
		{Role: "model", Text: "bienvenido"},
		{Role: "user", Text: "precio"},
		{Role: "model", Text: "$100"},
	}

	// Last 2
	got := FormatMessages(msgs, 2)
	want := "[user]: precio\n[model]: $100\n"
	if got != want {
		t.Errorf("FormatMessages(4, 2) = %q, want %q", got, want)
	}

	// All (0 = no limit)
	got = FormatMessages(msgs, 0)
	if got == "" {
		t.Error("FormatMessages(4, 0) should return all messages")
	}

	// Empty
	got = FormatMessages(nil, 5)
	if got != "" {
		t.Errorf("FormatMessages(nil, 5) = %q, want empty", got)
	}
}

func TestPipelineResult_SetMeta(t *testing.T) {
	r := &PipelineResult{}

	r.SetMeta("key1", "value1")
	if r.Metadata == nil {
		t.Fatal("expected Metadata to be initialized")
	}
	if r.Metadata["key1"] != "value1" {
		t.Errorf("expected value1, got %v", r.Metadata["key1"])
	}

	r.SetMeta("key2", 42)
	if r.Metadata["key2"] != 42 {
		t.Errorf("expected 42, got %v", r.Metadata["key2"])
	}
}

func TestPipelineResult_SetMeta_Overwrite(t *testing.T) {
	r := &PipelineResult{}

	r.SetMeta("key", "old")
	r.SetMeta("key", "new")
	if r.Metadata["key"] != "new" {
		t.Errorf("expected 'new', got %v", r.Metadata["key"])
	}
}

func TestUsageAddMultipleBreakdowns(t *testing.T) {
	u := &Usage{
		Breakdown: []ModelUsage{{Agent: "a1"}},
	}
	u.Add(&Usage{
		Breakdown: []ModelUsage{{Agent: "a2"}, {Agent: "a3"}},
	})
	if len(u.Breakdown) != 3 {
		t.Errorf("expected 3 breakdown entries, got %d", len(u.Breakdown))
	}
}
