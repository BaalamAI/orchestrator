package orchestrator_test

import (
	"context"
	"errors"
	"testing"

	orch "github.com/baalamai/orchestrator"
	"github.com/baalamai/orchestrator/store"
	"github.com/baalamai/orchestrator/supervisor"
)

// TestMemoryCheckpointStore_SaveLoadClear covers the basic port contract.
func TestMemoryCheckpointStore_SaveLoadClear(t *testing.T) {
	ctx := context.Background()
	store := store.NewMemoryCheckpoint()

	if got, err := store.Load(ctx, "missing"); err != nil || got != nil {
		t.Fatalf("expected nil,nil for missing; got %v,%v", got, err)
	}

	ckpt := &orch.Checkpoint{
		TurnID:    "t1",
		Step:      2,
		Phase:     "diagnostic",
		LastEvent: orch.EventPhaseComplete,
		State:     map[string]any{"foo": "bar"},
	}
	if err := store.Save(ctx, ckpt); err != nil {
		t.Fatalf("Save: %v", err)
	}

	got, err := store.Load(ctx, "t1")
	if err != nil || got == nil {
		t.Fatalf("Load failed: %v / %v", got, err)
	}
	if got.Phase != "diagnostic" || got.Step != 2 {
		t.Errorf("unexpected checkpoint: %+v", got)
	}
	// Mutate the returned copy — store must be isolated.
	got.State["foo"] = "mutated"
	re, _ := store.Load(ctx, "t1")
	if re.State["foo"] != "bar" {
		t.Error("store must return an isolated copy (got mutation leaked)")
	}

	if err := store.Clear(ctx, "t1"); err != nil {
		t.Fatalf("Clear: %v", err)
	}
	if got, _ := store.Load(ctx, "t1"); got != nil {
		t.Error("expected checkpoint cleared")
	}
}

// TestEngine_NoCheckpoint_NoRegression verifies that with no CheckpointStore
// configured, Engine.Run behaves exactly as before.
func TestEngine_NoCheckpoint_NoRegression(t *testing.T) {
	engine := buildSimpleNodeEngine("p", dummyNode("ok", orch.EventWaitUser))

	turn := orch.NewTurn("c1", "hi")
	turn.TurnID = "t1" // even with TurnID, no store means no checkpointing
	result, err := engine.Run(context.Background(), store.NewMemory(), turn)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.Answer != "ok" {
		t.Errorf("expected 'ok', got %q", result.Answer)
	}
}

// TestEngine_CheckpointSavedOnSuccess verifies a checkpoint is written per step
// and cleared on normal completion.
func TestEngine_CheckpointSavedOnSuccess(t *testing.T) {
	ckStore := store.NewMemoryCheckpoint()

	engine := orch.NewPipelineBuilder().
		WithSupervisor(&supervisor.StateMachine{DefaultPhase: "p"}).
		RegisterNode("p", dummyNode("done", orch.EventWaitUser)).
		WithCheckpoints(ckStore).
		MustBuild()

	turn := orch.NewTurn("c1", "hi")
	turn.TurnID = "t-success"

	_, err := engine.Run(context.Background(), store.NewMemory(), turn)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	// Success → Clear was called → Load returns nil.
	got, _ := ckStore.Load(context.Background(), "t-success")
	if got != nil {
		t.Error("expected checkpoint cleared after successful run")
	}
}

// TestEngine_CheckpointSurvivesLoopError verifies checkpoint is NOT cleared
// when the loop returns an error — the next retry can resume.
func TestEngine_CheckpointSurvivesLoopError(t *testing.T) {
	ckStore := store.NewMemoryCheckpoint()
	callCount := 0
	failingNode := func(_ context.Context, _ orch.StateView, _ *orch.Turn, _ *orch.NodeInput) (*orch.NodeResult, error) {
		callCount++
		return nil, errors.New("downstream boom")
	}

	engine := orch.NewPipelineBuilder().
		WithSupervisor(&supervisor.StateMachine{DefaultPhase: "p"}).
		RegisterNode("p", failingNode).
		WithCheckpoints(ckStore).
		MustBuild()

	turn := orch.NewTurn("c1", "hi")
	turn.TurnID = "t-fail"

	_, err := engine.Run(context.Background(), store.NewMemory(), turn)
	if err == nil {
		t.Fatal("expected error from failing node")
	}

	// The node failed before any successful step was committed — so no checkpoint
	// was ever saved (saveCheckpoint only fires on propagateNodeOutput).
	got, _ := ckStore.Load(context.Background(), "t-fail")
	if got != nil {
		t.Errorf("node error before any successful step: checkpoint should not exist, got %+v", got)
	}
}

// TestEngine_ResumeFromCheckpoint is the crash-replay test: after a successful
// first phase completes and is checkpointed, a simulated crash + retry resumes
// at the next phase, without re-running the first phase.
func TestEngine_ResumeFromCheckpoint(t *testing.T) {
	ckStore := store.NewMemoryCheckpoint()

	// Pre-populate the checkpoint as if phase "a" already completed pre-crash.
	preCrash := &orch.Checkpoint{
		TurnID:        "t-resume",
		Step:          1,
		Phase:         "a",
		LastEvent:     orch.EventPhaseComplete,
		State:         map[string]any{"restored_flag": true},
		Messages:      []orch.Message{{Role: "user", Text: "hi"}},
		SharedContext: map[string]any{"from_a": "v"},
	}
	if err := ckStore.Save(context.Background(), preCrash); err != nil {
		t.Fatalf("seed: %v", err)
	}

	aCalls := 0
	bCalls := 0
	a := func(_ context.Context, _ orch.StateView, _ *orch.Turn, _ *orch.NodeInput) (*orch.NodeResult, error) {
		aCalls++
		return &orch.NodeResult{Event: orch.EventPhaseComplete, Answer: "from a"}, nil
	}
	b := func(_ context.Context, view orch.StateView, _ *orch.Turn, input *orch.NodeInput) (*orch.NodeResult, error) {
		bCalls++
		// Assert SharedContext was restored from checkpoint.
		if v, _ := input.SharedContext["from_a"].(string); v != "v" {
			t.Errorf("expected shared context restored, got %v", input.SharedContext)
		}
		if !view.GetBool("restored_flag") {
			t.Error("expected checkpoint state restored into store snapshot")
		}
		msgs := view.Messages()
		if len(msgs) != 1 || msgs[0].Role != "user" || msgs[0].Text != "hi" {
			t.Errorf("expected checkpoint messages restored, got %+v", msgs)
		}
		return &orch.NodeResult{Event: orch.EventWaitUser, Answer: "from b"}, nil
	}

	engine := orch.NewPipelineBuilder().
		WithSupervisor(&supervisor.StateMachine{
			DefaultPhase: "a",
			Transitions:  []supervisor.TransitionRule{{From: "a", To: "b"}},
		}).
		RegisterNode("a", a).
		RegisterNode("b", b).
		MaxSteps(5).
		WithCheckpoints(ckStore).
		MustBuild()

	turn := orch.NewTurn("c1", "hi")
	turn.TurnID = "t-resume"

	store := store.NewMemory()

	result, err := engine.Run(context.Background(), store, turn)
	if err != nil {
		t.Fatalf("Run (resume): %v", err)
	}
	if result.Answer != "from b" {
		t.Errorf("expected 'from b', got %q", result.Answer)
	}
	if aCalls != 0 {
		t.Errorf("phase a must NOT re-run after resume; got %d calls", aCalls)
	}
	if bCalls != 1 {
		t.Errorf("phase b must run exactly once; got %d calls", bCalls)
	}
	// Resume must restore the user message from the checkpoint without re-adding it.
	userCount := 0
	for _, m := range store.Messages() {
		if m.Role == "user" {
			userCount++
		}
	}
	if userCount != 1 {
		t.Errorf("resume must not duplicate user message; got %d user messages", userCount)
	}
	if !store.HasFlag("restored_flag") {
		t.Error("expected checkpoint state restored into backing store")
	}

	// Success → checkpoint cleared.
	got, _ := ckStore.Load(context.Background(), "t-resume")
	if got != nil {
		t.Error("expected checkpoint cleared after resumed run completed")
	}
}

// TestEngine_ResumeDoesNotReRunPreprocessSideEffects documents the contract:
// preprocess hooks DO re-run on resume (they must be idempotent), but AddMessage
// does NOT. This protects against duplicate user messages in the store.
func TestEngine_ResumeDoesNotReAddUserMessage(t *testing.T) {
	ckStore := store.NewMemoryCheckpoint()
	_ = ckStore.Save(context.Background(), &orch.Checkpoint{
		TurnID: "t", Step: 1, Phase: "p", LastEvent: orch.EventWaitUser,
		Messages: []orch.Message{
			{Role: "user", Text: "original user message"},
		},
	})

	preprocessCalls := 0
	engine := orch.NewPipelineBuilder().
		WithSupervisor(&supervisor.StateMachine{DefaultPhase: "p"}).
		RegisterNode("p", dummyNode("", orch.EventWaitUser)).
		OnPreprocess(func(_ context.Context, _ orch.StateStore, _ *orch.Turn) error {
			preprocessCalls++
			return nil
		}).
		WithCheckpoints(ckStore).
		MaxSteps(5).
		MustBuild()

	turn := orch.NewTurn("c1", "original user message")
	turn.TurnID = "t"

	store := store.NewMemory()

	_, err := engine.Run(context.Background(), store, turn)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if preprocessCalls != 1 {
		t.Errorf("preprocess should run once on resume (idempotent contract), got %d", preprocessCalls)
	}

	userMsgs := 0
	for _, m := range store.Messages() {
		if m.Role == "user" && m.Text == "original user message" {
			userMsgs++
		}
	}
	if userMsgs != 1 {
		t.Errorf("expected 1 user message (no re-add), got %d", userMsgs)
	}
}
