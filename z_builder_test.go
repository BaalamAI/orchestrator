package orchestrator

import (
	"context"
	"testing"
)

func TestBuilder_NoSupervisor_Error(t *testing.T) {
	_, err := NewPipelineBuilder().
		RegisterNode("a", dummyNode("ok", EventWaitUser)).
		Build()
	if err == nil {
		t.Error("expected error when supervisor is missing")
	}
}

func TestBuilder_NoAgents_Error(t *testing.T) {
	_, err := NewPipelineBuilder().
		WithSupervisor(&StateMachineSupervisor{DefaultPhase: "a"}).
		Build()
	if err == nil {
		t.Error("expected error when no agents registered")
	}
}

func TestBuilder_ValidBuild(t *testing.T) {
	engine, err := NewPipelineBuilder().
		WithSupervisor(&StateMachineSupervisor{DefaultPhase: "a"}).
		RegisterNode("a", dummyNode("ok", EventWaitUser)).
		Build()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if engine == nil {
		t.Error("expected non-nil engine")
	}
}

func TestBuilder_MustBuild_Panics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Error("expected panic from MustBuild with invalid config")
		}
	}()
	NewPipelineBuilder().MustBuild()
}

func TestBuilder_StateMachine_MissingTransitionTarget(t *testing.T) {
	_, err := NewPipelineBuilder().
		WithSupervisor(&StateMachineSupervisor{
			DefaultPhase: "a",
			Transitions:  []TransitionRule{{From: "a", To: "b"}},
		}).
		RegisterNode("a", dummyNode("ok", EventWaitUser)).
		Build()
	if err == nil {
		t.Error("expected error for missing transition target 'b'")
	}
}

func TestBuilder_StateMachine_MissingConditionalTarget(t *testing.T) {
	_, err := NewPipelineBuilder().
		WithSupervisor(&StateMachineSupervisor{
			DefaultPhase: "a",
			ConditionalTransitions: []ConditionalTransition{
				{From: "a", To: "missing", Condition: func(_ StateStore) bool { return true }},
			},
		}).
		RegisterNode("a", dummyNode("ok", EventWaitUser)).
		Build()
	if err == nil {
		t.Error("expected error for missing conditional target")
	}
}

func TestBuilder_LinearSupervisor_MissingPhase(t *testing.T) {
	_, err := NewPipelineBuilder().
		WithSupervisor(NewLinearSupervisor("a", "b")).
		RegisterNode("a", dummyNode("ok", EventWaitUser)).
		Build()
	if err == nil {
		t.Error("expected error for missing linear phase 'b'")
	}
}

func TestBuilder_LinearSupervisor_ValidBuild(t *testing.T) {
	engine, err := NewPipelineBuilder().
		WithSupervisor(NewLinearSupervisor("a", "b")).
		RegisterNode("a", dummyNode("a", EventPhaseComplete)).
		RegisterNode("b", dummyNode("b", EventWaitUser)).
		Build()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	result, err := engine.Run(context.Background(), NewMemoryStore(), NewTurn("c1", "hi"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Answer != "b" {
		t.Errorf("expected answer b, got %s", result.Answer)
	}
}

func TestBuilder_MaxSteps(t *testing.T) {
	engine := NewPipelineBuilder().
		MaxSteps(5).
		WithSupervisor(&StateMachineSupervisor{DefaultPhase: "a"}).
		RegisterNode("a", dummyNode("ok", EventWaitUser)).
		MustBuild()

	if engine.maxSteps != 5 {
		t.Errorf("expected maxSteps 5, got %d", engine.maxSteps)
	}
}

func TestBuilder_Repeatable(t *testing.T) {
	engine := NewPipelineBuilder().
		WithSupervisor(&StateMachineSupervisor{DefaultPhase: "a"}).
		RegisterNode("a", dummyNode("ok", EventWaitUser)).
		RegisterNode("b", dummyNode("ok", EventWaitUser)).
		Repeatable("a", "b").
		MustBuild()

	if !engine.repeatablePhases["a"] || !engine.repeatablePhases["b"] {
		t.Error("expected phases a and b to be repeatable")
	}
}

func TestBuilder_Hooks(t *testing.T) {
	engine := NewPipelineBuilder().
		WithSupervisor(&StateMachineSupervisor{DefaultPhase: "a"}).
		RegisterNode("a", dummyNode("ok", EventWaitUser)).
		OnFatalPreprocess(func(_ context.Context, _ StateStore, _ *Turn) error { return nil }).
		OnPreprocess(func(_ context.Context, _ StateStore, _ *Turn) error { return nil }).
		OnPostprocess(func(_ context.Context, _ StateStore, _ *Turn, _ *PipelineResult) error { return nil }).
		MustBuild()

	if len(engine.fatalPreprocessHooks) != 1 {
		t.Errorf("expected 1 fatal preprocess hook, got %d", len(engine.fatalPreprocessHooks))
	}
	if len(engine.preprocessHooks) != 1 {
		t.Errorf("expected 1 preprocess hook, got %d", len(engine.preprocessHooks))
	}
	if len(engine.postprocessHooks) != 1 {
		t.Errorf("expected 1 postprocess hook, got %d", len(engine.postprocessHooks))
	}
}

func TestBuilder_WithLogger(t *testing.T) {
	logger := &testLogger{}
	engine := NewPipelineBuilder().
		WithSupervisor(&StateMachineSupervisor{DefaultPhase: "a"}).
		RegisterNode("a", dummyNode("ok", EventWaitUser)).
		WithLogger(logger).
		MustBuild()

	if engine.logger == nil {
		t.Error("expected logger to be set")
	}

	// Run to verify logger is called
	engine.Run(context.Background(), NewMemoryStore(), NewTurn("c1", "hi"))
	if !logger.called {
		t.Error("expected logger to be called during Run")
	}
}

type testLogger struct{ called bool }

func (l *testLogger) Info(_ context.Context, _ string, _ ...any)  { l.called = true }
func (l *testLogger) Error(_ context.Context, _ string, _ ...any) { l.called = true }
func (l *testLogger) Warn(_ context.Context, _ string, _ ...any)  { l.called = true }

func TestBuilder_DuplicateRegisterOverwrites(t *testing.T) {
	engine := NewPipelineBuilder().
		WithSupervisor(&StateMachineSupervisor{DefaultPhase: "a"}).
		RegisterNode("a", dummyNode("first", EventWaitUser)).
		RegisterNode("a", dummyNode("second", EventWaitUser)).
		MustBuild()

	result, _ := engine.Run(context.Background(), NewMemoryStore(), NewTurn("c1", "hi"))
	if result.Answer != "second" {
		t.Errorf("expected second node to win, got %s", result.Answer)
	}
}

func TestBuilder_DefaultMaxSteps(t *testing.T) {
	engine := NewPipelineBuilder().
		WithSupervisor(&StateMachineSupervisor{DefaultPhase: "a"}).
		RegisterNode("a", dummyNode("ok", EventWaitUser)).
		MustBuild()

	if engine.maxSteps != 3 {
		t.Errorf("expected default maxSteps 3, got %d", engine.maxSteps)
	}
}

// ── BuildFromConfig tests ───────────────────────────────────────────

func TestBuildFromConfig_NoSupervisor_Error(t *testing.T) {
	_, err := BuildFromConfig(PipelineConfig{
		Nodes: []NodeConfig{{Phase: "a", Fn: dummyNode("ok", EventWaitUser)}},
	})
	if err == nil {
		t.Error("expected error when supervisor is missing")
	}
}

func TestBuildFromConfig_NoNodes_Error(t *testing.T) {
	_, err := BuildFromConfig(PipelineConfig{
		Supervisor: &StateMachineSupervisor{DefaultPhase: "a"},
	})
	if err == nil {
		t.Error("expected error when no nodes registered")
	}
}

func TestBuildFromConfig_ValidConfig(t *testing.T) {
	engine, err := BuildFromConfig(PipelineConfig{
		MaxSteps:   5,
		Supervisor: &StateMachineSupervisor{DefaultPhase: "a"},
		Nodes: []NodeConfig{
			{Phase: "a", Fn: dummyNode("hello", EventWaitUser), Repeatable: true},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if engine.maxSteps != 5 {
		t.Errorf("expected maxSteps 5, got %d", engine.maxSteps)
	}
	if !engine.repeatablePhases["a"] {
		t.Error("expected phase 'a' to be repeatable")
	}
}

func TestBuildFromConfig_Hooks(t *testing.T) {
	called := false
	hook := func(_ context.Context, _ StateStore, _ *Turn, _ *PipelineResult) error {
		called = true
		return nil
	}

	engine, err := BuildFromConfig(PipelineConfig{
		Supervisor: &StateMachineSupervisor{DefaultPhase: "a"},
		Nodes:      []NodeConfig{{Phase: "a", Fn: dummyNode("ok", EventWaitUser)}},
		Hooks:      HookConfig{PostprocessHooks: []PostprocessHook{hook}},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	engine.Run(context.Background(), NewMemoryStore(), NewTurn("c1", "hi"))
	if !called {
		t.Error("expected postprocess hook to be called")
	}
}

func TestBuildFromConfig_ConcurrentNode(t *testing.T) {
	engine, err := BuildFromConfig(PipelineConfig{
		Supervisor: &StateMachineSupervisor{DefaultPhase: "a"},
		Nodes: []NodeConfig{
			{Phase: "a", Fn: dummyNode("ok", EventWaitUser), Concurrent: true},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	entry := engine.nodeRegistry["a"]
	if !entry.concurrencySafe {
		t.Error("expected node 'a' to be marked concurrent-safe")
	}
}

func TestBuildFromConfig_EmptyHooks_NoPanic(t *testing.T) {
	_, err := BuildFromConfig(PipelineConfig{
		Supervisor: &StateMachineSupervisor{DefaultPhase: "a"},
		Nodes:      []NodeConfig{{Phase: "a", Fn: dummyNode("ok", EventWaitUser)}},
		Hooks:      HookConfig{}, // all empty
	})
	if err != nil {
		t.Fatalf("unexpected error with empty hooks: %v", err)
	}
}
