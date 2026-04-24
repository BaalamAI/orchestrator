package orchestrator_test

import (
	"context"
	"testing"

	orch "github.com/baalamai/orchestrator"
	"github.com/baalamai/orchestrator/store"
	"github.com/baalamai/orchestrator/supervisor"
)

func TestBuilder_NoSupervisor_Error(t *testing.T) {
	_, err := orch.NewPipelineBuilder().
		RegisterNode("a", dummyNode("ok", orch.EventWaitUser)).
		Build()
	if err == nil {
		t.Error("expected error when supervisor is missing")
	}
}

func TestBuilder_NoAgents_Error(t *testing.T) {
	_, err := orch.NewPipelineBuilder().
		WithSupervisor(&supervisor.StateMachine{DefaultPhase: "a"}).
		Build()
	if err == nil {
		t.Error("expected error when no agents registered")
	}
}

func TestBuilder_ValidBuild(t *testing.T) {
	engine, err := orch.NewPipelineBuilder().
		WithSupervisor(&supervisor.StateMachine{DefaultPhase: "a"}).
		RegisterNode("a", dummyNode("ok", orch.EventWaitUser)).
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
	orch.NewPipelineBuilder().MustBuild()
}

func TestBuilder_StateMachine_MissingTransitionTarget(t *testing.T) {
	_, err := orch.NewPipelineBuilder().
		WithSupervisor(&supervisor.StateMachine{
			DefaultPhase: "a",
			Transitions:  []supervisor.TransitionRule{{From: "a", To: "b"}},
		}).
		RegisterNode("a", dummyNode("ok", orch.EventWaitUser)).
		Build()
	if err == nil {
		t.Error("expected error for missing transition target 'b'")
	}
}

func TestBuilder_StateMachine_MissingConditionalTarget(t *testing.T) {
	_, err := orch.NewPipelineBuilder().
		WithSupervisor(&supervisor.StateMachine{
			DefaultPhase: "a",
			ConditionalTransitions: []supervisor.ConditionalTransition{
				{From: "a", To: "missing", Condition: func(_ orch.StateStore) bool { return true }},
			},
		}).
		RegisterNode("a", dummyNode("ok", orch.EventWaitUser)).
		Build()
	if err == nil {
		t.Error("expected error for missing conditional target")
	}
}

func TestBuilder_LinearSupervisor_MissingPhase(t *testing.T) {
	_, err := orch.NewPipelineBuilder().
		WithSupervisor(supervisor.NewLinear("a", "b")).
		RegisterNode("a", dummyNode("ok", orch.EventWaitUser)).
		Build()
	if err == nil {
		t.Error("expected error for missing linear phase 'b'")
	}
}

func TestBuilder_LinearSupervisor_ValidBuild(t *testing.T) {
	engine, err := orch.NewPipelineBuilder().
		WithSupervisor(supervisor.NewLinear("a", "b")).
		RegisterNode("a", dummyNode("a", orch.EventPhaseComplete)).
		RegisterNode("b", dummyNode("b", orch.EventWaitUser)).
		Build()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	result, err := engine.Run(context.Background(), store.NewMemory(), orch.NewTurn("c1", "hi"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Answer != "b" {
		t.Errorf("expected answer b, got %s", result.Answer)
	}
}

func TestBuilder_MaxSteps(t *testing.T) {
	engine := orch.NewPipelineBuilder().
		MaxSteps(5).
		WithSupervisor(&supervisor.StateMachine{DefaultPhase: "a"}).
		RegisterNode("a", dummyNode("ok", orch.EventWaitUser)).
		MustBuild()

	if engine.MaxStepsForTest() != 5 {
		t.Errorf("expected maxSteps 5, got %d", engine.MaxStepsForTest())
	}
}

func TestBuilder_Repeatable(t *testing.T) {
	engine := orch.NewPipelineBuilder().
		WithSupervisor(&supervisor.StateMachine{DefaultPhase: "a"}).
		RegisterNode("a", dummyNode("ok", orch.EventWaitUser)).
		RegisterNode("b", dummyNode("ok", orch.EventWaitUser)).
		Repeatable("a", "b").
		MustBuild()

	if !engine.RepeatablePhaseForTest("a") || !engine.RepeatablePhaseForTest("b") {
		t.Error("expected phases a and b to be repeatable")
	}
}

func TestBuilder_Hooks(t *testing.T) {
	engine := orch.NewPipelineBuilder().
		WithSupervisor(&supervisor.StateMachine{DefaultPhase: "a"}).
		RegisterNode("a", dummyNode("ok", orch.EventWaitUser)).
		OnFatalPreprocess(func(_ context.Context, _ orch.StateStore, _ *orch.Turn) error { return nil }).
		OnPreprocess(func(_ context.Context, _ orch.StateStore, _ *orch.Turn) error { return nil }).
		OnPostprocess(func(_ context.Context, _ orch.StateStore, _ *orch.Turn, _ *orch.PipelineResult) error { return nil }).
		MustBuild()

	if engine.FatalPreprocessHookCountForTest() != 1 {
		t.Errorf("expected 1 fatal preprocess hook, got %d", engine.FatalPreprocessHookCountForTest())
	}
	if engine.PreprocessHookCountForTest() != 1 {
		t.Errorf("expected 1 preprocess hook, got %d", engine.PreprocessHookCountForTest())
	}
	if engine.PostprocessHookCountForTest() != 1 {
		t.Errorf("expected 1 postprocess hook, got %d", engine.PostprocessHookCountForTest())
	}
}

func TestBuilder_WithLogger(t *testing.T) {
	logger := &testLogger{}
	engine := orch.NewPipelineBuilder().
		WithSupervisor(&supervisor.StateMachine{DefaultPhase: "a"}).
		RegisterNode("a", dummyNode("ok", orch.EventWaitUser)).
		WithLogger(logger).
		MustBuild()

	if engine.LoggerForTest() == nil {
		t.Error("expected logger to be set")
	}

	// Run to verify logger is called
	engine.Run(context.Background(), store.NewMemory(), orch.NewTurn("c1", "hi"))
	if !logger.called {
		t.Error("expected logger to be called during Run")
	}
}

func TestBuilder_DuplicateRegisterOverwrites(t *testing.T) {
	engine := orch.NewPipelineBuilder().
		WithSupervisor(&supervisor.StateMachine{DefaultPhase: "a"}).
		RegisterNode("a", dummyNode("first", orch.EventWaitUser)).
		RegisterNode("a", dummyNode("second", orch.EventWaitUser)).
		MustBuild()

	result, _ := engine.Run(context.Background(), store.NewMemory(), orch.NewTurn("c1", "hi"))
	if result.Answer != "second" {
		t.Errorf("expected second node to win, got %s", result.Answer)
	}
}

func TestBuilder_DefaultMaxSteps(t *testing.T) {
	engine := orch.NewPipelineBuilder().
		WithSupervisor(&supervisor.StateMachine{DefaultPhase: "a"}).
		RegisterNode("a", dummyNode("ok", orch.EventWaitUser)).
		MustBuild()

	if engine.MaxStepsForTest() != 3 {
		t.Errorf("expected default maxSteps 3, got %d", engine.MaxStepsForTest())
	}
}

// ── BuildFromConfig tests ───────────────────────────────────────────

func TestBuildFromConfig_NoSupervisor_Error(t *testing.T) {
	_, err := orch.BuildFromConfig(orch.PipelineConfig{
		Nodes: []orch.NodeConfig{{Phase: "a", Fn: dummyNode("ok", orch.EventWaitUser)}},
	})
	if err == nil {
		t.Error("expected error when supervisor is missing")
	}
}

func TestBuildFromConfig_NoNodes_Error(t *testing.T) {
	_, err := orch.BuildFromConfig(orch.PipelineConfig{
		Supervisor: &supervisor.StateMachine{DefaultPhase: "a"},
	})
	if err == nil {
		t.Error("expected error when no nodes registered")
	}
}

func TestBuildFromConfig_ValidConfig(t *testing.T) {
	engine, err := orch.BuildFromConfig(orch.PipelineConfig{
		MaxSteps:   5,
		Supervisor: &supervisor.StateMachine{DefaultPhase: "a"},
		Nodes: []orch.NodeConfig{
			{Phase: "a", Fn: dummyNode("hello", orch.EventWaitUser), Repeatable: true},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if engine.MaxStepsForTest() != 5 {
		t.Errorf("expected maxSteps 5, got %d", engine.MaxStepsForTest())
	}
	if !engine.RepeatablePhaseForTest("a") {
		t.Error("expected phase 'a' to be repeatable")
	}
}

func TestBuildFromConfig_Hooks(t *testing.T) {
	called := false
	hook := func(_ context.Context, _ orch.StateStore, _ *orch.Turn, _ *orch.PipelineResult) error {
		called = true
		return nil
	}

	engine, err := orch.BuildFromConfig(orch.PipelineConfig{
		Supervisor: &supervisor.StateMachine{DefaultPhase: "a"},
		Nodes:      []orch.NodeConfig{{Phase: "a", Fn: dummyNode("ok", orch.EventWaitUser)}},
		Hooks:      orch.HookConfig{PostprocessHooks: []orch.PostprocessHook{hook}},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	engine.Run(context.Background(), store.NewMemory(), orch.NewTurn("c1", "hi"))
	if !called {
		t.Error("expected postprocess hook to be called")
	}
}

func TestBuildFromConfig_ConcurrentNode(t *testing.T) {
	engine, err := orch.BuildFromConfig(orch.PipelineConfig{
		Supervisor: &supervisor.StateMachine{DefaultPhase: "a"},
		Nodes: []orch.NodeConfig{
			{Phase: "a", Fn: dummyNode("ok", orch.EventWaitUser), Concurrent: true},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !engine.NodeConcurrentSafeForTest("a") {
		t.Error("expected node 'a' to be marked concurrent-safe")
	}
}

func TestBuildFromConfig_EmptyHooks_NoPanic(t *testing.T) {
	_, err := orch.BuildFromConfig(orch.PipelineConfig{
		Supervisor: &supervisor.StateMachine{DefaultPhase: "a"},
		Nodes:      []orch.NodeConfig{{Phase: "a", Fn: dummyNode("ok", orch.EventWaitUser)}},
		Hooks:      orch.HookConfig{}, // all empty
	})
	if err != nil {
		t.Fatalf("unexpected error with empty hooks: %v", err)
	}
}
