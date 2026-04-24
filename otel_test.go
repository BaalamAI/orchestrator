package orchestrator_test

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel/metric/noop"
	tracenoop "go.opentelemetry.io/otel/trace/noop"

	orch "github.com/baalamai/orchestrator"
	"github.com/baalamai/orchestrator/store"
	"github.com/baalamai/orchestrator/supervisor"
)

// TestEngine_Build_WithExplicitNoopTracerMeter verifies that wiring an explicit
// noop tracer/meter via the builder does not break engine construction.
func TestEngine_Build_WithExplicitNoopTracerMeter(t *testing.T) {
	tracer := tracenoop.NewTracerProvider().Tracer("test")
	meter := noop.NewMeterProvider().Meter("test")

	engine := orch.NewPipelineBuilder().
		WithSupervisor(&supervisor.StateMachine{DefaultPhase: "a"}).
		RegisterNode("a", dummyNode("ok", orch.EventWaitUser)).
		WithTracer(tracer).
		WithMeter(meter).
		MustBuild()

	if !engine.TracerIsSetForTest() {
		t.Fatal("expected tracer to be set")
	}
	if !engine.MeterIsSetForTest() {
		t.Fatal("expected meter to be set")
	}
	if !engine.InstrumentsIsSetForTest() {
		t.Fatal("expected instruments to be set")
	}
}

// TestEngine_Run_NoopObservability verifies that a full Run() completes
// successfully when no tracer/meter are configured (default noop behavior).
// This is the regression guard — existing callers must keep working.
func TestEngine_Run_NoopObservability(t *testing.T) {
	engine := buildSimpleNodeEngine("greet", dummyNode("hi", orch.EventWaitUser))

	result, err := engine.Run(context.Background(), store.NewMemory(), orch.NewTurn("c1", "hola"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Answer != "hi" {
		t.Errorf("expected 'hi', got %q", result.Answer)
	}
}

// TestBuildFromConfig_WithTracerMeter verifies the declarative PipelineConfig
// path wires Tracer and Meter through to the Engine.
func TestBuildFromConfig_WithTracerMeter(t *testing.T) {
	tracer := tracenoop.NewTracerProvider().Tracer("test")
	meter := noop.NewMeterProvider().Meter("test")

	cfg := orch.PipelineConfig{
		MaxSteps:   3,
		Supervisor: &supervisor.StateMachine{DefaultPhase: "a"},
		Tracer:     tracer,
		Meter:      meter,
		Nodes: []orch.NodeConfig{
			{Phase: "a", Fn: dummyNode("ok", orch.EventWaitUser)},
		},
	}

	engine, err := orch.BuildFromConfig(cfg)
	if err != nil {
		t.Fatalf("BuildFromConfig: %v", err)
	}
	if !engine.TracerIsSetForTest() || !engine.MeterIsSetForTest() || !engine.InstrumentsIsSetForTest() {
		t.Fatal("expected tracer/meter/instruments to be wired")
	}

	result, err := engine.Run(context.Background(), store.NewMemory(), orch.NewTurn("c1", "test"))
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if result.Answer != "ok" {
		t.Errorf("expected 'ok', got %q", result.Answer)
	}
}

// TestEngine_Run_UsesInstruments_NoPanic is a smoke test that tokens counter
// and phase duration histogram don't panic when a node returns Usage with breakdowns.
func TestEngine_Run_UsesInstruments_NoPanic(t *testing.T) {
	nodeWithUsage := func(_ context.Context, _ orch.StateView, _ *orch.Turn, _ *orch.NodeInput) (*orch.NodeResult, error) {
		return &orch.NodeResult{
			Event:  orch.EventWaitUser,
			Answer: "done",
			Usage: &orch.Usage{
				PromptTokens:     10,
				CompletionTokens: 5,
				TotalTokens:      15,
				Breakdown: []orch.ModelUsage{
					{Agent: "test", Model: "claude-opus-4-7", Provider: "anthropic", PromptTokens: 10, CompletionTokens: 5, TotalTokens: 15},
				},
			},
		}, nil
	}

	engine := orch.NewPipelineBuilder().
		WithSupervisor(&supervisor.StateMachine{DefaultPhase: "a"}).
		RegisterNode("a", nodeWithUsage).
		MustBuild()

	result, err := engine.Run(context.Background(), store.NewMemory(), orch.NewTurn("c1", "test"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Usage == nil || result.Usage.TotalTokens != 15 {
		t.Errorf("expected total_tokens=15, got %+v", result.Usage)
	}
}
