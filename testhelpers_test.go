package orchestrator_test

import (
	"context"

	orch "github.com/baalamai/orchestrator"
	"github.com/baalamai/orchestrator/supervisor"
)

// dummyNode returns a NodeFunc that emits the given answer and event.
func dummyNode(answer string, event orch.EventType) orch.NodeFunc {
	return func(_ context.Context, _ orch.StateView, _ *orch.Turn, _ *orch.NodeInput) (*orch.NodeResult, error) {
		return &orch.NodeResult{Event: event, Answer: answer}, nil
	}
}

// buildSimpleNodeEngine assembles a one-node engine around a StateMachine with
// the given phase as default. Used by tests that need an engine without
// configuring a full supervisor tree.
func buildSimpleNodeEngine(phase string, node orch.NodeFunc) *orch.Engine {
	return orch.NewPipelineBuilder().
		WithSupervisor(&supervisor.StateMachine{DefaultPhase: phase}).
		RegisterNode(phase, node).
		MustBuild()
}

// mockRouter is an IntentRouter implementation used by engine tests.
type mockRouter struct {
	target orch.Phase
	err    error
	usage  *orch.Usage
}

func (m *mockRouter) Route(_ context.Context, _ orch.StateStore, _ string) (orch.Phase, error) {
	return m.target, m.err
}

func (m *mockRouter) Usage() *orch.Usage { return m.usage }

// testLogger is a Logger that records whether any method was called.
type testLogger struct{ called bool }

func (l *testLogger) Info(_ context.Context, _ string, _ ...any)  { l.called = true }
func (l *testLogger) Error(_ context.Context, _ string, _ ...any) { l.called = true }
func (l *testLogger) Warn(_ context.Context, _ string, _ ...any)  { l.called = true }
