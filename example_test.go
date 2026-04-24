package orchestrator_test

import (
	"context"
	"fmt"

	orch "github.com/baalamai/orchestrator"
	st "github.com/baalamai/orchestrator/store"
	sup "github.com/baalamai/orchestrator/supervisor"
)

func ExampleEngine_Run() {
	engine := orch.NewPipelineBuilder().
		WithSupervisor(sup.NewLinear("greet", "farewell")).
		RegisterNode("greet", func(_ context.Context, _ orch.StateView, turn *orch.Turn, _ *orch.NodeInput) (*orch.NodeResult, error) {
			return &orch.NodeResult{Event: orch.EventPhaseComplete, Answer: "Hola " + turn.Text + "!"}, nil
		}).
		RegisterNode("farewell", func(_ context.Context, _ orch.StateView, _ *orch.Turn, _ *orch.NodeInput) (*orch.NodeResult, error) {
			return &orch.NodeResult{Event: orch.EventWaitUser, Answer: "En que mas te puedo ayudar?"}, nil
		}).
		MustBuild()

	store := st.NewMemory()
	result, err := engine.Run(context.Background(), store, orch.NewTurn("conv-1", "mundo"))
	if err != nil {
		panic(err)
	}
	fmt.Println(result.Answer)
	// Output: En que mas te puedo ayudar?
}

func Example_linearSupervisor() {
	// LinearSupervisor executes phases in order: intake → process → respond.
	// Each phase must emit EventPhaseComplete to advance to the next one.
	engine := orch.NewPipelineBuilder().
		WithSupervisor(sup.NewLinear("intake", "process", "respond")).
		RegisterNode("intake", func(_ context.Context, _ orch.StateView, _ *orch.Turn, _ *orch.NodeInput) (*orch.NodeResult, error) {
			return &orch.NodeResult{Event: orch.EventPhaseComplete}, nil
		}).
		RegisterNode("process", func(_ context.Context, _ orch.StateView, _ *orch.Turn, _ *orch.NodeInput) (*orch.NodeResult, error) {
			return &orch.NodeResult{Event: orch.EventPhaseComplete}, nil
		}).
		RegisterNode("respond", func(_ context.Context, _ orch.StateView, turn *orch.Turn, _ *orch.NodeInput) (*orch.NodeResult, error) {
			return &orch.NodeResult{Event: orch.EventWaitUser, Answer: "Procesado: " + turn.Text}, nil
		}).
		MustBuild()

	result, _ := engine.Run(context.Background(), st.NewMemory(), orch.NewTurn("c1", "test"))
	fmt.Println(result.Answer)
	// Output: Procesado: test
}

func Example_stateMachineSupervisor() {
	// StateMachine supervisor uses rules to decide the next phase:
	// 1. Transitions on EventPhaseComplete (diagnostic → payment)
	// 2. State flags (chosen_payment=true → register)
	// 3. DefaultPhase as fallback
	supv := &sup.StateMachine{
		DefaultPhase: "diagnostic",
		Transitions: []sup.TransitionRule{
			{From: "diagnostic", To: "payment"},
		},
		FlagRules: []sup.FlagRule{
			{Flag: "chosen_payment", Phase: "register"},
		},
	}

	engine := orch.NewPipelineBuilder().
		WithSupervisor(supv).
		RegisterNode("diagnostic", func(_ context.Context, _ orch.StateView, _ *orch.Turn, _ *orch.NodeInput) (*orch.NodeResult, error) {
			return &orch.NodeResult{
				Event:  orch.EventPhaseComplete,
				Answer: "Diagnostico listo",
				Delta:  orch.StateDelta{Updates: map[string]any{"diagnostic_complete": true}},
			}, nil
		}).
		RegisterNode("payment", func(_ context.Context, _ orch.StateView, _ *orch.Turn, _ *orch.NodeInput) (*orch.NodeResult, error) {
			return &orch.NodeResult{Event: orch.EventWaitUser, Answer: "Selecciona tu metodo de pago"}, nil
		}).
		RegisterNode("register", func(_ context.Context, _ orch.StateView, _ *orch.Turn, _ *orch.NodeInput) (*orch.NodeResult, error) {
			return &orch.NodeResult{Event: orch.EventWaitUser, Answer: "Registro completo"}, nil
		}).
		MustBuild()

	// First turn: starts at diagnostic (default) → cascades to payment
	result, _ := engine.Run(context.Background(), st.NewMemory(), orch.NewTurn("c1", "hola"))
	fmt.Println(result.Answer)
	// Output: Selecciona tu metodo de pago
}
