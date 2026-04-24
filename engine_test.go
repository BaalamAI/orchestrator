package orchestrator_test

import (
	"context"
	"fmt"
	"testing"

	orch "github.com/baalamai/orchestrator"
	"github.com/baalamai/orchestrator/store"
	"github.com/baalamai/orchestrator/supervisor"
)

func TestEngine_SimpleRun(t *testing.T) {
	engine := buildSimpleNodeEngine("greet", dummyNode("hello!", orch.EventWaitUser))

	result, err := engine.Run(context.Background(), store.NewMemory(), orch.NewTurn("c1", "hi"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Answer != "hello!" {
		t.Errorf("expected hello!, got %s", result.Answer)
	}
}

func TestEngine_UserMessageAddedToStore(t *testing.T) {
	engine := buildSimpleNodeEngine("echo", dummyNode("ok", orch.EventWaitUser))
	store := store.NewMemory()

	engine.Run(context.Background(), store, orch.NewTurn("c1", "test message"))

	msgs := store.Messages()
	if len(msgs) < 1 {
		t.Fatal("expected at least 1 message in store")
	}
	if msgs[0].Role != "user" || msgs[0].Text != "test message" {
		t.Errorf("expected user message 'test message', got %+v", msgs[0])
	}
}

func TestEngine_FatalPreprocessHook_StopsPipeline(t *testing.T) {
	engine := orch.NewPipelineBuilder().
		WithSupervisor(&supervisor.StateMachine{DefaultPhase: "a"}).
		RegisterNode("a", dummyNode("should not run", orch.EventWaitUser)).
		OnFatalPreprocess(func(_ context.Context, _ orch.StateStore, _ *orch.Turn) error {
			return fmt.Errorf("auth failed")
		}).
		MustBuild()

	result, err := engine.Run(context.Background(), store.NewMemory(), orch.NewTurn("c1", "hi"))
	if err == nil {
		t.Fatal("expected error from fatal hook")
	}
	if result != nil {
		t.Error("expected nil result on fatal hook error")
	}
}

func TestEngine_PreprocessHook_ErrorLoggedButContinues(t *testing.T) {
	hookCalled := false
	engine := orch.NewPipelineBuilder().
		WithSupervisor(&supervisor.StateMachine{DefaultPhase: "a"}).
		RegisterNode("a", dummyNode("ok", orch.EventWaitUser)).
		OnPreprocess(func(_ context.Context, _ orch.StateStore, _ *orch.Turn) error {
			hookCalled = true
			return fmt.Errorf("non-fatal error")
		}).
		MustBuild()

	result, err := engine.Run(context.Background(), store.NewMemory(), orch.NewTurn("c1", "hi"))
	if err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}
	if !hookCalled {
		t.Error("preprocess hook was not called")
	}
	if result.Answer != "ok" {
		t.Errorf("expected ok, got %s", result.Answer)
	}
}

func TestEngine_PostprocessHook_Runs(t *testing.T) {
	postprocessed := false
	engine := orch.NewPipelineBuilder().
		WithSupervisor(&supervisor.StateMachine{DefaultPhase: "a"}).
		RegisterNode("a", dummyNode("raw", orch.EventWaitUser)).
		OnPostprocess(func(_ context.Context, _ orch.StateStore, _ *orch.Turn, result *orch.PipelineResult) error {
			postprocessed = true
			result.Answer = "processed"
			return nil
		}).
		MustBuild()

	result, err := engine.Run(context.Background(), store.NewMemory(), orch.NewTurn("c1", "hi"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !postprocessed {
		t.Error("postprocess hook was not called")
	}
	if result.Answer != "processed" {
		t.Errorf("expected processed, got %s", result.Answer)
	}
}

func TestEngine_MultiplePostprocessHooksOrder(t *testing.T) {
	var order []int
	makeHook := func(n int) orch.PostprocessHook {
		return func(_ context.Context, _ orch.StateStore, _ *orch.Turn, _ *orch.PipelineResult) error {
			order = append(order, n)
			return nil
		}
	}

	engine := orch.NewPipelineBuilder().
		WithSupervisor(&supervisor.StateMachine{DefaultPhase: "a"}).
		RegisterNode("a", dummyNode("ok", orch.EventWaitUser)).
		OnPostprocess(makeHook(1), makeHook(2), makeHook(3)).
		MustBuild()

	engine.Run(context.Background(), store.NewMemory(), orch.NewTurn("c1", "hi"))

	if len(order) != 3 {
		t.Fatalf("expected 3 hooks, got %d", len(order))
	}
	for i, v := range order {
		if v != i+1 {
			t.Errorf("hook %d executed at position %d", v, i)
		}
	}
}

func TestEngine_ContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	node := func(_ context.Context, _ orch.StateView, _ *orch.Turn, _ *orch.NodeInput) (*orch.NodeResult, error) {
		cancel() // Cancel after first execution
		return &orch.NodeResult{Event: orch.EventStepSuccess, Answer: "ok"}, nil
	}

	engine := orch.NewPipelineBuilder().
		MaxSteps(5).
		WithSupervisor(&supervisor.StateMachine{DefaultPhase: "a"}).
		RegisterNode("a", node).
		Repeatable("a").
		MustBuild()

	_, err := engine.Run(ctx, store.NewMemory(), orch.NewTurn("c1", "hi"))
	if err == nil {
		t.Error("expected error from cancelled context")
	}
}

func TestEngine_AgentError(t *testing.T) {
	node := func(_ context.Context, _ orch.StateView, _ *orch.Turn, _ *orch.NodeInput) (*orch.NodeResult, error) {
		return nil, fmt.Errorf("agent crashed")
	}

	engine := buildSimpleNodeEngine("a", node)
	_, err := engine.Run(context.Background(), store.NewMemory(), orch.NewTurn("c1", "hi"))
	if err == nil {
		t.Error("expected error from agent")
	}
}

func TestEngine_MaxStepsRespected(t *testing.T) {
	callCount := 0
	node := func(_ context.Context, _ orch.StateView, _ *orch.Turn, _ *orch.NodeInput) (*orch.NodeResult, error) {
		callCount++
		return &orch.NodeResult{Event: orch.EventStepSuccess, Answer: "step"}, nil
	}

	engine := orch.NewPipelineBuilder().
		MaxSteps(2).
		WithSupervisor(&supervisor.StateMachine{DefaultPhase: "a"}).
		RegisterNode("a", node).
		Repeatable("a").
		MustBuild()

	engine.Run(context.Background(), store.NewMemory(), orch.NewTurn("c1", "hi"))

	if callCount != 2 {
		t.Errorf("expected max 2 calls, got %d", callCount)
	}
}

func TestEngine_EmptyAnswerDoesNotOverwrite(t *testing.T) {
	step := 0
	makeNode := func(answer string, event orch.EventType) orch.NodeFunc {
		return func(_ context.Context, _ orch.StateView, _ *orch.Turn, _ *orch.NodeInput) (*orch.NodeResult, error) {
			step++
			return &orch.NodeResult{Event: event, Answer: answer}, nil
		}
	}

	engine := orch.NewPipelineBuilder().
		WithSupervisor(&supervisor.StateMachine{
			DefaultPhase: "first",
			Transitions:  []supervisor.TransitionRule{{From: "first", To: "second"}},
		}).
		RegisterNode("first", makeNode("real answer", orch.EventPhaseComplete)).
		RegisterNode("second", makeNode("", orch.EventWaitUser)).
		MustBuild()

	result, _ := engine.Run(context.Background(), store.NewMemory(), orch.NewTurn("c1", "hi"))
	if result.Answer != "real answer" {
		t.Errorf("expected 'real answer' preserved, got %q", result.Answer)
	}
}

func TestEngine_SupervisorAndAgentUsageCombined(t *testing.T) {
	router := &mockRouter{
		target: "a",
		usage:  &orch.Usage{PromptTokens: 50, CompletionTokens: 20, TotalTokens: 70},
	}
	sup := &supervisor.StateMachine{DefaultPhase: "a", Router: router}

	node := func(_ context.Context, _ orch.StateView, _ *orch.Turn, _ *orch.NodeInput) (*orch.NodeResult, error) {
		return &orch.NodeResult{
			Event:  orch.EventWaitUser,
			Answer: "ok",
			Usage:  &orch.Usage{PromptTokens: 100, CompletionTokens: 50, TotalTokens: 150},
		}, nil
	}

	engine := orch.NewPipelineBuilder().
		WithSupervisor(sup).
		RegisterNode("a", node).
		MustBuild()

	result, _ := engine.Run(context.Background(), store.NewMemory(), orch.NewTurn("c1", "hi"))
	if result.Usage == nil {
		t.Fatal("expected combined usage")
	}
	// 50 (router) + 100 (node) = 150
	if result.Usage.PromptTokens != 150 {
		t.Errorf("expected 150 combined prompt tokens, got %d", result.Usage.PromptTokens)
	}
}

func TestEngine_UsageAccumulated(t *testing.T) {
	node := func(_ context.Context, _ orch.StateView, _ *orch.Turn, _ *orch.NodeInput) (*orch.NodeResult, error) {
		return &orch.NodeResult{
			Event:  orch.EventWaitUser,
			Answer: "ok",
			Usage: &orch.Usage{
				PromptTokens: 100, CompletionTokens: 50, TotalTokens: 150,
				Breakdown: []orch.ModelUsage{{Agent: "test", PromptTokens: 100}},
			},
		}, nil
	}

	engine := buildSimpleNodeEngine("a", node)
	result, _ := engine.Run(context.Background(), store.NewMemory(), orch.NewTurn("c1", "hi"))

	if result.Usage == nil {
		t.Fatal("expected usage in result")
	}
	if result.Usage.TotalTokens != 150 {
		t.Errorf("expected 150 total tokens, got %d", result.Usage.TotalTokens)
	}
	if len(result.Usage.Breakdown) != 1 {
		t.Errorf("expected 1 breakdown entry, got %d", len(result.Usage.Breakdown))
	}
}

// ── NodeFunc (pure-function agent) tests ────────────────────────────

func TestEngine_NodeFunc_SimpleRun(t *testing.T) {
	engine := orch.NewPipelineBuilder().
		WithSupervisor(&supervisor.StateMachine{DefaultPhase: "greet"}).
		RegisterNode("greet", dummyNode("hello from node!", orch.EventWaitUser)).
		MustBuild()

	result, err := engine.Run(context.Background(), store.NewMemory(), orch.NewTurn("c1", "hi"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.Answer != "hello from node!" {
		t.Errorf("expected 'hello from node!', got %s", result.Answer)
	}
}

func TestEngine_NodeFunc_DeltaApplied(t *testing.T) {
	node := func(_ context.Context, _ orch.StateView, _ *orch.Turn, _ *orch.NodeInput) (*orch.NodeResult, error) {
		return &orch.NodeResult{
			Event:  orch.EventWaitUser,
			Answer: "done",
			Delta: orch.StateDelta{
				Updates: map[string]any{"product": "herbicida", "diagnostic_complete": true},
			},
		}, nil
	}

	engine := orch.NewPipelineBuilder().
		WithSupervisor(&supervisor.StateMachine{DefaultPhase: "diag"}).
		RegisterNode("diag", node).
		MustBuild()

	store := store.NewMemory()
	engine.Run(context.Background(), store, orch.NewTurn("c1", "hi"))

	if !store.HasFlag("diagnostic_complete") {
		t.Error("expected diagnostic_complete flag set")
	}
	state := store.State()
	if state["product"] != "herbicida" {
		t.Errorf("expected product=herbicida, got %v", state["product"])
	}
}

func TestEngine_NodeFunc_ReceivesSnapshot(t *testing.T) {
	var receivedView orch.StateView

	node := func(_ context.Context, view orch.StateView, _ *orch.Turn, _ *orch.NodeInput) (*orch.NodeResult, error) {
		receivedView = view
		return &orch.NodeResult{Event: orch.EventWaitUser, Answer: "ok"}, nil
	}

	engine := orch.NewPipelineBuilder().
		WithSupervisor(&supervisor.StateMachine{DefaultPhase: "a"}).
		RegisterNode("a", node).
		MustBuild()

	store := store.NewMemory()
	store.SetState("existing_key", "value")

	engine.Run(context.Background(), store, orch.NewTurn("c1", "hi"))

	if receivedView == nil {
		t.Fatal("expected node to receive a StateView")
	}
	v, ok := receivedView.Get("existing_key")
	if !ok || v != "value" {
		t.Errorf("expected existing_key=value in snapshot, got %v", v)
	}
}

func TestEngine_NodeFunc_MiddlewareChain(t *testing.T) {
	var order []string

	mw1 := func(next orch.NodeFunc) orch.NodeFunc {
		return func(ctx context.Context, view orch.StateView, turn *orch.Turn, input *orch.NodeInput) (*orch.NodeResult, error) {
			order = append(order, "mw1-before")
			result, err := next(ctx, view, turn, input)
			order = append(order, "mw1-after")
			return result, err
		}
	}

	mw2 := func(next orch.NodeFunc) orch.NodeFunc {
		return func(ctx context.Context, view orch.StateView, turn *orch.Turn, input *orch.NodeInput) (*orch.NodeResult, error) {
			order = append(order, "mw2-before")
			result, err := next(ctx, view, turn, input)
			order = append(order, "mw2-after")
			return result, err
		}
	}

	node := func(_ context.Context, _ orch.StateView, _ *orch.Turn, _ *orch.NodeInput) (*orch.NodeResult, error) {
		order = append(order, "node")
		return &orch.NodeResult{Event: orch.EventWaitUser, Answer: "ok"}, nil
	}

	engine := orch.NewPipelineBuilder().
		WithSupervisor(&supervisor.StateMachine{DefaultPhase: "a"}).
		RegisterNode("a", node, mw1, mw2).
		MustBuild()

	engine.Run(context.Background(), store.NewMemory(), orch.NewTurn("c1", "hi"))

	expected := []string{"mw1-before", "mw2-before", "node", "mw2-after", "mw1-after"}
	if len(order) != len(expected) {
		t.Fatalf("expected order %v, got %v", expected, order)
	}
	for i, v := range expected {
		if order[i] != v {
			t.Errorf("position %d: expected %s, got %s", i, v, order[i])
		}
	}
}

func TestEngine_NodeFunc_PrePostAgentHooks(t *testing.T) {
	var hookOrder []string

	preHook := func(_ context.Context, _ orch.StateView, _ *orch.Turn, phase string) error {
		hookOrder = append(hookOrder, "pre:"+phase)
		return nil
	}
	postHook := func(_ context.Context, _ orch.StateView, _ *orch.Turn, _ *orch.NodeResult, phase string) error {
		hookOrder = append(hookOrder, "post:"+phase)
		return nil
	}

	engine := orch.NewPipelineBuilder().
		WithSupervisor(&supervisor.StateMachine{DefaultPhase: "a"}).
		RegisterNode("a", dummyNode("ok", orch.EventWaitUser)).
		OnPreAgent(preHook).
		OnPostAgent(postHook).
		MustBuild()

	engine.Run(context.Background(), store.NewMemory(), orch.NewTurn("c1", "hi"))

	if len(hookOrder) != 2 {
		t.Fatalf("expected 2 hooks, got %v", hookOrder)
	}
	if hookOrder[0] != "pre:a" {
		t.Errorf("expected pre:a, got %s", hookOrder[0])
	}
	if hookOrder[1] != "post:a" {
		t.Errorf("expected post:a, got %s", hookOrder[1])
	}
}

func TestEngine_NodeFunc_MiddlewareEnrichesInput(t *testing.T) {
	captureMW := func(next orch.NodeFunc) orch.NodeFunc {
		return func(ctx context.Context, view orch.StateView, turn *orch.Turn, input *orch.NodeInput) (*orch.NodeResult, error) {
			input.CapturedFields = map[string]any{"product": "insecticida"}
			input.RAGContext = "contexto RAG"
			return next(ctx, view, turn, input)
		}
	}

	var receivedInput *orch.NodeInput
	node := func(_ context.Context, _ orch.StateView, _ *orch.Turn, input *orch.NodeInput) (*orch.NodeResult, error) {
		receivedInput = input
		return &orch.NodeResult{Event: orch.EventWaitUser, Answer: "ok"}, nil
	}

	engine := orch.NewPipelineBuilder().
		WithSupervisor(&supervisor.StateMachine{DefaultPhase: "a"}).
		RegisterNode("a", node, captureMW).
		MustBuild()

	engine.Run(context.Background(), store.NewMemory(), orch.NewTurn("c1", "hi"))

	if receivedInput == nil {
		t.Fatal("expected node to receive input")
	}
	if receivedInput.CapturedFields["product"] != "insecticida" {
		t.Errorf("expected product=insecticida, got %v", receivedInput.CapturedFields["product"])
	}
	if receivedInput.RAGContext != "contexto RAG" {
		t.Errorf("expected RAG context, got %s", receivedInput.RAGContext)
	}
}

func TestStateDelta_Merge(t *testing.T) {
	d1 := &orch.StateDelta{
		Updates: map[string]any{"a": 1, "b": 2},
		Deletes: []string{"x"},
	}
	d2 := &orch.StateDelta{
		Updates: map[string]any{"b": 99, "c": 3},
		Deletes: []string{"y"},
	}
	d1.Merge(d2)

	if d1.Updates["a"] != 1 {
		t.Errorf("expected a=1, got %v", d1.Updates["a"])
	}
	if d1.Updates["b"] != 99 {
		t.Errorf("expected b=99 (overwrite), got %v", d1.Updates["b"])
	}
	if d1.Updates["c"] != 3 {
		t.Errorf("expected c=3, got %v", d1.Updates["c"])
	}
	if len(d1.Deletes) != 2 {
		t.Errorf("expected 2 deletes, got %d", len(d1.Deletes))
	}
}
