// Package orchestrator provides a generic framework for building multi-phase
// agent pipelines with a supervisor loop. It decouples routing logic (supervisor)
// from execution (agents), supports pre/post-processing hooks, and offers
// multiple supervision strategies.
//
// # Entry point
//
// The main entry point is [Engine.Run], which executes the full pipeline:
// preprocess hooks, supervisor-agent loop, and postprocess hooks.
//
// # Package layout (hexagonal)
//
// Root package — Core + Ports:
//
//   - domain.go        Domain types: Turn, Message, StateView, StateDelta, NodeFunc,
//     NodeResult, EventType, Phase, Usage, PipelineResult.
//   - ports.go         Outbound ports the core depends on: StateStore, Supervisor,
//     SupervisorValidator, ParallelSupervisor, IntentRouter,
//     ErrorClassifier, CostCalculator.
//   - hooks.go         Hook signatures: PreprocessHook, PostprocessHook,
//     FatalPreprocessHook, SupervisorHook, PreAgentHook,
//     PostAgentHook, Logger.
//   - errors.go        ErrorCategory enum.
//   - checkpoint.go    Checkpoint type + CheckpointStore port.
//   - orchestrator.go  Engine struct, RetryPolicy, errorLedger.
//   - run.go           Engine.Run entry point and checkpoint resume.
//   - loop.go          The supervisor→agent execution cycle.
//   - retry.go         Retry logic and node execution (compute / commit).
//   - parallel.go      Parallel phase execution.
//   - budget.go        Budget/cost enforcement.
//   - snapshot.go      StateView helper wrapping a StateStore.
//   - metrics.go       OTel instruments owned by the Engine.
//   - builder.go       PipelineBuilder — fluent constructor.
//   - config.go        PipelineConfig — declarative constructor.
//
// Adapters in subpackages:
//
//   - supervisor/      Concrete Supervisor implementations (Linear, StateMachine)
//     with deterministic rules and optional LLM routing.
//   - store/           In-memory StateStore and CheckpointStore adapters (Memory,
//     MemoryCheckpoint) for tests and lightweight pipelines.
//   - llm/             LLM subsystem: Client + Tool ports and the ReAct-style
//     NewToolLoopNode adapter that wraps them into a NodeFunc.
//   - hook/            Higher-order helpers for composing lifecycle hooks
//     (ConditionalPreprocess, WithTimeout, ComposeParallel, …).
//
// Dependency rule: subpackages depend on the root package; the root package
// does not depend on any adapter subpackage except llm/ (for the tool-loop
// observability inject point).
//
// # Runtime execution order
//
//	NewPipelineBuilder()             Engine.Run(ctx, store, turn)
//	  .WithSupervisor(sup)             |
//	  .RegisterNode("phase", fn)       |-- 1. FatalPreprocessHooks (error stops pipeline)
//	  .OnPreprocess(hook)              |-- 2. PreprocessHooks (errors logged, non-fatal)
//	  .OnPostprocess(hook)             |-- 3. store.AddMessage("user", text)
//	  .Build() -> Engine               |-- 4. executeLoop
//	                                   |      |-- Supervisor.DecideNextStep()
//	                                   |      |-- retryAndCommit(phase)
//	                                   |      |-- Apply NodeResult + StateDelta
//	                                   |      |-- EventWaitUser -> break
//	                                   |-- 5. PostprocessHooks
//	                                   |-- 6. return PipelineResult
//
// # Retry policy
//
// The engine supports an optional [RetryPolicy] that retries failed agents
// before propagating the error. Configure it via [PipelineBuilder.WithRetry].
// Zero value (no WithRetry call) means no retries — the agent runs exactly once.
package orchestrator
