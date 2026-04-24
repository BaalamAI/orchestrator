// Package orchestrator provides a generic framework for building multi-phase
// agent pipelines with a supervisor loop. It decouples routing logic (supervisor)
// from execution (agents), supports pre/post-processing hooks, and offers
// multiple supervision strategies (linear, state-machine with optional LLM routing).
//
// # Entry point
//
// The main entry point is [Engine.Run], which executes the full pipeline:
// preprocess hooks, supervisor-agent loop, and postprocess hooks.
//
// # File reading order
//
//  1. domain.go       — Start here: types and interfaces (Turn, NodeResult, StateStore, Supervisor)
//  2. engine.go       — Engine struct, Run(), hook types, retry/budget config
//  3. loop.go         — The supervisor-agent execution cycle
//  4. retry.go        — Retry logic and node execution (compute/commit)
//  5. parallel.go     — Parallel phase execution, budget, cost estimation
//  6. supervisor.go   — How the next phase is decided (StateMachineSupervisor, LinearSupervisor)
//  7. builder.go      — How an Engine is constructed (PipelineBuilder, fluent API)
//  8. teststore.go    — Only if writing tests: in-memory StateStore
//  9. adapters/       — Only if integrating with infrastructure (Redis, webhooks, hooks)
//
// # Runtime execution order
//
//	NewPipelineBuilder()             Engine.Run(ctx, store, turn)
//	  .WithSupervisor(sup)             |
//	  .RegisterAgent("phase", h)       |-- 1. FatalPreprocessHooks (error stops pipeline)
//	  .OnPreprocess(hook)              |-- 2. PreprocessHooks (errors logged, non-fatal)
//	  .OnPostprocess(hook)             |-- 3. store.AddMessage("user", text)
//	  .Build() -> Engine               |-- 4. executeLoop (max 3 iterations)
//	                                   |      |-- Supervisor.DecideNextStep()
//	                                   |      |-- retryAndCommit(phase)
//	                                   |      |     |-- attempt 1..MaxAttempts
//	                                   |      |     |-- warn log on each retry
//	                                   |      |     |-- ShouldRetry? → skip if false
//	                                   |      |     |-- ctx.Err()? → abort
//	                                   |      |-- Apply AgentResult + StateUpdates
//	                                   |      |-- EventWaitUser -> break
//	                                   |-- 5. PostprocessHooks
//	                                   |-- 6. return PipelineResult
//
// # Retry policy
//
// The engine supports an optional [RetryPolicy] that retries failed agents
// before propagating the error. Configure it via [PipelineBuilder.WithRetry]:
//
//	engine := NewPipelineBuilder().
//	    WithRetry(RetryPolicy{
//	        MaxAttempts: 3,
//	        ShouldRetry: func(err error) bool {
//	            // Custom predicate in addition to the ErrorClassifier.
//	            // The engine already short-circuits CategoryPermanent errors.
//	            return true
//	        },
//	    }).
//	    WithClassifier(myClassifier).
//	    ...
//
// Zero value (no WithRetry call) means no retries — the agent runs exactly once,
// preserving the original behavior.
//
// The retry loop checks ctx.Err() before each attempt, so context cancellation
// always takes priority. Intermediate failures are logged as warnings; only the
// final error (after all attempts) is logged as an error and returned.
//
// # Adapters subpackage
//
// The adapters/ subpackage contains infrastructure implementations:
// RedisStateAdapter (production StateStore), webhook conversion,
// built-in postprocess hooks (fallback, WhatsApp formatting, save response),
// and PipelineDefaults() for pre-configured builders.
package orchestrator
