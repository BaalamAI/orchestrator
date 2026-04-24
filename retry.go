package orchestrator

import (
	"context"
	"fmt"
)

// ── Node execution modes ────────────────────────────────────────────
// computeNode runs an agent purely (no state mutation) — safe for parallel execution.
// commitNode runs an agent and persists its delta to the store — used for sequential phases.
// retryDeferred / retryAndCommit wrap these with the retry loop.

// retryDeferred retries computeNode (no store mutation).
// Used by executeParallelPhases to avoid data races during concurrent execution.
func (e *Engine) retryDeferred(ctx context.Context, store StateStore, turn *Turn, phase Phase, policy RetryPolicy, ledger *errorLedger, sharedCtx map[string]any) (*NodeResult, error) {
	return e.retryLoop(ctx, store, turn, phase, policy, ledger, sharedCtx, e.computeNode)
}

// retryAndCommit retries commitNode for the given phase. Errors are recorded
// in the ledger for cross-phase tracking.
func (e *Engine) retryAndCommit(ctx context.Context, store StateStore, turn *Turn, phase Phase, policy RetryPolicy, ledger *errorLedger, sharedCtx map[string]any) (*NodeResult, error) {
	return e.retryLoop(ctx, store, turn, phase, policy, ledger, sharedCtx, e.commitNode)
}

// nodeExecutor is the function signature for computeNode and commitNode.
type nodeExecutor func(ctx context.Context, store StateStore, turn *Turn, phase Phase, entry registeredNode, sharedCtx map[string]any) (*NodeResult, error)

// retryLoop is the shared retry implementation. The executor parameter determines
// whether state mutations are applied immediately (commitNode) or deferred (computeNode).
func (e *Engine) retryLoop(ctx context.Context, store StateStore, turn *Turn, phase Phase, policy RetryPolicy, ledger *errorLedger, sharedCtx map[string]any, executor nodeExecutor) (*NodeResult, error) {
	entry, ok := e.nodeRegistry[phase]
	if !ok {
		return nil, fmt.Errorf("unknown phase: %s", phase)
	}

	maxAttempts := policy.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = 1
	}

	var lastErr error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		nodeResult, err := executor(ctx, store, turn, phase, entry, sharedCtx)
		if err == nil {
			return nodeResult, nil
		}

		lastErr = err
		ledger.record(err)

		// Permanent errors (auth, invalid input, context limit, circuit breaker open, …)
		// short-circuit retries. The classifier owns this decision; swap in a custom
		// ErrorClassifier to change which errors are treated as permanent.
		if e.classifier.Classify(err) == CategoryPermanent {
			e.logWarn(ctx, "Permanent error, skipping retries", "phase", phase, "error", err)
			break
		}

		if policy.ShouldRetry != nil && !policy.ShouldRetry(err) {
			break
		}

		if attempt < maxAttempts {
			e.logWarn(ctx, "Agent error, retrying", "phase", phase, "attempt", attempt, "max_attempts", maxAttempts, "error", err)
		}
	}

	// All retries exhausted — try fallback if configured
	if policy.OnRetryExhausted != nil {
		fallback, fbErr := policy.OnRetryExhausted(phase, lastErr)
		if fbErr != nil {
			return nil, fbErr
		}
		if fallback != "" {
			e.logInfo(ctx, "Retry exhausted, falling back", "from", phase, "to", fallback)
			fbEntry, ok := e.nodeRegistry[fallback]
			if !ok {
				return nil, fmt.Errorf("unknown fallback phase: %s", fallback)
			}
			return executor(ctx, store, turn, fallback, fbEntry, sharedCtx)
		}
	}

	return nil, lastErr
}

// commitNode runs a pure-function agent with its middleware chain, pre/post hooks,
// and persists the resulting delta to the store.
func (e *Engine) commitNode(ctx context.Context, store StateStore, turn *Turn, phase Phase, entry registeredNode, sharedCtx map[string]any) (*NodeResult, error) {
	nodeResult, err := e.computeNode(ctx, store, turn, phase, entry, sharedCtx)
	if err != nil {
		return nil, err
	}

	// Persist delta — only the engine mutates state
	e.commitDelta(ctx, store, phase, &nodeResult.Delta)

	return nodeResult, nil
}

// computeNode executes a pure-function agent with its middleware chain and pre/post
// hooks but does NOT persist the delta. Used by executeParallelPhases to defer
// state mutations until after all concurrent phases complete.
func (e *Engine) computeNode(ctx context.Context, store StateStore, turn *Turn, phase Phase, entry registeredNode, sharedCtx map[string]any) (*NodeResult, error) {
	snapshot := NewSnapshot(store)
	input := &NodeInput{Extra: make(map[string]any), SharedContext: sharedCtx}

	// Pre-agent hooks
	for _, hook := range e.preAgentHooks {
		if err := hook(ctx, snapshot, turn, phase); err != nil {
			e.logError(ctx, "Pre-agent hook error", "phase", phase, "error", err)
		}
	}

	// Build middleware chain (wrap from last to first)
	chain := entry.handler
	for i := len(entry.middleware) - 1; i >= 0; i-- {
		chain = entry.middleware[i](chain)
	}

	// Execute through middleware chain
	nodeResult, err := chain(ctx, snapshot, turn, input)
	if err != nil {
		return nil, err
	}

	// Post-agent hooks
	for _, hook := range e.postAgentHooks {
		if err := hook(ctx, snapshot, turn, nodeResult, phase); err != nil {
			e.logError(ctx, "Post-agent hook error", "phase", phase, "error", err)
		}
	}

	return nodeResult, nil
}

// commitDelta writes delta mutations to the store and persists.
func (e *Engine) commitDelta(ctx context.Context, store StateStore, phase string, delta *StateDelta) {
	if delta.Updates != nil {
		for k, v := range delta.Updates {
			store.SetState(k, v)
		}
	}
	for _, k := range delta.Deletes {
		store.SetState(k, nil)
	}
	if delta.Updates != nil || len(delta.Deletes) > 0 {
		if err := store.Save(); err != nil {
			e.logError(ctx, "Checkpoint save error after node", "phase", phase, "error", err)
		}
	}
}
