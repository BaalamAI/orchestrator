package orchestrator

import (
	"context"
	"fmt"
	"sync"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/sync/errgroup"
)

// executeParallelPhases runs concurrent phases in parallel, then sequential
// phases one-by-one. Results are merged: last non-empty answer wins, deltas
// are merged, usage is accumulated.
//
// Concurrent phases use computeNode (no store mutation) to avoid data races.
// The merged delta is committed to the store once after all concurrent phases complete.
func (e *Engine) executeParallelPhases(ctx context.Context, store StateStore, turn *Turn, phases []Phase, policy RetryPolicy, ledger *errorLedger, sharedCtx map[string]any) (*NodeResult, error) {
	concurrent, sequential, err := e.partitionPhases(phases)
	if err != nil {
		return nil, err
	}

	merged := &NodeResult{}
	var mu sync.Mutex

	if len(concurrent) > 0 {
		if err := e.runConcurrentPhases(ctx, store, turn, concurrent, policy, ledger, sharedCtx, merged, &mu); err != nil {
			return nil, err
		}
		// Apply merged delta from parallel phases to store (single-threaded)
		e.commitDelta(ctx, store, "parallel", &merged.Delta)
	}

	for _, phase := range sequential {
		nr, err := e.retryAndCommit(ctx, store, turn, phase, policy, ledger, sharedCtx)
		if err != nil {
			return nil, fmt.Errorf("error in %s agent: %w", phase, err)
		}
		mergeInto(merged, &mu, nr)
	}

	return merged, nil
}

// partitionPhases splits phases into concurrent and sequential groups based on
// the nodeRegistry concurrencySafe flag. Returns an error if any phase is unknown.
func (e *Engine) partitionPhases(phases []Phase) (concurrent, sequential []Phase, err error) {
	for _, phase := range phases {
		entry, ok := e.nodeRegistry[phase]
		if !ok {
			return nil, nil, fmt.Errorf("unknown phase: %s", phase)
		}
		if entry.concurrencySafe {
			concurrent = append(concurrent, phase)
		} else {
			sequential = append(sequential, phase)
		}
	}
	return concurrent, sequential, nil
}

// runConcurrentPhases fans out concurrent-safe phases with retryDeferred (no
// store mutation) and merges each result into `merged` under `mu`.
func (e *Engine) runConcurrentPhases(ctx context.Context, store StateStore, turn *Turn, phases []Phase, policy RetryPolicy, ledger *errorLedger, sharedCtx map[string]any, merged *NodeResult, mu *sync.Mutex) error {
	eg, gCtx := errgroup.WithContext(ctx)
	for _, phase := range phases {
		eg.Go(func() error {
			phaseCtx, span := e.tracer.Start(gCtx, "orchestrator.node.concurrent",
				trace.WithAttributes(attribute.String("phase", phase)),
			)
			defer span.End()

			nr, err := e.retryDeferred(phaseCtx, store, turn, phase, policy, ledger, sharedCtx)
			if err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
				return fmt.Errorf("error in %s agent: %w", phase, err)
			}
			mergeInto(merged, mu, nr)
			return nil
		})
	}
	return eg.Wait()
}

// mergeInto merges nr's answer, delta, usage, shared context, metadata, and event
// into merged under mu. Last non-empty answer wins; highest-priority event wins.
func mergeInto(merged *NodeResult, mu *sync.Mutex, nr *NodeResult) {
	mu.Lock()
	defer mu.Unlock()
	if nr.Answer != "" {
		merged.Answer = nr.Answer
	}
	merged.Delta.Merge(&nr.Delta)
	mergeUsage(merged, nr.Usage)
	mergeStringMap(&merged.SharedContext, nr.SharedContext)
	mergeStringMap(&merged.Metadata, nr.Metadata)
	if eventPriority(nr.Event) > eventPriority(merged.Event) {
		merged.Event = nr.Event
	}
}

// mergeUsage accumulates src usage into dst, allocating dst.Usage on first non-nil src.
func mergeUsage(dst *NodeResult, src *Usage) {
	if src == nil {
		return
	}
	if dst.Usage == nil {
		dst.Usage = &Usage{}
	}
	dst.Usage.Add(src)
}

// mergeStringMap copies src entries into *dst, lazily allocating *dst when src is non-empty.
func mergeStringMap(dst *map[string]any, src map[string]any) {
	for k, v := range src {
		if *dst == nil {
			*dst = make(map[string]any)
		}
		(*dst)[k] = v
	}
}

// eventPriority returns a numeric priority for event types (higher = takes precedence).
// Order: WaitUser > BudgetExceeded > ErrorThreshold > PhaseComplete > default.
// Used to pick the dominant event when merging results from parallel execution.
func eventPriority(e EventType) int {
	switch e {
	case EventWaitUser:
		return 4
	case EventBudgetExceeded:
		return 3
	case EventErrorThreshold:
		return 2
	case EventPhaseComplete:
		return 1
	default:
		return 0
	}
}

