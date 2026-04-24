package orchestrator

import (
	"context"
	"fmt"
	"sync"

	"golang.org/x/sync/errgroup"
)

// ComposeParallel returns a middleware that executes multiple providers in parallel
// before calling the next handler. It automatically merges their results into
// NodeInput, accumulates Usage, and merges Deltas into the final NodeResult.
func ComposeParallel(providers ...NodeProvider) AgentMiddleware {
	return func(next NodeFunc) NodeFunc {
		return func(ctx context.Context, view StateView, turn *Turn, input *NodeInput) (*NodeResult, error) {
			if len(providers) == 0 {
				return next(ctx, view, turn, input)
			}

			results, err := runProvidersParallel(ctx, view, turn, providers)
			if err != nil {
				return nil, err
			}

			delta, usage, metadata := aggregateProviderResults(input, results)

			finalResult, err := next(ctx, view, turn, input)
			if err != nil {
				return nil, err
			}

			mergeProviderIntoResult(finalResult, delta, usage, metadata)
			return finalResult, nil
		}
	}
}

// runProvidersParallel fans out providers across goroutines and collects their
// results positionally. Returns the first error from any provider.
func runProvidersParallel(ctx context.Context, view StateView, turn *Turn, providers []NodeProvider) ([]*ProviderResult, error) {
	g, gctx := errgroup.WithContext(ctx)
	results := make([]*ProviderResult, len(providers))
	var mu sync.Mutex

	for i, p := range providers {
		i, p := i, p
		g.Go(func() error {
			res, err := p.Provide(gctx, view, turn)
			if err != nil {
				return err
			}
			mu.Lock()
			results[i] = res
			mu.Unlock()
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, fmt.Errorf("parallel providers failed: %w", err)
	}
	return results, nil
}

// aggregateProviderResults injects each provider's value into NodeInput and
// accumulates deltas, usage and metadata across all results.
func aggregateProviderResults(input *NodeInput, results []*ProviderResult) (StateDelta, Usage, map[string]any) {
	if input.Extra == nil {
		input.Extra = make(map[string]any)
	}

	var delta StateDelta
	var usage Usage
	metadata := make(map[string]any)

	for _, res := range results {
		if res == nil {
			continue
		}
		injectProviderValue(input, res)
		if res.Delta != nil {
			delta.Merge(res.Delta)
		}
		if res.Usage != nil {
			usage.Add(res.Usage)
		}
		for k, v := range res.Metadata {
			metadata[k] = v
		}
	}
	return delta, usage, metadata
}

// injectProviderValue routes a provider's Value into the specialized NodeInput
// field (RAGContext, CapturedFields) or falls back to the Extra bag.
func injectProviderValue(input *NodeInput, res *ProviderResult) {
	switch res.InputKey {
	case "rag_context":
		if s, ok := res.Value.(string); ok {
			input.RAGContext = s
		}
	case "captured_fields":
		if m, ok := res.Value.(map[string]any); ok {
			input.CapturedFields = m
		}
	default:
		if res.InputKey != "" {
			input.Extra[res.InputKey] = res.Value
		}
	}
}

// mergeProviderIntoResult merges accumulated provider delta/usage/metadata into
// the agent's final result. Existing metadata keys on finalResult win over
// provider metadata; delta and usage are combined additively.
func mergeProviderIntoResult(finalResult *NodeResult, delta StateDelta, usage Usage, metadata map[string]any) {
	delta.Merge(&finalResult.Delta)
	finalResult.Delta = delta

	if finalResult.Usage == nil {
		finalResult.Usage = &Usage{}
	}
	finalResult.Usage.Add(&usage)

	if finalResult.Metadata == nil {
		finalResult.Metadata = make(map[string]any)
	}
	for k, v := range metadata {
		if _, exists := finalResult.Metadata[k]; !exists {
			finalResult.Metadata[k] = v
		}
	}
}
