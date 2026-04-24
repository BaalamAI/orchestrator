package hook

import (
	"context"
	"fmt"
	"sync"

	"github.com/baalamai/orchestrator"
	"golang.org/x/sync/errgroup"
)

// ComposeParallel returns a middleware that executes multiple providers in parallel
// before calling the next handler. It automatically merges their results into
// NodeInput, accumulates Usage, and merges Deltas into the final NodeResult.
func ComposeParallel(providers ...orchestrator.NodeProvider) orchestrator.AgentMiddleware {
	return func(next orchestrator.NodeFunc) orchestrator.NodeFunc {
		return func(ctx context.Context, view orchestrator.StateView, turn *orchestrator.Turn, input *orchestrator.NodeInput) (*orchestrator.NodeResult, error) {
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

func runProvidersParallel(ctx context.Context, view orchestrator.StateView, turn *orchestrator.Turn, providers []orchestrator.NodeProvider) ([]*orchestrator.ProviderResult, error) {
	g, gctx := errgroup.WithContext(ctx)
	results := make([]*orchestrator.ProviderResult, len(providers))
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

func aggregateProviderResults(input *orchestrator.NodeInput, results []*orchestrator.ProviderResult) (orchestrator.StateDelta, orchestrator.Usage, map[string]any) {
	if input.Extra == nil {
		input.Extra = make(map[string]any)
	}

	var delta orchestrator.StateDelta
	var usage orchestrator.Usage
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

func injectProviderValue(input *orchestrator.NodeInput, res *orchestrator.ProviderResult) {
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

func mergeProviderIntoResult(finalResult *orchestrator.NodeResult, delta orchestrator.StateDelta, usage orchestrator.Usage, metadata map[string]any) {
	delta.Merge(&finalResult.Delta)
	finalResult.Delta = delta

	if finalResult.Usage == nil {
		finalResult.Usage = &orchestrator.Usage{}
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
