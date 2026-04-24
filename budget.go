package orchestrator

// BudgetConfig defines token, cost, and error limits for a pipeline turn.
// Zero values mean no limit for that dimension.
type BudgetConfig struct {
	// MaxTokens is the maximum total tokens (prompt + completion) allowed per turn.
	MaxTokens int32
	// MaxCostUSD is the maximum estimated cost in USD allowed per turn.
	MaxCostUSD float64
	// MaxTransientErrors is the maximum number of transient errors accumulated
	// across all phases before the pipeline stops.
	MaxTransientErrors int
}

// budgetExceeded returns true if accumulated usage exceeds either token or cost limit.
func (e *Engine) budgetExceeded(result *PipelineResult, budget BudgetConfig) bool {
	if result.Usage == nil {
		return false
	}
	if budget.MaxTokens > 0 && result.Usage.TotalTokens >= budget.MaxTokens {
		return true
	}
	if budget.MaxCostUSD > 0 && e.estimateCost(result) >= budget.MaxCostUSD {
		return true
	}
	return false
}

// estimateCost calculates the accumulated USD cost from per-model breakdowns
// using the configured CostCalculator. Returns 0 when no calculator is wired
// (via the default noopCostCalculator) — in that case MaxCostUSD budget limits
// never trip.
func (e *Engine) estimateCost(result *PipelineResult) float64 {
	if result.Usage == nil {
		return 0
	}
	var total float64
	for _, mu := range result.Usage.Breakdown {
		total += e.cost.Calculate(mu.Model, int(mu.PromptTokens), int(mu.CompletionTokens))
	}
	return total
}
