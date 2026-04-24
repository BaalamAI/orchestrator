package orchestrator

// ErrorClassifier maps provider-specific errors to orchestrator retry categories.
// Implementations inspect errors returned by LLM SDKs (OpenAI, Anthropic, Google, …)
// or downstream libraries and return the category that best describes them,
// letting the engine decide whether to retry.
//
// Callers wire a concrete classifier via WithClassifier. Without one, the engine
// uses noopClassifier, which treats every non-nil error as CategoryTransient —
// making RetryPolicy retry unknown failures by default. Pass a real classifier
// (or a custom ShouldRetry predicate) to short-circuit specific error types.
type ErrorClassifier interface {
	Classify(err error) ErrorCategory
}

// CostCalculator computes USD cost from per-call token usage for budget enforcement.
// Implementations own the pricing table for the models they support.
//
// Callers wire a concrete calculator via WithCostCalculator. Without one, the
// engine uses noopCostCalculator, which returns 0 — cost-based budget limits
// (BudgetConfig.MaxCostUSD) never trip unless a real calculator is supplied.
type CostCalculator interface {
	Calculate(model string, promptTokens, completionTokens int) float64
}

type noopClassifier struct{}

func (noopClassifier) Classify(err error) ErrorCategory {
	if err == nil {
		return CategoryUnknown
	}
	return CategoryTransient
}

type noopCostCalculator struct{}

func (noopCostCalculator) Calculate(string, int, int) float64 { return 0 }
