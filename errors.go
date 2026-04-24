package orchestrator

// ErrorCategory groups error types by retry strategy. An ErrorClassifier maps
// concrete provider errors to one of these categories.
type ErrorCategory int

const (
	// CategoryUnknown is reserved for nil errors or unclassified cases.
	CategoryUnknown ErrorCategory = iota
	// CategoryPermanent — do not retry (auth failure, invalid input, context limit, …).
	CategoryPermanent
	// CategoryTransient — retry with standard backoff (timeouts, overloaded, transient network).
	CategoryTransient
	// CategoryRateLimit — retry with longer backoff (provider rate limit hit).
	CategoryRateLimit
)

func (c ErrorCategory) String() string {
	switch c {
	case CategoryPermanent:
		return "permanent"
	case CategoryTransient:
		return "transient"
	case CategoryRateLimit:
		return "rate_limit"
	default:
		return "unknown"
	}
}
