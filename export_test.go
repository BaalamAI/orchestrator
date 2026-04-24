package orchestrator

// This file lives in `package orchestrator` (internal) and is compiled only
// under `go test`. It exposes unexported Engine state to the external
// `orchestrator_test` package, so those tests can live outside the cycle that
// root-package tests would otherwise create when importing the store/supervisor
// subpackages (which themselves import the root package).
//
// Intentionally does NOT import any subpackage — that would reintroduce the
// cycle this refactor is fixing.

// ── Engine accessors for tests ──────────────────────────────────────

// MaxStepsForTest exposes the private maxSteps field.
func (e *Engine) MaxStepsForTest() int { return e.maxSteps }

// RepeatablePhaseForTest reports whether a phase is registered as repeatable.
func (e *Engine) RepeatablePhaseForTest(phase string) bool {
	return e.repeatablePhases[phase]
}

// FatalPreprocessHookCountForTest returns the number of fatal preprocess hooks.
func (e *Engine) FatalPreprocessHookCountForTest() int { return len(e.fatalPreprocessHooks) }

// PreprocessHookCountForTest returns the number of preprocess hooks.
func (e *Engine) PreprocessHookCountForTest() int { return len(e.preprocessHooks) }

// PostprocessHookCountForTest returns the number of postprocess hooks.
func (e *Engine) PostprocessHookCountForTest() int { return len(e.postprocessHooks) }

// LoggerForTest returns the configured logger (nil if none).
func (e *Engine) LoggerForTest() Logger { return e.logger }

// TracerIsSetForTest reports whether a tracer is wired.
func (e *Engine) TracerIsSetForTest() bool { return e.tracer != nil }

// MeterIsSetForTest reports whether a meter is wired.
func (e *Engine) MeterIsSetForTest() bool { return e.meter != nil }

// InstrumentsIsSetForTest reports whether the instrument set is wired.
func (e *Engine) InstrumentsIsSetForTest() bool { return e.instruments != nil }

// NodeConcurrentSafeForTest reports whether a registered node was marked as
// concurrency-safe.
func (e *Engine) NodeConcurrentSafeForTest(phase string) bool {
	entry, ok := e.nodeRegistry[phase]
	return ok && entry.concurrencySafe
}
