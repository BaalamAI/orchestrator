# Orchestrator

Framework genérico para pipelines de agentes con supervisor loop. Desacopla el **routing** (supervisor) de la **ejecución** (agentes) y conecta ambos mediante **hooks** reutilizables.

```
┌─────────────────────────────────────────────────────────────────────┐
│                github.com/baalamai/orchestrator                     │
│                                                                     │
│   ROUTING            EJECUCIÓN              PERSISTENCIA           │
│   ────────           ──────────             ─────────────          │
│   Supervisor  ──▶   AgentHandler   ──▶     StateStore             │
│   (decide fase)     NodeFunc + MW           (impl. del consumidor) │
│                         │                                           │
│                     PipelineResult                                  │
│                     {Answer, Usage, Metadata}                       │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Índice

1. [Flujo de ejecución](#flujo-de-ejecución)
2. [Tipos de agentes](#tipos-de-agentes)
3. [Supervisores](#supervisores)
4. [Hooks](#hooks)
5. [Middleware (NodeFunc)](#middleware-nodefunc)
6. [Ejecución paralela](#ejecución-paralela)
7. [ToolLoop (ReAct)](#toolloop-react)
8. [State Persistence](#state-persistence)
9. [Checkpoint / Resume](#checkpoint--resume)
10. [Budget y Retry](#budget-y-retry)
11. [Usage Tracking](#usage-tracking)
12. [Observabilidad (OpenTelemetry)](#observabilidad-opentelemetry)
13. [Quick Start](#quick-start)
14. [Estructura de archivos](#estructura-de-archivos)
15. [API de referencia](#api-de-referencia)

---

## Flujo de ejecución

```
Engine.Run(ctx, store, turn)
│
├─ [1] FatalPreprocessHooks ─────────── error → detiene pipeline, retorna error
│
├─ [2] PreprocessHooks ──────────────── error → se loguea, continúa
│
├─ [3] Checkpoint resume (si Turn.TurnID existe) ─── o ── store.AddMessage("user", turn.Text)
│
└─ [4] executeLoop (máx N pasos, default=3)
        │
        ├── decidePhases(store, turn.Text, currentPhase, lastEvent) → fases
        │
        ├── Anti-stutter: misma fase + sin PhaseComplete + no Repeatable → break
        │
        ├── runPhases(fases) ── single o parallel según supervisor
        │       │
        │       ├── retryLoop con RetryPolicy (MaxAttempts)
        │       │       │
        │       │       └── computeNode / commitNode (según concurrent-safe)
        │       │             │
        │       │             ├── snapshot = NewSnapshot(store)
        │       │             ├── PreAgentHooks
        │       │             ├── middleware chain (mw1 → mw2 → agente)
        │       │             ├── PostAgentHooks
        │       │             └── commitDelta → store.Save()
        │       │
        ├── propagateNodeOutput → result.Answer, result.Usage, result.Metadata
        │
        ├── saveCheckpoint (fire-and-forget)
        │
        └── stopEvent:
                EventWaitUser       → break (responde al usuario)
                EventPhaseComplete → continúa (cascada a siguiente fase)
                EventBudgetExceeded → break (presupuesto agotado)
                EventErrorThreshold  → break (demasiados errores transitorios)
                maxSteps          → break

├─ [5] PostprocessHooks(store, turn, result)
│
└─ [6] ClearCheckpoint (on success)
```

**PipelineResult.Metadata** contiene siempre al finalizar:

| Clave | Constante | Valor |
|-------|-----------|-------|
| `"final_phase"` | `MetaFinalPhase` | Última fase ejecutada |
| `"final_event"` | `MetaFinalEvent` | Último evento (`wait_user`, etc.) |
| `"total_steps"` | `MetaTotalSteps` | Número de iteraciones del loop |
| `"total_cost_usd"` | `MetaTotalCostUSD` | Costo acumulado en USD |
| `"budget_exceeded"` | `MetaBudgetExceeded` | true si se detuvo por presupuesto |
| `"error_threshold"` | `MetaErrorThreshold` | true si se detuvo por error threshold |
| `"error_ledger"` | `MetaErrorLedger` | map[ErrorCategory]int con errores acumulados |

---

## Tipos de agentes

El framework soporta dos firmas de agente:

```
AgentHandler (mutable)        NodeFunc (puro / funcional)
───────────────────────      ────────────────────────────
func(ctx, StateStore, *Turn)  func(ctx, StateView, *Turn, *NodeInput)
    *AgentResult                 *NodeResult

Recibe StateStore mutable    Recibe StateView (snapshot read-only)
Puede mutar estado          Retorna StateDelta con los cambios
directamente               El engine aplica el delta
                           Soporta cadena de AgentMiddleware
```

### Cuándo usar cada uno

| Situación | Tipo recomendado |
|-----------|---------------|
| Agente nuevo con RAG o captura de campos | `NodeFunc` + middlewares |
| Agente existente sin refactorizar | `AgentHandler` |
| Tests con aislamiento de estado | `NodeFunc` (snapshot evita side-effects) |
| Necesita mutar estado inmediatamente | `AgentHandler` |

### StateView — interfaz de solo lectura

Los `NodeFunc` reciben un **snapshot** del estado al momento de la llamada:

```go
type StateView interface {
    Get(key string) (any, bool)    // valor + existe?
    GetString(key string) string   // conversión segura a string
    GetBool(key string) bool        // conversión segura a bool
    HasFlag(key string) bool       // true si el valor es bool(true)
    State() map[string]any        // copia del estado completo
    Messages() []Message          // copia del historial
    Memory() map[string]any     // memoria cross-sesión
}
```

### StateDelta — mutaciones declarativas

```go
type StateDelta struct {
    Updates map[string]any  // claves a escribir / sobrescribir
    Deletes []string        // claves a eliminar
}

func (d *StateDelta) Merge(other *StateDelta)  // other gana en conflicto
```

### NodeInput — contexto pre-computado por middlewares

```go
type NodeInput struct {
    CapturedFields map[string]any  // campos extraídos por capture middleware
    RAGContext     string        // contexto de conocimiento inyectado por RAG middleware
    Extra          map[string]any  // bag extensible para providers custom
    SharedContext map[string]any  // contexto cross-fases acumulado
}
```

### Constructores de resultado

```go
// AgentHandler
WaitUser(answer)                          // EventWaitUser
StepSuccess(answer)                       // EventStepSuccess
PhaseComplete(answer)                     // EventPhaseComplete
PhaseCompleteWithState(answer, updates)   // EventPhaseComplete + StateUpdates

// NodeFunc
return &NodeResult{
    Event:  EventWaitUser,
    Answer: "respuesta",
    Delta:  StateDelta{Updates: map[string]any{"k": "v"}, Deletes: []string{"old"}},
    Usage:  usage,
}
```

---

## Supervisores

El supervisor decide **qué fase ejecutar** en cada iteraci��n del loop.

### LinearSupervisor

Ejecuta fases en secuencia fija. Avanza al recibir `EventPhaseComplete`.

```
phases = ["intake", "process", "respond"]

paso 1: intake    ─── PhaseComplete ──▶  paso 2: process
paso 2: process   ─── PhaseComplete ──▶  paso 3: respond
paso 3: respond   ─── WaitUser ─────▶   fin (responde al usuario)
```

```go
sup := supervisor.NewLinear("intake", "process", "respond")
```

### StateMachineSupervisor

Combina cuatro mecanismos de decisión en orden de prioridad:

```
DecideNextStep(currentPhase, lastEvent)
│
├─ [P1] lastEvent == PhaseComplete?
│        │
│        ├── ConditionalTransitions: From==current && Condition(store)==true  → fase
│        └── Transitions:           From==current                            → fase
│
├─ [P2] currentPhase == "" (inicio) && hay Router?
│        │
│        ├── Router.Route(store, text) (LLM clasifica intención)             → fase
│        └── Router falla → cae a P3
│
├─ [P3] FlagRules (recorre en orden, primer match gana)
│        Flag "register_complete" → "complete"
│        …
│
└─ [P4] DefaultPhase                                                         → fase
```

> **Nota:** El Router LLM solo se consulta al inicio del turno (`currentPhase == ""`). Durante el loop usa exclusivamente reglas deterministas.

```go
sup := &supervisor.StateMachine{
    DefaultPhase: "diagnostic",

    ConditionalTransitions: []supervisor.ConditionalTransition{
        {From: "payment", To: "premium", Condition: func(s orchestrator.StateStore) bool {
            return s.HasFlag("is_premium_plan")
        }},
    },

    Transitions: []supervisor.TransitionRule{
        {From: "diagnostic", To: "payment"},
        {From: "payment",    To: "register"},
        {From: "register",   To: "complete"},
    },

    FlagRules: []supervisor.FlagRule{
        {Flag: "register_complete", Phase: "complete"},
    },

    Router: myIntentRouter, // opcional
}
```

### ParallelSupervisor

Un supervisor puede implementar `ParallelSupervisor` para devolver múltiples fases. El engine particiona en concurrentes y secuenciales:

```go
type ParallelSupervisor interface {
    Supervisor
    DecideNextSteps(ctx, store, text, phase, event) ([]Phase, reason, error)
}
```

Fases marcadas con `RegisterConcurrentNode` usan `computeNode` (sin mutación) y se ejecutan en paralelo. Las demás se ejecutan secuencialmente después.

---

## Hooks

Hay seis tipos de hooks. Se registran en el builder.

```
Orden de ejecución
─────────────────
[1] FatalPreprocessHook   ← error detiene pipeline
[2] PreprocessHook       ← error solo se loguea
[3] (checkpoint resume o store.AddMessage)
[4] loop:
      SupervisorHook      ← fire-and-forget (goroutine)
      PreAgentHook       ← solo para NodeFunc
      (ejecuta agente)
      PostAgentHook     ← solo para NodeFunc
[5] PostprocessHook      ← error solo se loguea
[6] ClearCheckpoint   ← fire-and-forget
```

### Firmas

```go
type FatalPreprocessHook func(ctx, store, *Turn) error
type PreprocessHook    func(ctx, store, *Turn) error
type SupervisorHook   func(ctx, store, *Turn, step int, phase Phase, reason string, usage *Usage) error
type PreAgentHook    func(ctx, view StateView, *Turn, phase string) error
type PostAgentHook  func(ctx, view StateView, *Turn, result *NodeResult, phase string) error
type PostprocessHook func(ctx, store, *Turn, *PipelineResult) error
```

### Registro en el builder

```go
engine := orchestrator.NewPipelineBuilder().
    WithSupervisor(sup).
    RegisterNode("main", handler).

    OnFatalPreprocess(authHook).
    OnPreprocess(metricsHook, enrichHook).
    OnSupervisorDecision(auditHook).
    OnPreAgent(validationHook).
    OnPostAgent(loggingHook).
    OnPostprocess(fallback, formatHook).

    MustBuild()
```

---

## Middleware (NodeFunc)

Los middlewares solo aplican a agentes registrados con `RegisterNode`. Siguen el patrón classicode "onion layers": el primer middleware es la capa más externa.

```
RegisterNode("fase", agente, mw1, mw2)

Orden de llamada:
┌─────────────────────────────────────────┐
│ mw1                                     │
│   ┌─────────────────────────────────┐   │
│   │ mw2                             │   │
│   │   ┌─────────────────────────┐   │   │
│   │   │ agente (NodeFunc)       │   │   │
│   │   └─────────────────────────┘   │   │
│   └─────────────────────────────────┘   │
└─────────────────────────────────────────┘
```

### Ejemplo: captura de campos + RAG

```go
captureMW := func(next orchestrator.NodeFunc) orchestrator.NodeFunc {
    return func(ctx context.Context, view orchestrator.StateView, turn *orchestrator.Turn, input *orchestrator.NodeInput) (*orchestrator.NodeResult, error) {
        input.CapturedFields = map[string]any{
            "product": extractProduct(turn.Text),
        }
        return next(ctx, view, turn, input)
    }
}

ragMW := func(next orchestrator.NodeFunc) orchestrator.NodeFunc {
    return func(ctx context.Context, view orchestrator.StateView, turn *orchestrator.Turn, input *orchestrator.NodeInput) (*orchestrator.NodeResult, error) {
        input.RAGContext = ragEngine.Search(ctx, turn.Text)
        return next(ctx, view, turn, input)
    }
}

agente := func(ctx context.Context, view orchestrator.StateView, turn *orchestrator.Turn, input *orchestrator.NodeInput) (*orchestrator.NodeResult, error) {
    product := input.CapturedFields["product"].(string)
    context := input.RAGContext

    return &orchestrator.NodeResult{
        Event:  orchestrator.EventWaitUser,
        Answer: generate(product, context),
        Delta: orchestrator.StateDelta{
            Updates: map[string]any{"last_product": product},
        },
    }, nil
}

builder.RegisterNode("diagnostic", agente, captureMW, ragMW)
```

---

## Ejecución paralela

Cuando el supervisor devuelve múltiples fases, el engine las particiona:

1. **Concurrent-safe** (`RegisterConcurrentNode`): ejecución en paralelo con `computeNode` (sin mutar estado)
2. **Secuenciales**: ejecución uno por uno con `commitNode` (mutan estado)

Los resultados se fusionan:
- `Answer`: el último no-vacío gana
- `Delta`: se mergean (later wins)
- `Usage`: se acumula
- `Event`: prioridad (WaitUser > BudgetExceeded > ErrorThreshold > PhaseComplete)

```go
builder.RegisterConcurrentNode("parallel_lookup", lookupAgent)
builder.RegisterNode("sequential_process", processAgent)
```

---

## ToolLoop (ReAct)

El subpaquete `llm` provee un `NodeFunc` que implementa un ReAct-style loop:

```
1. Call LLM con messages + tools
2. If tool_use → invoke tools → append results → loop
3. If end_turn → return answer
```

### Uso

```go
toolLoop := llm.NewToolLoopNode(client, llm.ToolLoopOptions{
    Model:    "gemini-2.0-flash",
    SystemPrompt: func(view orchestrator.StateView, turn *orchestrator.Turn) string {
        return "Eres un asistente médico..."
    },
    Tools: []llm.Tool{lookupTool, scheduleTool},
    MaxIterations: 5,
})

engine.RegisterNode("diagnostic", toolLoop)
```

### Options

| Campo | Default | Descripción |
|-------|---------|-------------|
| `Model` | - | Identificador del modelo |
| `SystemPrompt` | "" | Prompt del sistema |
| `Tools` | [] | Herramientas disponibles |
| `MaxIterations` | 8 | Límite del loop interno |
| `Temperature` | 0 | Temperatura de sampling |
| `MaxTokens` | 0 | Máx tokens de respuesta |
| `EventOnComplete` | EventWaitUser |.Event al terminar |
| `OnToolError` | ToolErrorReport | Política de errores |

---

## State Persistence

El engine persiste estado después de cada fase:

```
NodeResult.Delta  /  AgentResult.StateUpdates
            │
            ▼
     Engine (único que escribe):
       commitDelta → store.SetState(k, v) → store.Save()
```

**Principio:** los agentes retornan *qué cambió*; el engine es el único que persiste.

### StateStore — interfaz

```go
type StateStore interface {
    State() map[string]any
    SetState(key string, value any)
    HasFlag(key string) bool
    Messages() []Message
    AddMessage(role, text string) error
    Save() error
}
```

| Implementación | Cuándo usarla |
|---------------|--------------|
| `store.Memory` | Tests, ejemplos |
| RedisStateAdapter | Producción (en consumidor) |

---

## Checkpoint / Resume

El engine soporta **checkpoint mid-turn**:

```go
cfg := orchestrator.PipelineConfig{
    Checkpoints: myCheckpointStore,
}

turn := orchestrator.NewTurn(convID, text)
turn.TurnID = interactionID  // ID estable para resume
```

### Semántica

- **Save**: después de cada step exitoso, fire-and-forget
- **Resume**: en `Run()`, si TurnID + Checkpoint existen:
  - Restaura `phase`, `lastEvent`, `steps`, `sharedContext`, `Usage`
  - **Salta** `store.AddMessage("user")` (ya en store)
  - **Re-ejecuta** hooks preprocess (idempotentes)
- **Clear**: en completion normal

### Contrato de idempotencia

⚠️ Tools con side-effects visibles (enviar mensaje, cobrar tarjeta) **deben** deduplicar por natural key.

---

## Budget y Retry

### RetryPolicy

```go
engine := orchestrator.NewPipelineBuilder().
    WithRetry(orchestrator.RetryPolicy{
        MaxAttempts: 3,
        ShouldRetry: func(err error) bool {
            return !strings.Contains(err.Error(), "invalid input")
        },
        OnRetryExhausted: func(phase orchestrator.Phase, err error) (orchestrator.Phase, error) {
            return "fallback", nil  // redirige a fallback
        },
    })
```

Errores clasificados por `ErrorClassifier`:
- `CategoryPermanent`: short-circuits retries
- `CategoryTransient`, `CategoryRateLimit`: sujetas a retry

### BudgetConfig

```go
engine := orchestrator.NewPipelineBuilder().
    WithBudget(orchestrator.BudgetConfig{
        MaxTokens:        10000,
        MaxCostUSD:       0.50,
        MaxTransientErrors: 3,
    })
```

---

## Usage Tracking

Tokens se acumulan de cada fuente:

```
Supervisor.Usage()      ──────────────────────┐
NodeFunc.Usage()        ──────────────────┐   │
llm.Tool.Usage()       ────────────────┐   │   │
                                     ▼   ▼
                         PipelineResult.Usage
```

### ModelUsage

```go
type ModelUsage struct {
    Agent            string  // "router", "diagnostic", …
    Model            string  // "gemini-2.0-flash", …
    Provider         string  // "google", "anthropic", …
    PromptTokens     int32
    CompletionTokens int32
    TotalTokens      int32
}
```

---

## Observabilidad (OpenTelemetry)

El engine emite **spans** y **métricas** cuando se configura `Tracer` y `Meter`:

```go
cfg := orchestrator.PipelineConfig{
    Tracer: otel.Tracer("orchestrator"),
    Meter:  otel.Meter("orchestrator"),
}
```

### Jerarquía de spans

```
orchestrator.turn
├── orchestrator.supervisor
├── orchestrator.node
│   ├── orchestrator.node.attempt
│   └── orchestrator.node.concurrent  ← solo en paralelo
├── orchestrator.llm
└── orchestrator.tool
```

### Métricas

| Nombre | Tipo | Labels |
|--------|------|--------|
| `orchestrator.tokens` | Int64Counter | `phase`, `model`, `provider`, `kind` |
| `orchestrator.cost_usd` | Float64Counter | `phase`, `model` |
| `orchestrator.phase.duration_ms` | Float64Histogram | `phase`, `outcome` |
| `orchestrator.llm.duration_ms` | Float64Histogram | `model`, `provider` |
| `orchestrator.tool.duration_ms` | Float64Histogram | `tool` |
| `orchestrator.tool.calls` | Int64Counter | `tool`, `outcome` |
| `orchestrator.retries` | Int64Counter | `phase`, `category` |
| `orchestrator.checkpoint` | Int64Counter | `event` |

---

## Quick Start

### Ejemplo 1: Linear con dos fases

```go
engine := orchestrator.NewPipelineBuilder().
    WithSupervisor(supervisor.NewLinear("greet", "farewell")).
    RegisterNode("greet", func(ctx context.Context, view orchestrator.StateView, turn *orchestrator.Turn, input *orchestrator.NodeInput) (*orchestrator.NodeResult, error) {
        return &orchestrator.NodeResult{
            Event:  orchestrator.EventPhaseComplete,
            Answer: "Hola " + turn.Text + "!",
        }, nil
    }).
    RegisterNode("farewell", func(ctx context.Context, view orchestrator.StateView, turn *orchestrator.Turn, input *orchestrator.NodeInput) (*orchestrator.NodeResult, error) {
        return &orchestrator.NodeResult{
            Event:  orchestrator.EventWaitUser,
            Answer: "¿En qué más te puedo ayudar?",
        }, nil
    }).
    MustBuild()

store := store.NewMemory()
result, _ := engine.Run(context.Background(), store, orchestrator.NewTurn("conv-1", "mundo"))
fmt.Println(result.Answer) // "¿En qué más te puedo ayudar?"
```

### Ejemplo 2: StateMachine con flags

```go
engine := orchestrator.NewPipelineBuilder().
    WithSupervisor(&supervisor.StateMachine{
        DefaultPhase: "diagnostic",
        Transitions:  []supervisor.TransitionRule{{From: "diagnostic", To: "payment"}},
        FlagRules:    []supervisor.FlagRule{{Flag: "register_complete", Phase: "complete"}},
    }).
    RegisterNode("diagnostic", diagnosticAgent, ragMiddleware).
    RegisterNode("payment", paymentAgent).
    RegisterNode("complete", completeAgent).
    MustBuild()
```

### Ejemplo 3: ToolLoop

```go
engine := orchestrator.NewPipelineBuilder().
    WithSupervisor(supervisor.NewLinear("intake", "lookup", "respond")).
    RegisterNode("intake", intakeAgent).
    RegisterNode("lookup", llm.NewToolLoopNode(client, llm.ToolLoopOptions{
        Model:    "gemini-2.0-flash",
        SystemPrompt: func(view orchestrator.StateView, turn *orchestrator.Turn) string { return "Busca información..." },
        Tools: []llm.Tool{searchTool},
    })).
    RegisterNode("respond", respondAgent).
    MustBuild()
```

---

## Estructura de archivos

```
lib/orchestrator/
├── doc.go                ← Package doc (go doc)
├── domain.go             ← Tipos centrales: Turn, Message, Usage, EventType,
│                           StateView, StateDelta, NodeFunc, NodeInput, NodeResult
├── ports.go             ← Interfaces: StateStore, Supervisor, ParallelSupervisor,
│                           IntentRouter, ErrorClassifier, CostCalculator
├── builder.go          ← PipelineBuilder (API fluent)
├── run.go             ← Engine.Run(), checkpoint resume logic
├── loop.go            ← Execution loop: executeLoop, decidePhases, runPhases
├── retry.go           ← retryLoop, computeNode, commitNode, commitDelta
├── parallel.go        ← executeParallelPhases, partición concurrent/secuencial
├── checkpoint.go      ← Checkpoint, CheckpointStore
├── hooks.go           ← Hook signatures
├── orchestrator.go    ← Engine struct, RetryPolicy, errorLedger
├── config.go         ← PipelineConfig (alternativa declarativa)
├── metrics.go        ← OpenTelemetry instruments
├── errors.go         ← ErrorCategory
├── budget.go        ← BudgetConfig
├── snapshot.go       ← NewSnapshot() → StateView
│
├── supervisor/
│   ├── linear.go         ← LinearSupervisor
│   ├── statemachine.go   ← StateMachineSupervisor
│   └── ...
│
├── store/
│   ├── memory.go       ← store.Memory (in-memory StateStore)
│   ├── checkpoint.go  ← store.MemoryCheckpoint (in-memory)
│   └── ...
│
├── llm/
│   ├── toolloop.go    ← NewToolLoopNode (ReAct loop)
│   ├── ports.go      ← Client, Tool, ToolDefinition interfaces
│   └── ...
│
├── hook/
│   ├── hook.go
│   └── middleware.go
│
└── obs/
    └── obs.go         ← Observability context pass-through
```

### Orden de lectura (primera vez)

| # | Archivo | Qué encontrarás |
|---|---------|---------------|
| 1 | `domain.go` | **Empieza aquí.** Tipos centrales |
| 2 | `builder.go` | API del builder |
| 3 | `run.go` + `loop.go` | `Engine.Run()` y execution loop |
| 4 | `supervisor/statemachine.go` | Lógica de routing |
| 5 | `retry.go` | Retry loop y commit |
| 6 | `parallel.go` | Ejecución paralela |
| 7 | `llm/toolloop.go` | ReAct con tools |
| 8 | `store/memory.go` | Implementación in-memory |

---

## API de referencia

### Builder

| Método | Descripción |
|--------|-----------|
| `NewPipelineBuilder()` | Builder con defaults (`maxSteps=3`) |
| `.WithSupervisor(s)` | **Requerido.** Supervisor |
| `.RegisterNode(fase, fn, mw...)` | Registra `NodeFunc` con middlewares |
| `.RegisterConcurrentNode(fase, fn, mw...)` | Marca como safe para paralelo |
| `.MaxSteps(n)` | Máximo iteraciones (default: 3) |
| `.Repeatable(fases...)` | Fases que pueden re-entrar sin PhaseComplete |
| `.WithRetry(policy)` | RetryPolicy |
| `.WithBudget(budget)` | BudgetConfig |
| `.WithLogger(l)` | Logger |
| `.WithClassifier(c)` | ErrorClassifier |
| `.WithTracer(t)` | OpenTelemetry tracer |
| `.WithMeter(m)` | OpenTelemetry meter |
| `.WithCheckpoints(s)` | CheckpointStore |
| `.OnFatalPreprocess(hooks...)` | Hooks pre-loop (error para pipeline) |
| `.OnPreprocess(hooks...)` | Hooks pre-loop (error se loguea) |
| `.OnSupervisorDecision(hooks...)` | Hook por cada decisión |
| `.OnPreAgent(hooks...)` | Hook antes de cada NodeFunc |
| `.OnPostAgent(hooks...)` | Hook después de cada NodeFunc |
| `.OnPostprocess(hooks...)` | Hooks post-loop |
| `.Build()` | Retorna error si config inválida |
| `.MustBuild()` | panic en error |

### Interfaces

```go
type StateStore interface {
    State() map[string]any
    SetState(key string, value any)
    HasFlag(key string) bool
    Messages() []Message
    AddMessage(role, text string) error
    Save() error
}

type StateView interface {
    Get(key string) (any, bool)
    GetString(key string) string
    GetBool(key string) bool
    HasFlag(key string) bool
    State() map[string]any
    Messages() []Message
    Memory() map[string]any
}

type Supervisor interface {
    DecideNextStep(ctx, store, text, phase, event) (Phase, reason, error)
    Usage() *Usage
}

type ParallelSupervisor interface {
    Supervisor
    DecideNextSteps(ctx, store, text, phase, event) ([]Phase, reason, error)
}

type NodeFunc func(ctx, view, turn, input) (*NodeResult, error)

type AgentMiddleware func(next NodeFunc) NodeFunc
```

### Eventos

| Evento | Efecto en el loop | Constructor |
|--------|-----------------|--------------|
| `EventWaitUser` | Detiene el loop | `WaitUser(answer)` |
| `EventStepSuccess` | Continúa siguiente iteración | `StepSuccess(answer)` |
| `EventPhaseComplete` | Cascada a siguiente fase | `PhaseComplete(answer)` |
| `EventBudgetExceeded` | Detiene por presupuesto | - |
| `EventErrorThreshold` | Detiene por errores | - |