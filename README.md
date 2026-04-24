# Orchestrator

Framework genérico para pipelines de agentes con supervisor loop. Desacopla el **routing** (supervisor) de la **ejecución** (agentes) y conecta ambos mediante **hooks** reutilizables.

```
┌─────────────────────────────────────────────────────────────────────┐
│                        shared/orchestrator                          │
│                                                                     │
│   ROUTING            EJECUCIÓN              PERSISTENCIA           │
│   ────────           ──────────             ─────────────          │
│   Supervisor  ──▶   AgentHandler   ──▶     StateStore             │
│   (decide fase)     NodeFunc + MW           (Redis / Memory)       │
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
6. [State Persistence](#state-persistence)
7. [Usage Tracking](#usage-tracking)
8. [Quick Start](#quick-start)
9. [Estructura de archivos](#estructura-de-archivos)
10. [API de referencia](#api-de-referencia)

---

## Flujo de ejecución

```
Engine.Run(ctx, store, turn)
│
├─ [1] FatalPreprocessHooks ─────────── error → detiene pipeline, retorna error
│
├─ [2] PreprocessHooks ──────────────── error → se loguea, continúa
│
├─ [3] store.AddMessage("user", turn.Text)
│
└─ [4] Loop (máx N pasos, default=3)
        │
        ├── Supervisor.DecideNextStep(store, text, currentPhase, lastEvent) → fase
        │
        ├── SupervisorHook(store, turn, step, fase, reason, usage)  ← fire-and-forget
        │
        ├── ¿fase tiene NodeFunc?
        │       │
        │       ├── snapshot = NewSnapshot(store)  ← copia congelada del estado
        │       ├── PreAgentHooks(snapshot, turn, fase)
        │       ├── mw1(mw2(agente))(snapshot, turn, input)  ← cadena de middlewares
        │       ├── PostAgentHooks(snapshot, turn, result, fase)
        │       └── engine aplica result.Delta → store.Save()  ← checkpoint
        │
        ├── ¿fase tiene AgentHandler?
        │       ├── handler(ctx, store, turn)  ← store mutable directo
        │       └── engine aplica result.StateUpdates → store.Save()
        │
        ├── Anti-stutter: misma fase + sin PhaseComplete + no Repeatable → break
        │
        └── Control de flujo:
                EventWaitUser      → break (responde al usuario)
                EventPhaseComplete → continúa (cascada a siguiente fase)
                EventStepSuccess   → continúa (misma o siguiente fase)

├─ [5] PostprocessHooks(store, turn, result)
│
└─ [6] return PipelineResult{Answer, Usage, Metadata}
```

**PipelineResult.Metadata** contiene siempre al finalizar:

| Clave | Constante | Valor |
|-------|-----------|-------|
| `"final_phase"` | `MetaFinalPhase` | Última fase ejecutada |
| `"final_event"` | `MetaFinalEvent` | Último evento (`wait_user`, etc.) |
| `"total_steps"` | `MetaTotalSteps` | Número de iteraciones del loop |

---

## Tipos de agentes

El framework soporta dos firmas de agente. Puedes mezclarlas en el mismo pipeline.

```
AgentHandler (legacy / mutable)       NodeFunc (puro / funcional)
───────────────────────────────       ────────────────────────────
func(ctx, StateStore, *Turn)          func(ctx, StateView, *Turn, *NodeInput)
    *AgentResult                           *NodeResult

Recibe StateStore mutable             Recibe StateView (snapshot read-only)
Puede mutar estado directamente       Retorna StateDelta con los cambios
store.SetState("k", v)                NodeResult{Delta: {Updates: {"k": v}}}
Engine aplica StateUpdates            Engine aplica Delta

Sin soporte de middlewares            Soporta cadena de AgentMiddleware
```

### Cuándo usar cada uno

| Situación | Tipo recomendado |
|-----------|-----------------|
| Agente nuevo con RAG o captura de campos | `NodeFunc` + middlewares |
| Agente existente sin refactorizar | `AgentHandler` |
| Tests con aislamiento de estado | `NodeFunc` (snapshot evita side-effects) |
| Necesita mutar estado y leer el resultado inmediatamente | `AgentHandler` |

### StateView — interfaz de solo lectura

Los `NodeFunc` reciben un **snapshot** del estado al momento de la llamada. El snapshot no refleja cambios posteriores.

```go
type StateView interface {
    Get(key string) (any, bool)    // valor + existe?
    GetString(key string) string   // conversión segura a string
    GetBool(key string) bool       // conversión segura a bool
    HasFlag(key string) bool       // true si el valor es bool(true)
    State() map[string]any         // copia del estado completo
    Messages() []Message           // copia del historial
    Memory() map[string]any        // memoria cross-sesión (si el store la provee)
}
```

### StateDelta — mutaciones declarativas

```go
type StateDelta struct {
    Updates map[string]any  // claves a escribir / sobreescribir
    Deletes []string        // claves a eliminar
}

// Merge combina dos deltas (other gana en conflicto)
func (d *StateDelta) Merge(other *StateDelta)
```

### NodeInput — contexto pre-computado por middlewares

```go
type NodeInput struct {
    CapturedFields map[string]any  // campos extraídos por capture middleware
    RAGContext     string          // contexto de conocimiento inyectado por RAG middleware
    Extra          map[string]any  // bag extensible para middlewares custom
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

El supervisor decide **qué fase ejecutar** en cada iteración del loop.

### LinearSupervisor

Ejecuta fases en secuencia fija. Avanza al recibir `EventPhaseComplete`.

```
phases = ["intake", "process", "respond"]

paso 1: intake    ─── PhaseComplete ──▶  paso 2: process
paso 2: process   ─── PhaseComplete ──▶  paso 3: respond
paso 3: respond   ─── WaitUser ─────▶   fin (responde al usuario)
```

```go
sup := orch.NewLinearSupervisor("intake", "process", "respond")
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
├─ [P2] currentPhase == "" (inicio de turno) && hay Router?
│        │
│        ├── Router.Route(store, text) (LLM clasifica intención)             → fase
│        └── Router falla → cae a P3
│
├─ [P3] FlagRules (recorre en orden, primer match gana)
│        Flag "register_complete" → "complete"
│        Flag "chosen_payment"    → "register"
│        Flag "quote_complete"    → "payment"
│        …
│
└─ [P4] DefaultPhase                                                         → fase
```

> **Nota:** El Router LLM solo se consulta al inicio del turno (`currentPhase == ""`). Durante el loop usa exclusivamente reglas deterministas, lo que hace el flujo predecible y rápido.

```go
sup := &orch.StateMachineSupervisor{
    DefaultPhase: "diagnostic",

    // P1a: transiciones condicionales (se evalúan antes que Transitions)
    ConditionalTransitions: []orch.ConditionalTransition{
        {From: "payment", To: "premium", Condition: func(s orch.StateStore) bool {
            return s.HasFlag("is_premium_plan")
        }},
    },

    // P1b: transiciones simples por PhaseComplete
    Transitions: []orch.TransitionRule{
        {From: "diagnostic", To: "payment"},
        {From: "payment",    To: "register"},
        {From: "register",   To: "complete"},
    },

    // P3: reglas de estado (flags de Redis)
    FlagRules: []orch.FlagRule{
        {Flag: "register_complete", Phase: "complete"},
        {Flag: "chosen_payment",    Phase: "register"},
        {Flag: "quote_complete",    Phase: "payment"},
    },

    Router: myIntentRouter, // opcional, implementa IntentRouter
}
```

#### IntentRouter

```go
type IntentRouter interface {
    Route(ctx context.Context, store StateStore, text string) (Phase, error)
    Usage() *Usage
}
```

---

## Hooks

Hay seis tipos de hooks. Se registran en el builder y se ejecutan en el orden indicado.

```
Orden de ejecución
──────────────────
[1] FatalPreprocessHook   ← error detiene pipeline
[2] PreprocessHook        ← error solo se loguea
[3] (store.AddMessage)
[4] loop:
      SupervisorHook      ← fire-and-forget (goroutine), no bloquea
      PreAgentHook        ← solo para NodeFunc, error loguea, continúa
      (ejecuta agente)
      PostAgentHook       ← solo para NodeFunc, error loguea, continúa
[5] PostprocessHook       ← error solo se loguea
```

### Firmas

```go
// [1] Error detiene el pipeline
type FatalPreprocessHook func(ctx context.Context, store StateStore, turn *Turn) error

// [2] Error se loguea, pipeline continúa
type PreprocessHook func(ctx context.Context, store StateStore, turn *Turn) error

// [4a] Después de cada decisión del supervisor (goroutine, no bloquea)
type SupervisorHook func(ctx context.Context, store StateStore, turn *Turn,
                         step int, phase Phase, reason string, usage *Usage) error

// [4b] Antes del agente (solo NodeFunc) — recibe snapshot read-only
type PreAgentHook func(ctx context.Context, view StateView, turn *Turn, phase string) error

// [4c] Después del agente (solo NodeFunc) — recibe snapshot y resultado
type PostAgentHook func(ctx context.Context, view StateView, turn *Turn,
                        result *NodeResult, phase string) error

// [5] Después del loop completo
type PostprocessHook func(ctx context.Context, store StateStore, turn *Turn,
                          result *PipelineResult) error
```

### Registro en el builder

```go
engine := orch.NewPipelineBuilder().
    WithSupervisor(sup).
    RegisterAgent("main", handler).

    OnFatalPreprocess(authHook).                 // [1]
    OnPreprocess(metricsHook, enrichHook).       // [2]
    OnSupervisorDecision(auditHook).             // [4a]
    OnPreAgent(validationHook).                  // [4b]
    OnPostAgent(loggingHook).                    // [4c]
    OnPostprocess(fallback, formatHook, save).   // [5]

    MustBuild()
```

### Hooks built-in (`adapters/`)

| Hook | Tipo | Qué hace |
|------|------|----------|
| `FallbackHook` | Postprocess | Respuesta por defecto si `result.Answer == ""` |
| `WhatsAppFormatHook` | Postprocess | Convierte `**bold**` → `*bold*` |
| `SaveResponseHook` | Postprocess | Persiste la respuesta en `store.AddMessage("model", ...)` |

`PipelineDefaults()` registra los tres automáticamente:

```go
import "chatservices/shared/orchestrator/adapters"

engine := adapters.PipelineDefaults().   // maxSteps=3 + FallbackHook + WhatsAppFormat + SaveResponse
    WithSupervisor(mySupervisor).
    RegisterAgent("diagnostic", diagHandler).
    MustBuild()
```

---

## Middleware (NodeFunc)

Los middlewares solo aplican a agentes registrados con `RegisterNode`. Siguen el patrón clásico de "onion layers": el primer middleware es la capa más externa.

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

NodeInput fluye de afuera hacia adentro:
mw1 escribe CapturedFields
  → mw2 escribe RAGContext
    → agente recibe ambos
```

### Firma

```go
type AgentMiddleware func(next NodeFunc) NodeFunc
```

### Ejemplo: captura de campos + RAG

```go
captureMW := func(next orch.NodeFunc) orch.NodeFunc {
    return func(ctx context.Context, view orch.StateView, turn *orch.Turn, input *orch.NodeInput) (*orch.NodeResult, error) {
        // extrae entidades del texto del usuario
        input.CapturedFields = map[string]any{
            "product": extractProduct(turn.Text),
        }
        return next(ctx, view, turn, input)
    }
}

ragMW := func(next orch.NodeFunc) orch.NodeFunc {
    return func(ctx context.Context, view orch.StateView, turn *orch.Turn, input *orch.NodeInput) (*orch.NodeResult, error) {
        // busca contexto en la base de conocimiento
        input.RAGContext = ragEngine.Search(ctx, turn.Text)
        return next(ctx, view, turn, input)
    }
}

agente := func(ctx context.Context, view orch.StateView, turn *orch.Turn, input *orch.NodeInput) (*orch.NodeResult, error) {
    // usa los datos enriquecidos por los middlewares
    product := input.CapturedFields["product"].(string)
    context := input.RAGContext
    answer := llm.Generate(ctx, product, context)

    return &orch.NodeResult{
        Event:  orch.EventWaitUser,
        Answer: answer,
        Delta: orch.StateDelta{
            Updates: map[string]any{"last_product": product},
        },
    }, nil
}

builder.RegisterNode("diagnostic", agente, captureMW, ragMW)
```

---

## State Persistence

El engine persiste estado de forma atómica después de cada agente:

```
AgentResult.StateUpdates  /  NodeResult.Delta
           │
           ▼
    Engine (única entidad que escribe):
      for k, v := range updates { store.SetState(k, v) }
      store.Save()   ← checkpoint en Redis
```

**Principio:** los agentes retornan **qué cambió**; el engine es el único que persiste. Esto garantiza:
- **Atomicidad**: todas las mutaciones de un agente se persisten juntas
- **Visibilidad**: el engine sabe exactamente qué cambió en cada paso
- **Durabilidad**: si el pipeline falla en el siguiente paso, el checkpoint ya está en Redis

Los agentes **no deben** llamar `store.Save()` directamente.

### StateStore — interfaz de persistencia

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
|----------------|---------------|
| `MemoryStore` (`teststore.go`) | Tests, ejemplos |
| `RedisStateAdapter` (`adapters/`) | Producción, wrappea `*state.State` |

---

## Usage Tracking

Los tokens se acumulan de cada fuente a lo largo del pipeline:

```
Supervisor.DecideNextStep()  →  Supervisor.Usage()  ─────────────────┐
                                                                      │
AgentHandler retorna AgentResult.Usage                               ▼
NodeFunc retorna NodeResult.Usage                          PipelineResult.Usage
                                                           {PromptTokens,
                                                            CompletionTokens,
                                                            TotalTokens,
                                                            Breakdown: []ModelUsage}
```

**ModelUsage** desglosa por agente y modelo:

```go
type ModelUsage struct {
    Agent            string           // "router", "diagnostic", "rag", …
    Model            string           // "gemini-2.0-flash", "claude-opus-4", …
    Provider         string           // "google", "anthropic", …
    PromptTokens     int32
    CompletionTokens int32
    TotalTokens      int32
    ContextDetails   map[string]int32 // {"cached": 500, "live": 200}
}
```

**Conversión entre capas** (`adapters/usage.go`):

```go
orchUsage := adapters.ConvertUsageToOrchestrator(agentsUsage)  // agents.Usage → orchestrator.Usage
modelsUsage := adapters.ConvertUsageToModels(orchUsage)         // orchestrator.Usage → agents.Usage
```

---

## Quick Start

### Ejemplo 1: Linear (dos fases en secuencia)

```go
import (
    "context"
    orch "chatservices/shared/orchestrator"
)

engine := orch.NewPipelineBuilder().
    WithSupervisor(orch.NewLinearSupervisor("greet", "farewell")).
    RegisterAgent("greet", func(ctx context.Context, store orch.StateStore, turn *orch.Turn) (*orch.AgentResult, error) {
        return orch.PhaseComplete("Hola " + turn.Text + "!"), nil
    }).
    RegisterAgent("farewell", func(ctx context.Context, store orch.StateStore, turn *orch.Turn) (*orch.AgentResult, error) {
        return orch.WaitUser("¿En qué más te puedo ayudar?"), nil
    }).
    MustBuild()

store := orch.NewMemoryStore()
result, _ := engine.Run(context.Background(), store, orch.NewTurn("conv-1", "mundo"))
fmt.Println(result.Answer) // "¿En qué más te puedo ayudar?"
```

### Ejemplo 2: StateMachine con NodeFunc y middleware

```go
engine := orch.NewPipelineBuilder().
    WithSupervisor(&orch.StateMachineSupervisor{
        DefaultPhase: "diagnostic",
        Transitions:  []orch.TransitionRule{{From: "diagnostic", To: "payment"}},
        FlagRules:    []orch.FlagRule{{Flag: "register_complete", Phase: "complete"}},
    }).

    // Agente puro con middleware de RAG
    RegisterNode("diagnostic", diagnosticAgent, ragMiddleware, captureMiddleware).

    // Agente legacy
    RegisterAgent("payment", func(ctx context.Context, store orch.StateStore, turn *orch.Turn) (*orch.AgentResult, error) {
        return orch.WaitUser("¿Cuál es tu método de pago?"), nil
    }).

    RegisterAgent("complete", func(ctx context.Context, store orch.StateStore, turn *orch.Turn) (*orch.AgentResult, error) {
        return orch.WaitUser("Pedido confirmado. ¡Gracias!"), nil
    }).

    OnPostprocess(func(ctx context.Context, store orch.StateStore, turn *orch.Turn, result *orch.PipelineResult) error {
        return store.AddMessage("model", result.Answer)
    }).
    MustBuild()
```

---

## Estructura de archivos

```
orchestrator/
├── doc.go               ← Documentación del paquete (go doc)
├── domain.go            ← Tipos e interfaces: Turn, AgentResult, NodeResult,
│                           StateStore, StateView, StateDelta, NodeInput,
│                           Supervisor, EventType, Usage, Message
├── orchestrator.go      ← Engine.Run() y el execution loop
├── supervisor.go        ← StateMachineSupervisor, LinearSupervisor
├── builder.go           ← PipelineBuilder (API fluent)
├── snapshot.go          ← NewSnapshot(): crea StateView desde StateStore
├── teststore.go         ← MemoryStore (StateStore in-memory para tests)
└── adapters/
    ├── pipeline.go      ← PipelineDefaults() factory
    ├── hooks.go         ← FallbackHook, WhatsAppFormatHook, SaveResponseHook
    ├── redis_state.go   ← RedisStateAdapter (wrappea *state.State)
    ├── webhook.go       ← TurnFromWebHook(), WebHookFromTurn()
    ├── usage.go         ← ConvertUsageToOrchestrator(), ConvertUsageToModels()
    └── errors.go        ← Errores del adapter
```

### Orden de lectura (primera vez en el módulo)

| # | Archivo | Qué encontrarás |
|---|---------|-----------------|
| 1 | `domain.go` | **Empieza aquí.** Todos los tipos e interfaces |
| 2 | `orchestrator.go` | El execution loop completo |
| 3 | `supervisor.go` | `StateMachineSupervisor` y `LinearSupervisor` |
| 4 | `builder.go` | API fluent del builder |
| 5 | `teststore.go` | Solo si escribes tests: `MemoryStore` |
| 6 | `adapters/` | Solo si integras con Redis, webhooks o hooks built-in |

---

## API de referencia

### Builder

| Método | Descripción |
|--------|-------------|
| `NewPipelineBuilder()` | Builder con defaults (`maxSteps=3`) |
| `.WithSupervisor(s)` | **Requerido.** Supervisor de routing |
| `.RegisterAgent(fase, fn)` | Registra un `AgentHandler` (legacy/mutable) |
| `.RegisterNode(fase, fn, mw...)` | Registra un `NodeFunc` con middlewares opcionales |
| `.MaxSteps(n)` | Máximo de iteraciones del loop (default: 3) |
| `.Repeatable(fases...)` | Fases que pueden re-entrar con `EventStepSuccess` sin romper anti-stutter |
| `.WithLogger(l)` | Logger estructurado opcional |
| `.OnFatalPreprocess(hooks...)` | Hooks pre-loop: error detiene pipeline |
| `.OnPreprocess(hooks...)` | Hooks pre-loop: error solo se loguea |
| `.OnSupervisorDecision(hooks...)` | Hook por cada decisión del supervisor (fire-and-forget) |
| `.OnPreAgent(hooks...)` | Hook antes de cada `NodeFunc` |
| `.OnPostAgent(hooks...)` | Hook después de cada `NodeFunc` |
| `.OnPostprocess(hooks...)` | Hooks post-loop |
| `.Build()` | Construye el engine (retorna error si config inválida) |
| `.MustBuild()` | Build con `panic` en error |

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
    DecideNextStep(ctx context.Context, store StateStore, userText string,
                   currentPhase Phase, lastEvent EventType) (Phase, string, error)
    Usage() *Usage
}

type IntentRouter interface {
    Route(ctx context.Context, store StateStore, text string) (Phase, error)
    Usage() *Usage
}
```

### Eventos

| Evento | Efecto en el loop | Constructor |
|--------|-------------------|-------------|
| `EventWaitUser` | Detiene el loop, devuelve respuesta al usuario | `WaitUser(answer)` |
| `EventStepSuccess` | Continúa (siguiente iteración) | `StepSuccess(answer)` |
| `EventPhaseComplete` | Marca fase completa, cascadea a siguiente | `PhaseComplete(answer)` |

### Adapters

| Componente | Descripción |
|------------|-------------|
| `adapters.PipelineDefaults()` | Builder pre-configurado con `maxSteps=3` + 3 hooks built-in |
| `adapters.NewRedisStateAdapter(st)` | `StateStore` sobre `*state.State` (producción) |
| `adapters.TurnFromWebHook(w)` | Convierte `*webhook.WebHook` → `*Turn` |
| `adapters.WebHookFromTurn(turn)` | Extrae el webhook original del `turn.Metadata` |
| `adapters.ConvertUsageToOrchestrator(u)` | `agents.Usage` → `orchestrator.Usage` |
| `adapters.ConvertUsageToModels(u)` | `orchestrator.Usage` → `agents.Usage` |
