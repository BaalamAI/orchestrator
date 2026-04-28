# Arquitectura de `lib/orchestrator`

`lib/orchestrator` es la libreria que ejecuta pipelines de agentes multi-fase.
No es el servicio `services/orchestrator/` que recibe webhooks; este modulo es el
motor reusable que decide que fase corre, prepara el input del agente, aplica cambios
de estado y termina cuando el turno necesita volver al usuario.

---

## Que hace el modulo

```
Turn del usuario -> Engine -> PipelineResult
                    |         (Answer + Usage + Metadata)
                    |
                    +-- Supervisor decide fase(s)
                    +-- Middleware prepara contexto
                    +-- NodeFunc ejecuta el agente
                    +-- Engine aplica StateDelta
                    +-- Hooks observan / enriquecen el ciclo
```

El objetivo principal es separar tres responsabilidades que antes terminaban mezcladas:

1. **Decision de flujo**: el supervisor responde "que fase toca ahora?".
2. **Trabajo del agente**: cada nodo resuelve una fase concreta y devuelve un resultado.
3. **Mutacion de estado**: solo el engine aplica deltas al `StateStore`.

El diseno sigue el patron de agentes como funciones puras: un nodo recibe una foto de
estado (`StateView`) y retorna cambios propuestos (`StateDelta`). El nodo no recibe el
store mutable y no guarda directamente.

---

## Mapa del modulo

```
lib/orchestrator/
|
+-- domain.go          Tipos centrales: Turn, EventType, NodeFunc, StateDelta
+-- ports.go           Puertos: StateStore, Supervisor, IntentRouter, CostCalculator
+-- config.go          PipelineConfig declarativo
+-- builder.go         PipelineBuilder fluent
+-- run.go             Engine.Run: preprocess -> loop -> postprocess
+-- loop.go            Ciclo supervisor -> agente
+-- retry.go           Ejecucion de nodos, retry y commit de deltas
+-- parallel.go        Ejecucion paralela de fases concurrent-safe
+-- checkpoint.go      Snapshot mid-turn y CheckpointStore
+-- budget.go          Limites de tokens, costo y errores transitorios
+-- hooks.go           Firmas de hooks de ciclo de vida
+-- metrics.go         Instrumentos OpenTelemetry
+-- snapshot.go        Implementacion de StateView sobre StateStore
|
+-- supervisor/
|   +-- linear.go       Supervisor secuencial
|   +-- statemachine.go Supervisor por reglas + router opcional
|
+-- hook/
|   +-- hook.go         Helpers de hooks
|   +-- middleware.go   ComposeParallel para providers
|
+-- llm/
|   +-- ports.go        Puertos LLM y Tool
|   +-- toolloop.go     NodeFunc ReAct con tool calling
|
+-- store/
|   +-- memory.go       StateStore en memoria
|   +-- checkpoint.go   CheckpointStore en memoria
|
+-- obs/
    +-- obs.go          Bolsa de observabilidad para tool loops
```

Regla de dependencias: el paquete raiz define el core y los puertos. Los subpaquetes
aportan adapters concretos o helpers. Los servicios consumidores conectan adapters reales
para Redis, Mongo, LLMs, logging, trazas y costos.

---

## Flujo completo de un turno

```
Engine.Run(ctx, store, turn)
|
+-- 1. FatalPreprocessHooks
|      +-- si fallan, el turno se detiene
|
+-- 2. PreprocessHooks
|      +-- best-effort: errores se loguean y el turno continua
|
+-- 3. Checkpoint
|      +-- si existe checkpoint para TurnID: restaura estado e historial
|      +-- si no existe: store.AddMessage("user", turn.Text)
|
+-- 4. Loop principal
|      |
|      +-- Supervisor.DecideNextStep / DecideNextSteps
|      +-- SupervisorHooks
|      +-- computeNode / commitNode
|      |   +-- StateView snapshot
|      |   +-- PreAgentHooks
|      |   +-- Middleware chain
|      |   +-- NodeFunc
|      |   +-- PostAgentHooks
|      +-- Engine aplica StateDelta al StateStore
|      +-- merge de Answer, Usage, Metadata y SharedContext
|      +-- guarda checkpoint despues de cada paso exitoso
|      +-- evalua condicion de parada
|
+-- 5. PostprocessHooks
|
+-- 6. Limpia checkpoint en completion normal
|
+-- PipelineResult{Answer, Usage, Metadata}
```

### Condiciones de parada

| Evento / condicion | Efecto |
|--------------------|--------|
| `EventWaitUser` | El agente ya tiene respuesta y necesita esperar al usuario. |
| `BudgetConfig.MaxTokens` | Se alcanzo el limite acumulado de tokens del turno. |
| `BudgetConfig.MaxCostUSD` | Se alcanzo el costo estimado acumulado. |
| `BudgetConfig.MaxTransientErrors` | Hubo demasiados errores transitorios/rate limit. |
| `MaxSteps` | El loop llega al limite duro de iteraciones. |
| Anti-stutter | Evita repetir la misma fase si no esta marcada como `Repeatable`. |

---

## Conceptos principales

### Turn

`Turn` es el mensaje normalizado que entra al pipeline. Es agnostico al canal:
WhatsApp, web, API o cualquier integracion externa terminan convertidas a este tipo.

Campos importantes:

| Campo | Uso |
|-------|-----|
| `TurnID` | ID estable para checkpoint/resume. Si esta vacio, no hay checkpoint. |
| `ConversationID` | Identifica la conversacion. |
| `UserID`, `OrgID`, `OrgName` | Contexto multi-tenant. |
| `Channel`, `MessageType` | Metadata de entrada. |
| `Text` | Texto del usuario que el supervisor y los agentes leen. |
| `Metadata` | Bolsa para datos transitorios del request. |

### StateStore, StateView y StateDelta

El engine trabaja contra un `StateStore`, pero los agentes solo reciben `StateView`.
Esto mantiene a los nodos como funciones puras.

```
StateStore mutable -> NewSnapshot(store) -> StateView read-only
                                                 |
                                                 v
                                            NodeFunc
                                                 |
                                                 v
                                           StateDelta
                                                 |
                                                 v
                                      Engine.commitDelta()
```

`StateDelta` tiene dos listas:

```go
type StateDelta struct {
    Updates map[string]any
    Deletes []string
}
```

El engine aplica `Updates`, luego `Deletes`, y llama `store.Save()` si hubo cambios.

### NodeFunc

Un nodo es el agente de una fase:

```go
type NodeFunc func(
    ctx context.Context,
    view StateView,
    turn *Turn,
    input *NodeInput,
) (*NodeResult, error)
```

`NodeInput` trae contexto preparado por middleware:

| Campo | Uso |
|-------|-----|
| `CapturedFields` | Campos estructurados extraidos del mensaje. |
| `RAGContext` | Contexto textual de recuperacion. |
| `Extra` | Datos de providers custom. |
| `SharedContext` | Datos acumulados entre fases dentro del mismo turno. |

`NodeResult` comunica salida y control de flujo:

| Campo | Uso |
|-------|-----|
| `Event` | Decide si continuar, cascadear o esperar al usuario. |
| `Answer` | Respuesta candidata del turno; la ultima no vacia gana. |
| `Delta` | Cambios persistentes que el engine aplicara. |
| `Usage` | Tokens consumidos por el nodo o sus providers. |
| `Metadata` | Datos transitorios para postprocess hooks. |
| `SharedContext` | Datos para fases posteriores del mismo turno. |

### Eventos

| Evento | Significado |
|--------|-------------|
| `EventStepSuccess` | La fase hizo trabajo y el loop puede seguir. |
| `EventPhaseComplete` | La fase termino y el supervisor puede mover a la siguiente. |
| `EventWaitUser` | Hay que responder y esperar nuevo input del usuario. |
| `EventBudgetExceeded` | El turno supero presupuesto. |
| `EventErrorThreshold` | El ledger de errores supero el limite configurado. |

---

## Supervisores

El supervisor decide que fase ejecutar en cada iteracion. El engine solo sabe invocar el
puerto:

```go
type Supervisor interface {
    DecideNextStep(ctx context.Context, store StateStore, userText string,
        currentPhase Phase, lastEvent EventType) (Phase, string, error)
    Usage() *Usage
}
```

### `supervisor.Linear`

Ejecuta fases en orden fijo. Avanza cuando la fase actual emite `EventPhaseComplete`.
Es util para pipelines simples o pruebas.

```go
supervisor.NewLinear("intake", "diagnostic", "answer")
```

### `supervisor.StateMachine`

Combina reglas deterministicas con un router opcional:

1. Si el evento anterior fue `EventPhaseComplete`, evalua transiciones.
2. Al inicio del turno, si hay `Router`, clasifica intencion.
3. Si no hay routing aplicable, evalua `FlagRules` en el estado.
4. Si nada coincide, usa `DefaultPhase`.

```go
&supervisor.StateMachine{
    DefaultPhase: "diagnostic",
    Transitions: []supervisor.TransitionRule{
        {From: "diagnostic", To: "payment"},
    },
    ConditionalTransitions: []supervisor.ConditionalTransition{
        {From: "payment", To: "register", Condition: paymentChosen},
    },
    FlagRules: []supervisor.FlagRule{
        {Flag: "register_complete", Phase: "complete"},
    },
    Router: routerAdapter,
}
```

Los supervisores concretos implementan `SupervisorValidator`; por eso `Build()` falla temprano
si una regla apunta a una fase sin nodo registrado.

### ParallelSupervisor

Un supervisor puede retornar multiples fases con `DecideNextSteps`. El engine particiona:

| Tipo de nodo | Ejecucion |
|--------------|-----------|
| `RegisterConcurrentNode` / `NodeConfig.Concurrent` | Corre en paralelo sin mutar store. |
| Nodo normal | Corre secuencialmente despues. |

Las fases concurrentes usan `computeNode()`: calculan `NodeResult` sobre snapshots y el engine
mergea sus deltas en memoria. Luego hace un solo commit al store.

Reglas de merge:

| Dato | Regla |
|------|-------|
| `Answer` | La ultima respuesta no vacia gana. |
| `Delta` | Se mergean updates/deletes; conflictos los gana el ultimo merge. |
| `Usage` | Se suma. |
| `Metadata`, `SharedContext` | Se fusionan por key. |
| `Event` | Gana mayor prioridad: `WaitUser` > `BudgetExceeded` > `ErrorThreshold` > `PhaseComplete`. |

---

## Middleware y providers

El middleware envuelve un `NodeFunc`. Se registra por nodo y se ejecuta antes del agente.

```go
orchestrator.NodeConfig{
    Phase: "diagnostic",
    Fn:    diagnosticNode,
    Middleware: []orchestrator.AgentMiddleware{
        hook.ComposeParallel(captureProvider, ragProvider),
    },
}
```

`hook.ComposeParallel()` ejecuta varios `NodeProvider` en paralelo:

```go
type NodeProvider interface {
    Provide(ctx context.Context, view StateView, turn *Turn) (*ProviderResult, error)
}
```

Cada provider puede aportar:

| Campo | Destino |
|-------|---------|
| `InputKey: "rag_context"` | `input.RAGContext` |
| `InputKey: "captured_fields"` | `input.CapturedFields` |
| Otro `InputKey` | `input.Extra[key]` |
| `Delta` | Se mergea con el delta final del agente. |
| `Usage` | Se suma al uso del nodo. |
| `Metadata` | Se agrega al `NodeResult.Metadata` si el agente no lo sobreescribe. |

Esto permite correr captura, RAG, inventario u otros enriquecedores sin acoplarlos al engine.

---

## Hooks

Los hooks son puntos de extension alrededor del loop. Sirven para observabilidad,
agent tree, normalizacion de input, auditoria, follow-up o persistencia externa.

| Hook | Momento | Error |
|------|---------|-------|
| `FatalPreprocessHook` | Antes del loop | Detiene el turno. |
| `PreprocessHook` | Antes del loop | Se loguea y continua. |
| `SupervisorHook` | Despues de cada decision | Se loguea y continua. |
| `PreAgentHook` | Antes de cada nodo | Se loguea y continua. |
| `PostAgentHook` | Despues de cada nodo | Se loguea y continua. |
| `PostprocessHook` | Despues del loop | Se loguea y continua. |

El paquete `hook/` contiene helpers para componer hooks, agregar timeouts y ejecutar providers.

---

## Retry, errores y budget

Cada nodo puede ejecutarse con `RetryPolicy`:

```go
orchestrator.RetryPolicy{
    MaxAttempts: 3,
    ShouldRetry: func(err error) bool { return true },
    OnRetryExhausted: func(phase orchestrator.Phase, err error) (orchestrator.Phase, error) {
        return "fallback", nil
    },
}
```

El retry loop clasifica errores con `ErrorClassifier`:

| Categoria | Comportamiento |
|-----------|----------------|
| `CategoryPermanent` | No reintenta. |
| `CategoryTransient` | Puede reintentar. |
| `CategoryRateLimit` | Puede reintentar y cuenta como transitorio. |
| `CategoryUnknown` | Depende de `ShouldRetry`. |

El `errorLedger` acumula errores por turno. Si `BudgetConfig.MaxTransientErrors` se alcanza,
el loop termina con `EventErrorThreshold`.

`BudgetConfig` tambien puede cortar por tokens o costo:

```go
orchestrator.BudgetConfig{
    MaxTokens:          12000,
    MaxCostUSD:         0.25,
    MaxTransientErrors: 3,
}
```

El costo requiere un `CostCalculator`; si no se configura, el calculo de costo retorna `0`.

---

## Checkpoints y resume

Si se configura `CheckpointStore`, el engine guarda un snapshot despues de cada fase exitosa.
En una llamada posterior con el mismo `Turn.TurnID`, carga ese snapshot y continua desde ahi.

El checkpoint incluye:

| Campo | Uso |
|-------|-----|
| `TurnID` | Llave de resume. |
| `Step`, `Phase`, `LastEvent` | Posicion del loop. |
| `State`, `Messages` | Snapshot del store. |
| `Usage` | Tokens acumulados antes del crash. |
| `ErrorCounts` | Ledger de errores serializado. |
| `SharedContext` | Contexto intra-turno acumulado. |

Contrato importante: el engine reanuda fases, pero no deduplica efectos externos. Tools o hooks
que envian mensajes, crean links de pago o escriben en sistemas externos deben ser idempotentes
por llave natural. Ver tambien `docs/adr-tool-idempotency.md`.

---

## Tool loop LLM

El paquete `llm/` permite construir un `NodeFunc` con loop ReAct:

```
LLM completion
   |
   +-- stop_reason = tool_use
   |      +-- invoca tools solicitadas
   |      +-- mergea ToolResult.Delta y Usage
   |      +-- agrega tool messages y repite
   |
   +-- stop_reason terminal
          +-- retorna NodeResult{Answer, Event}
```

`NewToolLoopNode(client, opts)` no depende de un proveedor concreto. Los servicios consumidores
adaptan OpenAI, Anthropic, Gemini u otro SDK al puerto `llm.Client`.

Cada tool declara si es idempotente:

```go
type ToolDefinition struct {
    Name        string
    Description string
    InputSchema json.RawMessage
    Idempotent  bool
}
```

Ese flag documenta la expectativa del tool. El engine no lo enforza automaticamente.

---

## Observabilidad

El engine puede recibir `trace.Tracer` y `metric.Meter`. Si no se pasan, usa no-op providers.

Spans principales:

| Span | Archivo |
|------|---------|
| `orchestrator.turn` | `run.go` |
| `orchestrator.supervisor` | `loop.go` |
| `orchestrator.node` | `loop.go` |
| `orchestrator.node.attempt` | `retry.go` |
| `orchestrator.node.concurrent` | `parallel.go` |
| `orchestrator.llm` | `llm/toolloop.go` |
| `orchestrator.tool` | `llm/toolloop.go` |

Metricas principales:

| Metrica | Uso |
|---------|-----|
| `orchestrator.phase.duration_ms` | Duracion por fase y outcome. |
| `orchestrator.retries` | Reintentos por fase y categoria. |
| `orchestrator.checkpoint` | Eventos `save`, `miss`, `resume`, `clear`, `error`. |
| `orchestrator.tokens` | Tokens por fase, modelo, provider y tipo. |
| `orchestrator.cost_usd` | Costo acumulado por fase/modelo. |
| `orchestrator.llm.duration_ms` | Latencia de completions LLM. |
| `orchestrator.tool.duration_ms` | Latencia de tools. |
| `orchestrator.tool.calls` | Invocaciones de tools por outcome. |

El tool loop recibe observabilidad via contexto (`obs.WithObservability`) para no acoplarlo
directamente al `Engine`.

---

## Construccion de pipelines

Hay dos formas equivalentes de construir un engine.

### `PipelineConfig`

Usado por servicios que generan pipeline desde configuracion declarativa:

```go
cfg := orchestrator.PipelineConfig{
    MaxSteps:   4,
    Supervisor: supervisor.NewLinear("intake", "answer"),
    Nodes: []orchestrator.NodeConfig{
        {
            Phase: "intake",
            Fn:    intakeNode,
        },
        {
            Phase:      "answer",
            Fn:         answerNode,
            Repeatable: true,
        },
    },
}

engine, err := orchestrator.BuildFromConfig(cfg)
```

### `PipelineBuilder`

Util para wiring manual y tests:

```go
engine := orchestrator.NewPipelineBuilder().
    WithSupervisor(supervisor.NewLinear("intake", "answer")).
    MaxSteps(4).
    WithRetry(orchestrator.RetryPolicy{MaxAttempts: 2}).
    WithBudget(orchestrator.BudgetConfig{MaxTokens: 8000}).
    RegisterNode("intake", intakeNode).
    RegisterNode("answer", answerNode).
    MustBuild()
```

---

## Ejemplo minimo ejecutable

```go
package main

import (
    "context"
    "fmt"

    "github.com/baalamai/orchestrator"
    "github.com/baalamai/orchestrator/store"
    "github.com/baalamai/orchestrator/supervisor"
)

func main() {
    cfg := orchestrator.PipelineConfig{
        MaxSteps:   3,
        Supervisor: supervisor.NewLinear("saludo", "respuesta"),
        Nodes: []orchestrator.NodeConfig{
            {
                Phase: "saludo",
                Fn: func(ctx context.Context, view orchestrator.StateView, turn *orchestrator.Turn, input *orchestrator.NodeInput) (*orchestrator.NodeResult, error) {
                    return &orchestrator.NodeResult{
                        Event:  orchestrator.EventPhaseComplete,
                        Answer: "Hola, voy a preparar una respuesta.",
                    }, nil
                },
            },
            {
                Phase: "respuesta",
                Fn: func(ctx context.Context, view orchestrator.StateView, turn *orchestrator.Turn, input *orchestrator.NodeInput) (*orchestrator.NodeResult, error) {
                    return &orchestrator.NodeResult{
                        Event:  orchestrator.EventWaitUser,
                        Answer: "Listo. En que mas te ayudo?",
                    }, nil
                },
            },
        },
    }

    engine, err := orchestrator.BuildFromConfig(cfg)
    if err != nil {
        panic(err)
    }

    mem := store.NewMemory()
    result, err := engine.Run(context.Background(), mem, orchestrator.NewTurn("conv-1", "hola"))
    if err != nil {
        panic(err)
    }
    fmt.Println(result.Answer)
}
```

---

## Referencia tecnica

| Area | Archivo | Responsabilidad |
|------|---------|-----------------|
| Entrada | `run.go` | `Engine.Run`, hooks, checkpoint resume. |
| Loop | `loop.go` | Decision del supervisor, ejecucion de fase, condiciones de parada. |
| Ejecucion | `retry.go` | `computeNode`, `commitNode`, retry y commit de deltas. |
| Paralelo | `parallel.go` | Partition concurrent/sequential y merge de resultados. |
| Dominio | `domain.go` | `Turn`, `NodeFunc`, `NodeInput`, `NodeResult`, `Usage`. |
| Puertos | `ports.go` | `StateStore`, `Supervisor`, `IntentRouter`, clasificadores. |
| Config | `config.go`, `builder.go` | `PipelineConfig`, `PipelineBuilder`, validacion. |
| Estado | `snapshot.go`, `store/` | Vista read-only y stores en memoria. |
| Supervisores | `supervisor/` | Linear y StateMachine. |
| Middleware | `hook/middleware.go` | Providers paralelos y merge en `NodeInput`. |
| Hooks | `hooks.go`, `hook/hook.go` | Puntos de extension y helpers. |
| Resiliencia | `checkpoint.go`, `budget.go`, `errors.go` | Resume, limites y categorias de error. |
| LLM | `llm/` | Tool loop ReAct y puertos LLM/tool. |
| Observabilidad | `metrics.go`, `obs/` | OTel metrics, spans y contexto para tool loops. |

---

## Como validar cambios

Para cambios en el modulo:

```bash
just gazelle
just test-pkg //lib/orchestrator/...
```

Si el cambio toca imports, BUILD files o dependencias, correr `just gazelle` antes de testear.
