# Orchestrator

Framework para pipelines de agentes con supervisor loop.

---

## Por Qué Existe Este Framework

Imagina tu chatbot: al principio responde preguntas fixed. Pero luego necesitas múltiples passos: intake → diagnóstico → solución → confirmación. Cada if/else es un mini-monstruo. Si agregas WhatsApp + Web + Instagram, el caos se duplica.

Este framework nació de esa frustración. La idea: **separar quién decide qué hacer (supervisor) de quién lo hace (agentes)**. Como un director de orquesta: no toca ningún instrumento, pero sabe cuándo entra cada músico.

---

## Getting Started

Copia este código y ejecútalo:

```go
package main

import (
    "context"
    "fmt"

    "github.com/baalamai/orchestrator"
    "github.com/baalamai/orchestrator/supervisor"
    "github.com/baalamai/orchestrator/store"
)

func main() {
    // Definir el pipeline de forma declarativa
    cfg := orchestrator.PipelineConfig{
        MaxSteps:   3,
        Supervisor: supervisor.NewLinear("saludo", "respuesta"),
        Nodes: []orchestrator.NodeConfig{
            {
                Phase: "saludo",
                Fn: func(ctx context.Context, view orchestrator.StateView, turn *orchestrator.Turn, input *orchestrator.NodeInput) (*orchestrator.NodeResult, error) {
                    return &orchestrator.NodeResult{
                        Event:  orchestrator.EventPhaseComplete,
                        Answer: "¡Hola! ¿En qué te ayudo?",
                    }, nil
                },
            },
            {
                Phase: "respuesta",
                Fn: func(ctx context.Context, view orchestrator.StateView, turn *orchestrator.Turn, input *orchestrator.NodeInput) (*orchestrator.NodeResult, error) {
                    return &orchestrator.NodeResult{
                        Event:  orchestrator.EventWaitUser,
                        Answer: "Gracias por escribir. ¡Hasta luego!",
                    }, nil
                },
            },
        },
    }

    engine, err := orchestrator.BuildFromConfig(cfg)
    if err != nil {
        panic(err)
    }

    store := store.NewMemory()
    result, _ := engine.Run(context.Background(), store, orchestrator.NewTurn("conv-1", "mundo"))
    fmt.Println(result.Answer)
}
```

Ejecuta con `go run main.go`. Verás: `Gracias por escribir. ¡Hasta luego!`

### Qué acaba de pasar

1. **PipelineConfig** declara qué fases existen y en qué orden.
2. **supervisor.NewLinear("saludo", "respuesta")** dice: "ejecuta en orden: saludo, luego respuesta".
3. Cada **NodeConfig** define un agente: recibe lo que el usuario escribió, decide qué responder, devuelve un evento que le dice al supervisor qué viene después.
4. **engine.Run()** ejecuta el pipeline y devuelve el resultado final.

---

## Cómo Funciona Una Conversación

Imagina un usuario que escribe "tengo dolor de cabeza".

```
┌─────────────────────────────────────────────────────────┐
│ 1. Usuario escribe "tengo dolor de cabeza"             │
│                                                    │
│ 2. Supervisor: "¿Qué fase toca ahora?"              │
│    → ve que es el inicio → "saludo"                  │
│                                                    │
│ 3. Agente "saludo" responde:                        │
│    "¡Hola! ¿En qué te ayudo?"                       │
│    → devuelve EventPhaseComplete                    │
│                                                    │
│ 4. Supervisor: "pasó a la siguiente fase"         │
│    → "diagnóstico"                                   │
│                                                    │
│ 5. Agente "diagnóstico" pregunta:                  │
│    "¿Cuánto tiempo llevas con el dolor?"             │
│    → devuelve EventWaitUser                          │
│                                                    │
│ 6. Loop termina. Usuario ve la pregunta.           │
└─────────────────────────────────────────────────────────┘
```

Cada agente devuelve un **evento** que控制了 el flujo:

- **EventPhaseComplete**: "terminé mi escena, pasa a la siguiente"
- **EventWaitUser**: "necesito que el usuario me responda antes de continuar"
- **EventStepSuccess**: "terminé pero puedo volver a entrar"

---

## Conceptos Clave

### Fase

Una **fase** es una "escena" en tu obra. Cada fase tiene un agente que hace algo específico: hacer preguntas, buscar en una base de conocimiento, calcular un precio, enviar un email.

```go
{
    Phase: "diagnostico",
    Fn:    tuAgente,
}
```

### Supervisor

El **supervisor** es el director. Decide qué fase viene en cada momento. Viene con dos modelos:

- **Linear**: ejecuta fases en orden, una después de otra.
- **StateMachine**: decide según condiciones (flags en el estado, transiciones entre fases, o un router con LLM).

```go
// Linear: orden fijo
supervisor.NewLinear("saludo", "diagnostico", "respuesta")

// StateMachine: según el estado
&supervisor.StateMachine{
    DefaultPhase: "diagnostico",
    Transitions: []supervisor.TransitionRule{
        {From: "diagnostico", To: "respuesta"},
    },
}
```

### NodeConfig

Cada agente se declara con un **NodeConfig**:

```go
orchestrator.NodeConfig{
    Phase:       "diagnostico",
    Description: "Identifica el problema del usuario",  // opcional, para docs
    Fn: func(ctx context.Context, view orchestrator.StateView, turn *orchestrator.Turn, input *orchestrator.NodeInput) (*orchestrator.NodeResult, error) {
        // Tu lógica aquí
        return &orchestrator.NodeResult{
            Event:  orchestrator.EventWaitUser,
            Answer: "¿Puedes describir el problema?",
        }, nil
    },
    Repeatable: true,  // permite volver a entrar sin EventPhaseComplete
}
```

### Middleware

Los **middleware** son accesorios que se ponen antes de que el agente actúe. Puedes encadenar varios:

```go
Middleware: []orchestrator.AgentMiddleware{
    capturaMiddleware,   // extrae datos del mensaje (nombre, teléfono, etc.)
    ragMiddleware,       // busca en tu base de conocimiento
    tuMiddlewareCustom,
}
```

Cada middleware recibe el siguiente y devuelve uno nuevo. El orden de ejecución es: middleware1 → middleware2 → tuAgente.

### Estado (StateView y StateDelta)

El agente recibe una **StateView** (foto del estado actual, no puede modificar):
```go
nombre := view.GetString("nombre")  // obtiene un valor
flags := view.HasFlag("es_premium")  // pregunta si existe un flag
```

El agente devuelve un **StateDelta** (qué cambia):
```go
return &orchestrator.NodeResult{
    Delta: orchestrator.StateDelta{
        Updates: map[string]any{"nombre": "Juan", "edad": 30},
        Deletes: []string{"pending_nombre"},
    },
}
```

El engine aplica el delta automáticamente después de que el agente termina.

---

## Ejemplo Más Real: Recordar el Nombre

Este ejemplo pregunta el nombre, lo guarda, y lo usa en la siguiente fase:

```go
cfg := orchestrator.PipelineConfig{
    MaxSteps:   3,
    Supervisor: supervisor.NewLinear("pedir_nombre", "saludar"),
    Nodes: []orchestrator.NodeConfig{
        {
            Phase: "pedir_nombre",
            Fn: func(ctx context.Context, view orchestrator.StateView, turn *orchestrator.Turn, input *orchestrator.NodeInput) (*orchestrator.NodeResult, error) {
                return &orchestrator.NodeResult{
                    Event:  orchestrator.EventPhaseComplete,
                    Answer: "¿Cómo te llamas?",
                    Delta:  orchestrator.StateDelta{Updates: map[string]any{"esperando_nombre": true}},
                }, nil
            },
        },
        {
            Phase: "saludar",
            Fn: func(ctx context.Context, view orchestrator.StateView, turn *orchestrator.Turn, input *orchestrator.NodeInput) (*orchestrator.NodeResult, error) {
                nombre := view.GetString("nombre")
                if nombre == "" {
                    nombre = "amigo"
                }
                return &orchestrator.NodeResult{
                    Event:  orchestrator.EventWaitUser,
                    Answer: fmt.Sprintf("¡Hola, %s! ¿En qué te ayudo?", nombre),
                }, nil
            },
        },
    },
}
```

Nota: El segundo agente leerá el nombre que guardaste en el primer agente. El **StateDelta** del primero se convierte en el **StateView** del segundo.

---

## Ejemplo Completo: Captura + RAG

Supongamos que quieres un agente que:
1. Pregunte qué producto busca el usuario
2. Busque en tu base de conocimiento (RAG)
3. Recomiende productos

```go
cfg := orchestrator.PipelineConfig{
    MaxSteps:   3,
    Supervisor: supervisor.NewLinear("preguntar", "buscar", "recomendar"),
    Nodes: []orchestrator.NodeConfig{
        {
            Phase: "preguntar",
            Fn: func(ctx context.Context, view orchestrator.StateView, turn *orchestrator.Turn, input *orchestrator.NodeInput) (*orchestrator.NodeResult, error) {
                return &orchestrator.NodeResult{
                    Event:  orchestrator.EventPhaseComplete,
                    Answer: "¿Qué producto estás buscando?",
                }, nil
            },
        },
        {
            Phase: "buscar",
            Fn: func(ctx context.Context, view orchestrator.StateView, turn *orchestrator.Turn, input *orchestrator.NodeInput) (*orchestrator.NodeResult, error) {
                // input.RAGContext viene del middleware de RAG
                contexto := input.RAGContext
                if contexto == "" {
                    contexto = "No encontré información sobre ese producto."
                }
                return &orchestrator.NodeResult{
                    Event:  orchestrator.EventPhaseComplete,
                    Answer: contexto,
                    Delta:  orchestrator.StateDelta{Updates: map[string]any{"busqueda_realizada": true}},
                }, nil
            },
            // RAG middleware busca automáticamente en la base de conocimiento
            Middleware: []orchestrator.AgentMiddleware{
                ragMiddlewareCreator("productos"), // tu implementación de RAG
            },
        },
        {
            Phase: "recomendar",
            Fn: func(ctx context.Context, view orchestrator.StateView, turn *orchestrator.Turn, input *orchestrator.NodeInput) (*orchestrator.NodeResult, error) {
                return &orchestrator.NodeResult{
                    Event:  orchestrator.EventWaitUser,
                    Answer: "¿Te gustaría que te envíe más información por email?",
                }, nil
            },
        },
    },
}
```

El middleware de RAG llenó `input.RAGContext` automáticamente antes de que el agente se ejecutara.

---

## Errores y Reintentos

Si tu agente falla (por ejemplo, el LLM no responde), el framework puede reintentar automáticamente:

```go
cfg := orchestrator.PipelineConfig{
    MaxSteps:   3,
    Supervisor: supervisor.NewLinear("diagnostico", "respuesta"),
    Retry: orchestrator.RetryPolicy{
        MaxAttempts: 3,  // intenta hasta 3 veces
    },
    Nodes: []orchestrator.NodeConfig{
        // ...
    },
}
```

Los errores se clasifican en categorías:
- **Permanent**: invalid input, auth error — no reintenta
- **Transient**: network timeout, rate limit — reintenta
- **RateLimit**: el provider pidió esperar — reintenta con backoff

---

## Persistencia y Checkpoint

Si el servidor se cae mientras el pipeline está corriendo, el framework puede reanudar automáticamente desde donde quedó:

```go
cfg := orchestrator.PipelineConfig{
    // ...
    Checkpoints: miCheckpointStore,  // tu implementación
}
```

Cuando el usuario vuelve con el mismo `TurnID`, el engine:
1. Restaura el estado (fase actual, mensajes, tokens gastados)
2. No vuelve a guardar el mensaje del usuario (ya está)
3. Continúa desde donde quedó

---

## Referencia Rápida

| Concepto | Para qué sirve |
|----------|----------------|
| `PipelineConfig` | Declara el pipeline completo |
| `supervisor.NewLinear(...)` | Supervisor de orden fijo |
| `supervisor.StateMachine` | Supervisor con reglas |
| `NodeConfig` | Declara un agente |
| `StateView` | Foto del estado (solo lectura) |
| `StateDelta` | Cambios a aplicar |
| `EventPhaseComplete` | "Pasá a la siguiente fase" |
| `EventWaitUser` | "Esperá respuesta del usuario" |
| `Middleware` | Funciones que se ejecutan antes del agente |
| `RetryPolicy` | Configuración de reintentos |
| `CheckpointStore` | Persistencia de estado entre requests |

---

## Siguiente Paso

Este getting started muestra lo mínimo. Para pipelines más complejos:

- Usa **supervisor.StateMachine** con transiciones condicionales
- Usa **middleware de captura** para extraer datos del mensaje
- Usa **CheckpointStore** para resiliencia
- Configura **RetryPolicy** y **BudgetConfig** para controlar costos

Explora los paquetes `supervisor/`, `store/`, y `llm/` para ver qué viene incluido.