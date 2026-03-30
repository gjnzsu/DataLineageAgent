# DataLineageAgent — Architecture Design

> Generated: 2026-03-22
> Purpose: Reference architecture for the local POC — finance interest rate data pipeline, OpenAI gpt-4o lineage agent, and D3.js visualization.

---

## Diagram 1 — High-Level Architecture

```mermaid
graph TD
    subgraph DataSource["Data Source"]
        MFP["MockFedProvider\n(mock_provider.py)"]
    end

    subgraph Pipeline["Pipeline Orchestrator"]
        ORCH["run_pipeline.py"]
    end

    subgraph Medallion["Medallion Stages"]
        S0["Stage 0 — Provider\n(mock_provider)"]
        S1["Stage 1 — Ingest\n(ingest)"]
        S2["Stage 2 — Validate\n(validate)"]
        S3["Stage 3 — Transform\n(transform)"]
        S4["Stage 4 — Aggregate\n(aggregate)"]
    end

    subgraph LineageLayer["Lineage Layer"]
        LE["LineageEmitter\n(lineage_emitter.py)"]
        LT["LineageTracker\n(tracker.py)"]
        LS["LineageStore\n(store.py)"]
    end

    subgraph Storage["Storage"]
        F1["interest_rates.json\n(raw)"]
        F2["pipeline.duckdb\n(bronze / silver / gold)"]
        F3["lineage_store.json"]
        F4["metrics_report.json"]
        F5["agent_session_log.json"]
    end

    subgraph Observability["Observability"]
        OBS["observability/metrics.py\n(shared REGISTRY)"]
    end

    subgraph API["FastAPI — port 3000"]
        EP1["GET /lineage"]
        EP2["GET /lineage/{node_id}"]
        EP3["GET /summary"]
        EP4["GET /health"]
        EP5["GET /metrics"]
        EP6["GET / (UI)"]
    end

    subgraph UI["Frontend"]
        D3["D3.js DAG Visualization\n(ui/index.html)"]
    end

    subgraph AgentLayer["AI Agent Layer"]
        REPL["AgentREPL\n(agent.py)"]
        TR["ToolRegistry\n(tool_registry.py)"]
        T1["get_pipeline_summary"]
        T2["get_record_lineage"]
        T3["get_downstream"]
        T4["list_transformations"]
        T5["get_node_details"]
        GPT["OpenAI gpt-4o"]
    end

    subgraph Prometheus["Prometheus"]
        PROM["Prometheus Scraper\n(optional)"]
    end

    MFP --> ORCH
    ORCH --> S0 --> S1 --> S2 --> S3 --> S4

    S0 --> LE
    S1 --> LE
    S2 --> LE
    S3 --> LE
    S4 --> LE

    LE --> LT --> LS

    S1 --> F1
    S1 --> F2
    S2 --> F2
    S3 --> F2
    S4 --> F2
    LS --> F3
    ORCH --> F4
    REPL --> F5

    ORCH --> OBS
    S0 --> OBS
    S1 --> OBS
    S2 --> OBS
    S3 --> OBS
    S4 --> OBS
    REPL --> OBS

    EP1 --> LS
    EP2 --> LS
    EP3 --> LS
    EP5 --> OBS
    EP6 --> D3

    D3 --> EP1
    D3 --> EP3

    REPL --> GPT
    GPT --> TR
    TR --> T1
    TR --> T2
    TR --> T3
    TR --> T4
    TR --> T5
    T1 --> LS
    T2 --> LS
    T3 --> LS
    T4 --> LS
    T5 --> LS
    T1 --> OBS
    REPL --> OBS

    EP5 --> PROM
```

---

## Diagram 2 — Pipeline Run Sequence

```mermaid
sequenceDiagram
    participant User
    participant Orchestrator as Orchestrator<br/>(run_pipeline.py)
    participant MockFedProvider
    participant LineageEmitter
    participant LineageTracker
    participant DuckDB
    participant LineageStore
    participant MetricsRegistry

    User->>Orchestrator: python -m pipeline.run_pipeline

    Note over Orchestrator: Stage 0 — Provider
    Orchestrator->>MockFedProvider: generate_rates(n=50)
    MockFedProvider-->>Orchestrator: 50 raw records
    Orchestrator->>LineageEmitter: emit(stage=provider, records=50)
    LineageEmitter->>LineageTracker: add_node(source) + add_edge()
    Orchestrator->>MetricsRegistry: pipeline_records_total{stage=provider} += 50

    Note over Orchestrator: Stage 1 — Ingest
    Orchestrator->>DuckDB: write bronze table (50 records)
    DuckDB-->>Orchestrator: OK
    Orchestrator->>LineageEmitter: emit(stage=ingest, in=50, out=50)
    LineageEmitter->>LineageTracker: add_node(bronze) + add_edges(50)
    Orchestrator->>MetricsRegistry: pipeline_stage_duration_seconds{stage=ingest}
    Orchestrator->>MetricsRegistry: pipeline_records_total{stage=ingest} += 50

    Note over Orchestrator: Stage 2 — Validate
    Orchestrator->>DuckDB: read bronze, validate records
    DuckDB-->>Orchestrator: 50 records read
    Orchestrator->>DuckDB: write silver table (~45 valid records)
    DuckDB-->>Orchestrator: OK
    Orchestrator->>LineageEmitter: emit(stage=validate, in=50, out=45)
    LineageEmitter->>LineageTracker: add_node(silver_validated) + add_edges(45)
    Orchestrator->>MetricsRegistry: data_quality_records_passed += 45
    Orchestrator->>MetricsRegistry: data_quality_records_failed += 5

    Note over Orchestrator: Stage 3 — Transform
    Orchestrator->>DuckDB: read silver, apply transformations
    DuckDB-->>Orchestrator: 45 records
    Orchestrator->>DuckDB: write silver_transformed (~45 records)
    DuckDB-->>Orchestrator: OK
    Orchestrator->>LineageEmitter: emit(stage=transform, in=45, out=45)
    LineageEmitter->>LineageTracker: add_node(silver_transformed) + add_edges(45)
    Orchestrator->>MetricsRegistry: pipeline_stage_duration_seconds{stage=transform}

    Note over Orchestrator: Stage 4 — Aggregate
    Orchestrator->>DuckDB: read silver_transformed, aggregate by tenor/currency
    DuckDB-->>Orchestrator: gold aggregations
    Orchestrator->>DuckDB: write gold table
    DuckDB-->>Orchestrator: OK
    Orchestrator->>LineageEmitter: emit(stage=aggregate, in=45, out=N_gold)
    LineageEmitter->>LineageTracker: add_node(gold) + add_edges()
    Orchestrator->>MetricsRegistry: pipeline_records_total{stage=aggregate}

    Note over Orchestrator: Finalize
    Orchestrator->>LineageTracker: complete()
    LineageTracker->>LineageStore: write lineage_store.json (361 nodes, 272 edges)
    LineageStore-->>Orchestrator: OK
    Orchestrator->>MetricsRegistry: lineage_nodes_total=361, lineage_edges_total=272
    Orchestrator->>MetricsRegistry: flush → metrics_report.json
    Orchestrator-->>User: Pipeline complete
```

---

## Diagram 3 — Agent Q&A Sequence

```mermaid
sequenceDiagram
    participant User
    participant AgentREPL as AgentREPL<br/>(agent.py)
    participant OpenAI as OpenAI gpt-4o
    participant ToolRegistry as ToolRegistry<br/>(tool_registry.py)
    participant LineageStore
    participant PrometheusRegistry

    User->>AgentREPL: Enter question (e.g. "What nodes are downstream of record_42?")

    Note over AgentREPL: Build messages[]
    AgentREPL->>AgentREPL: messages.append({role: user, content: question})

    Note over AgentREPL: First LLM call
    AgentREPL->>OpenAI: chat.completions.create(model=gpt-4o, messages, tools=TOOLS)
    OpenAI-->>AgentREPL: response with tool_calls[{name: get_downstream, args: {node_id: record_42}}]

    Note over AgentREPL: Tool dispatch loop
    AgentREPL->>ToolRegistry: dispatch(tool_name=get_downstream, args={node_id: record_42})
    ToolRegistry->>LineageStore: load lineage_store.json
    LineageStore-->>ToolRegistry: graph (361 nodes, 272 edges)
    ToolRegistry->>ToolRegistry: traverse downstream from record_42
    ToolRegistry-->>AgentREPL: [{node_id, label, layer, ...}, ...]

    AgentREPL->>PrometheusRegistry: agent_tool_calls_total{tool=get_downstream} += 1
    AgentREPL->>AgentREPL: messages.append({role: tool, content: result})

    Note over AgentREPL: Second LLM call (synthesis)
    AgentREPL->>OpenAI: chat.completions.create(model=gpt-4o, messages_with_tool_result)
    OpenAI-->>AgentREPL: final natural-language answer

    AgentREPL->>AgentREPL: messages.append({role: assistant, content: answer})
    AgentREPL->>AgentREPL: write agent_session_log.json
    AgentREPL->>PrometheusRegistry: agent_turns_total += 1

    AgentREPL-->>User: Print final answer
    User->>AgentREPL: Next question or "exit"
```

---

## Diagram 4 — Lineage Graph Data Model

```mermaid
classDiagram
    class LineageTracker {
        +str run_id
        +datetime started_at
        +LineageGraph graph
        +dict _record_latest_node
        +record_event(stage, operation, data_type, label, record_id, parent_node_id, parent_node_ids) str
        +get_latest_node_id(record_id) str
        +complete() dict
        +get_graph() dict
    }

    class LineageGraph {
        +str run_id
        +datetime started_at
        +datetime completed_at
        +list~LineageNode~ nodes
        +list~LineageEdge~ edges
        +int total_nodes
        +int total_edges
    }

    class LineageNode {
        +str node_id
        +str stage
        +str data_type
        +str label
        +str record_id
        +dict attributes
        +str created_at
    }

    class LineageEdge {
        +str edge_id
        +str source_node_id
        +str target_node_id
        +str operation
    }

    class LineageStore {
        +str path
        +load() LineageGraph
        +save(graph LineageGraph) void
    }

    LineageTracker "1" --> "1" LineageGraph : owns
    LineageGraph "1" --> "0..*" LineageNode : contains
    LineageGraph "1" --> "0..*" LineageEdge : contains
    LineageEdge --> LineageNode : source_id refs
    LineageEdge --> LineageNode : target_id refs
    LineageStore ..> LineageGraph : reads / writes
    LineageTracker ..> LineageStore : persists via
```

---

## Diagram 5 — API Endpoint Map

```mermaid
graph LR
    subgraph Client["Clients"]
        D3UI["D3.js UI"]
        AGENT["AI Agent"]
        CURL["curl / Postman"]
        PROM["Prometheus Scraper"]
    end

    subgraph FastAPI["FastAPI — port 3000"]
        EP1["GET /api/lineage\nReturns full DAG\n(nodes + edges)"]
        EP2["GET /api/lineage/node/{node_id}\nReturns single node\n+ neighbors"]
        EP3["GET /api/metrics-report\nReturns pipeline run\nsummary stats"]
        EP4["GET /health\nReturns service\nliveness status"]
        EP5["GET /metrics\nReturns Prometheus\ntext exposition"]
        EP6["GET /\nServes ui/index.html\n(D3.js DAG viz)"]
        EP7["POST /api/chat\nMulti-turn NL chat\nvia GPT-4o agent"]
        EP8["POST /api/run-pipeline\nTrigger pipeline\nprogrammatically"]
    end

    subgraph DataLayer["Data Layer"]
        LS["lineage_store.json"]
        OBS["observability/metrics.py\n(shared REGISTRY)"]
        HTML["ui/index.html"]
    end

    D3UI -->|"Fetch DAG data"| EP1
    D3UI -->|"Fetch summary"| EP3
    D3UI -->|"Load UI shell"| EP6
    D3UI -->|"Click-to-chat query"| EP7
    AGENT -->|"Query lineage"| EP1
    AGENT -->|"Node detail"| EP2
    CURL -->|"Ad-hoc queries"| EP1
    CURL -->|"Ad-hoc queries"| EP2
    CURL -->|"Ad-hoc queries"| EP3
    CURL -->|"Health check"| EP4
    CURL -->|"Trigger pipeline"| EP8
    PROM -->|"Scrape"| EP5

    EP1 -->|"load()"| LS
    EP2 -->|"load() + filter"| LS
    EP3 -->|"load() + aggregate"| LS
    EP4 -->|"static response"| EP4
    EP5 -->|"generate_latest()"| OBS
    EP6 -->|"static file"| HTML
    EP7 -->|"load() + agent turn"| LS
    EP8 -->|"run_pipeline()"| LS
```

---

## Cross-Cutting Concerns

| Concern | Current State | Note |
|---|---|---|
| Single-process metrics registry | `observability/metrics.py` uses one shared `REGISTRY` | Works for single-process POC; multi-process Prometheus multiprocessing mode needed for production. |
| lineage_store.json read on every API call | `LineageStore.load()` reads from disk per request | No in-memory cache; acceptable for POC, but adds latency at scale. |
| No auth on API or agent | FastAPI has no authentication | Acceptable for local POC only. |
| Agent session log append pattern | `agent_session_log.json` grows unbounded | Fine for POC; needs rotation in production. |
| OpenAI gpt-4o dependency | External API call per agent turn | Requires network + API key; no fallback. |
| POST /api/chat stateless design | Full `messages[]` array sent and returned each turn | Client owns conversation state; server remains stateless and horizontally scalable. |

---

## Design Changes — 2026-03-30

### 1. Label-Match Fallback in `get_downstream`

`get_downstream` previously required an exact `node_id` UUID. It now falls back to a case-insensitive partial label/attribute search when the supplied value is not a known UUID. The fallback mirrors the existing behaviour of `get_node_details`.

Resolution rules (in priority order):
1. Exact `node_id` UUID match — use directly.
2. Label/attribute partial match returning exactly one node — resolve and proceed.
3. Match returning 2–5 nodes — return a disambiguation list with a hint to supply `record_id`.
4. Match returning >5 nodes — return a too-many-results error with a sample of labels.
5. No match — return an error.

### 2. `record_id` Disambiguation Parameter in `get_downstream`

An optional `record_id` parameter was added to `get_downstream`. When supplied alongside a label query, candidate nodes are filtered to those whose `record_id` matches before applying the resolution rules above. If the filter produces zero candidates, the unfiltered match list is used as a fallback.

### 3. Multi-Parent Edge Wiring in `LineageTracker.record_event()`

`record_event()` gained a `parent_node_ids: list[str]` parameter. When supplied, one edge is created from each parent to the new node. This replaces the single `parent_node_id` parameter and the automatic record-chain parent — `parent_node_ids` takes full precedence when present.

| Parameter | Behaviour |
|---|---|
| `parent_node_ids` provided | Create one edge per entry; ignore `parent_node_id` and record-chain. |
| `parent_node_id` provided | Create one edge from that node; ignore record-chain. |
| Neither provided | Auto-chain: create one edge from the latest node for `record_id`. |

### 4. SILVER→GOLD Edge Wiring in `aggregate.py`

The aggregate stage previously emitted GOLD nodes with no incoming edges, leaving them as orphans in the lineage graph. It now resolves the latest SILVER node for each source `record_id` via `LineageEmitter.get_latest_node_id()` and passes those IDs as `parent_node_ids` when emitting each GOLD node. This closes the SILVER→GOLD gap in the full RAW→BRONZE→SILVER→GOLD chain.

### 5. Embedded Chat Drawer & Click-to-Chat UX

The browser UI (`ui/index.html`) was extended with a collapsible chat drawer backed by `POST /api/chat`. Clicking any DAG node pre-fills the chat input with a contextual lineage question for that node, reducing the friction between visual exploration and conversational investigation. The chat layer is stateless on the server: the full `messages[]` array is round-tripped on each request.
