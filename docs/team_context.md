# DataLineageAgent — Team Context Document

Generated: 2026-03-22
For: Multi-agent documentation team

---

## 1. Project Overview

**Purpose**: Local proof-of-concept finance data pipeline that processes mock interest rate data through a medallion architecture (raw → bronze → silver → gold), captures runtime data lineage dynamically, exposes a GPT-4o powered REPL agent for natural-language lineage queries, and provides a D3.js DAG visualization via FastAPI.

**Tech Stack**:
- Python 3.12
- DuckDB (embedded analytics DB for bronze/silver/gold tables)
- FastAPI (REST API + static file serving)
- OpenAI SDK with `gpt-4o` model (AI REPL agent with function-calling)
- `prometheus_client` (observability metrics with shared REGISTRY)
- D3.js v7 (force-directed DAG visualization in browser)
- pytest (27 tests across 4 test modules)

**Architecture Pattern**: Medallion / Lambda-style pipeline with dynamic runtime lineage capture. Five sequential stages. Each stage emits lineage events to a shared `LineageTracker` which is serialized to JSON at the end.

---

## 2. Key Files and Responsibilities

### Pipeline Orchestrator

| File | Responsibility |
|------|----------------|
| `pipeline/run_pipeline.py` | Top-level orchestrator. Instantiates `LineageTracker` and `LineageEmitter` per stage. Runs all 5 stages in order. Collects Prometheus metrics. Writes `data/metrics_report.json`. Entry point: `python -m pipeline.run_pipeline`. |
| `pipeline/lineage_emitter.py` | Thin wrapper (`LineageEmitter`) used by each stage to emit lineage events. Initialized with a `LineageTracker` and a `stage` string. Delegates to `tracker.record_event()`. |

### Pipeline Stages

| File | Stage | Responsibility |
|------|-------|----------------|
| `pipeline/stages/mock_provider.py` | PROVIDER (Stage 0) | Generates 50 mock interest rate records with ~10% intentionally bad values. Writes to `data/raw/interest_rates.json`. Emits RAW lineage nodes. Rate types: SOFR, LIBOR, FED_FUNDS_RATE. Tenors: overnight, 1M, 3M, 6M. Currency: USD only. |
| `pipeline/stages/ingest.py` | BRONZE (Stage 1) | Reads raw JSON, creates `bronze_rates` DuckDB table, inserts all records. Emits INGESTED lineage nodes (one per record). Tracks `pipeline_records_total` and `pipeline_stage_duration_seconds`. |
| `pipeline/stages/validate.py` | SILVER (Stage 2) | Reads `bronze_rates`, validates each record. Creates `silver_rates` table with `is_valid` flag. Emits VALIDATED lineage nodes. Returns `(valid_records, invalid_records)`. |
| `pipeline/stages/transform.py` | SILVER (Stage 3) | Takes `valid_records`. Applies 4 transformations per record. Emits 4 TRANSFORMED lineage nodes per record. Updates `silver_rates` derived columns in DuckDB. |
| `pipeline/stages/aggregate.py` | GOLD (Stage 4) | Groups `silver_rates` (valid only) by `rate_type + tenor_normalized + effective_date`. Computes avg/min/max/count. Creates `gold_rates` table. Emits one AGGREGATED lineage node per group. |

### Lineage Engine

| File | Responsibility |
|------|----------------|
| `lineage/tracker.py` | Core lineage engine. `LineageTracker.record_event()` creates nodes and auto-chains edges by tracking last node per `record_id`. `complete()` finalizes and calls `write_lineage_store()`. |
| `lineage/store.py` | Persistence. `read_lineage_store()` loads `data/lineage_store.json`. `write_lineage_store(graph)` serializes to JSON. |

### AI Agent

| File | Responsibility |
|------|----------------|
| `agent/agent.py` | OpenAI gpt-4o REPL loop. Loads lineage store, runs function-calling chat loop. Logs session to `data/agent_session_log.json`. Entry point: `python -m agent.agent`. |
| `agent/tool_registry.py` | `TOOLS` list (OpenAI function schemas) + `dispatch(tool_name, args, graph, metrics)` router. |
| `agent/tools/get_pipeline_summary.py` | Tool 1. |
| `agent/tools/get_record_lineage.py` | Tool 2. |
| `agent/tools/get_downstream.py` | Tool 3. |
| `agent/tools/list_transformations.py` | Tool 4. |
| `agent/tools/get_node_details.py` | Tool 5. |

### API and UI

| File | Responsibility |
|------|----------------|
| `api/main.py` | FastAPI application. 5 routes. Reads lineage from `lineage_store.json`. Serves UI via `GET /`. |
| `ui/index.html` | Single-page D3.js v7 force-directed DAG. Fetches `/api/lineage`, renders nodes by stage with color coding and filters. |

### Observability

| File | Responsibility |
|------|----------------|
| `observability/metrics.py` | All Prometheus metric definitions on shared `REGISTRY`. Imported by all pipeline stages, agent, and API. |

---

## 3. Data Flow — End to End

```
STEP 0 — MockProvider (Stage: PROVIDER)
  Input:   none
  Output:  data/raw/interest_rates.json  (50 records, seed=42)
  Lineage: 50 RAW nodes emitted
  Record:  { record_id(UUID), source, rate_type, value, currency,
             tenor, effective_date, fetched_at }

STEP 1 — Ingest (Stage: BRONZE)
  Input:   data/raw/interest_rates.json
  Output:  DuckDB bronze_rates table (50 rows)
  Lineage: 50 INGESTED nodes + 50 edges (RAW -> INGESTED)

STEP 2 — Validate (Stage: SILVER)
  Input:   DuckDB bronze_rates
  Output:  DuckDB silver_rates (all 50 rows, is_valid flag set)
           Returns: (valid_records ~45, invalid_records ~5)
  Lineage: 50 VALIDATED nodes + 50 edges (INGESTED -> VALIDATED)
  Rules:   rate_type in {SOFR,LIBOR,FED_FUNDS_RATE}
           tenor in {overnight,1M,3M,6M}
           0 <= value <= 0.30
           effective_date and record_id present

STEP 3 — Transform (Stage: SILVER)
  Input:   valid_records list from validate
  Output:  DuckDB silver_rates updated (value_bps, value_pct,
           rate_category, tenor_normalized columns set)
  Lineage: 4 TRANSFORMED nodes per valid record
           chained: VALIDATED -> BPS -> PCT -> CATEGORY -> TENOR
  Transforms:
    BPS_CONVERSION:      value * 10000
    PCT_CONVERSION:      value * 100
    RATE_CLASSIFICATION: < 0.02=LOW, <= 0.05=MEDIUM, > 0.05=HIGH
    TENOR_NORMALIZATION: overnight->ON, 1M->1M, 3M->3M, 6M->6M

STEP 4 — Aggregate (Stage: GOLD)
  Input:   DuckDB silver_rates WHERE is_valid=TRUE
  Output:  DuckDB gold_rates (one row per rate_type+tenor+date group)
  Lineage: N AGGREGATED nodes + edges from last TRANSFORMED node
           of each source record to the gold group node

FINAL OUTPUTS
  data/lineage_store.json  — full graph (~361 nodes, ~272 edges for 50 records)
  data/metrics_report.json — pipeline run stats
  data/pipeline.duckdb     — bronze_rates, silver_rates, gold_rates
  data/agent_session_log.json — written on each agent REPL session
```

---

## 4. Lineage Engine — Nodes, Edges, and Auto-Chaining

### Data Classes (`lineage/tracker.py`)

**LineageNode** fields:
- `node_id: str` — UUID4
- `stage: str` — PROVIDER | BRONZE | SILVER | GOLD
- `data_type: str` — RAW | INGESTED | VALIDATED | TRANSFORMED | AGGREGATED
- `label: str` — e.g. `RAW:SOFR:overnight`, `TRANSFORM:BPS:rec-001[:8]`, `GOLD:SOFR:ON:2024-01-15`
- `record_id: Optional[str]` — business UUID from mock_provider; gold nodes use aggregation_id
- `attributes: dict` — stage-specific metadata
- `created_at: str` — ISO8601 UTC

**LineageEdge** fields:
- `edge_id: str` — UUID4
- `source_node_id: str`
- `target_node_id: str`
- `operation: str` — PRODUCE | INGEST | VALIDATE | TRANSFORM | AGGREGATE

**LineageGraph** fields:
- `run_id: str` — UUID4 for pipeline run
- `started_at: str` — ISO8601
- `completed_at: Optional[str]` — set on `tracker.complete()`
- `nodes: list[dict]`
- `edges: list[dict]`

### Auto-Chaining Mechanism

`LineageTracker` maintains `_record_latest_node: dict[str, str]` mapping `record_id -> last_node_id`.

On each `record_event()` call:
1. New `LineageNode` created, appended to `_graph.nodes`
2. Parent resolved: explicit `parent_node_id` arg takes priority; else `_record_latest_node[record_id]`
3. If parent resolved, `LineageEdge` created and appended to `_graph.edges`
4. `_record_latest_node[record_id]` updated to new node_id

Stages only call `emitter.emit(...)` — chaining is fully automatic via the tracker.

### Node Count Breakdown (50 records input)
- PROVIDER RAW: 50 nodes
- BRONZE INGESTED: 50 nodes, 50 edges
- SILVER VALIDATED: 50 nodes, 50 edges
- SILVER TRANSFORMED: ~45 valid x 4 = ~180 nodes, ~180 edges
- GOLD AGGREGATED: varies by grouping (rate_type x tenor x date)
- **Total: ~361 nodes, ~272 edges**

---

## 5. AI Agent — GPT-4o REPL and Tools

### REPL Loop (`agent/agent.py`)

1. Loads `data/lineage_store.json` via `read_lineage_store()`
2. Loads `data/metrics_report.json` if it exists (passed to tools as `metrics`)
3. Reads `OPENAI_API_KEY` from `.env`
4. Enters `while True: input()` loop (type `exit` or `quit` to stop)
5. Each user message appended to `messages[]`
6. Calls `openai.chat.completions.create(model="gpt-4o", tools=TOOLS, messages=messages)`
7. If `tool_calls` in response: each call dispatched via `dispatch()`, result appended as `role=tool` message, model called again
8. Final assistant text printed to stdout
9. Latency measured per turn; session log written to `data/agent_session_log.json` after each turn

**System prompt** tells gpt-4o the pipeline stage order (PROVIDER->BRONZE->SILVER->GOLD) and instructs it to always cite node_ids, record_ids, and stage names.

### Tool Definitions

#### Tool 1: `get_pipeline_summary`
- **Input**: `run_id: str` (optional)
- **Output**: `{ run_id, started_at, completed_at, lineage: { total_nodes, total_edges, nodes_by_stage }, pipeline: { total_records_produced, records_ingested, valid_record_count, invalid_record_count, gold_aggregation_count, stage_duration_seconds, ... } }`
- **Description**: High-level pipeline stats from lineage graph + metrics report

#### Tool 2: `get_record_lineage`
- **Input**: `record_id: str` (UUID from mock_provider)
- **Output**: `{ record_id, chain_length, lineage_chain: [nodes in chronological order RAW->...->last] }`
- **Algorithm**: Finds all nodes for `record_id`, takes latest by `created_at`, walks upstream via `parent_map` (built from edges), reverses to chronological order

#### Tool 3: `get_downstream`
- **Input**: `node_id: str`
- **Output**: `{ node_id, start_node, downstream_count, downstream_nodes: [node dicts] }`
- **Algorithm**: BFS over `children_map` (built from edges), excludes start node, returns all reachable downstream nodes
- **Use case**: Impact analysis — what would be affected if this node changed

#### Tool 4: `list_transformations`
- **Input**: `record_id: str`
- **Output**: `{ record_id, transformation_count, transformations: [{ step, node_id, label, transformation, attributes, applied_at }] }`
- **Description**: Lists the 4 transformation steps (BPS_CONVERSION, PCT_CONVERSION, RATE_CLASSIFICATION, TENOR_NORMALIZATION) applied to a valid record, in chronological order
- **Note**: Returns error if record not found or failed validation (no TRANSFORMED nodes)

#### Tool 5: `get_node_details`
- **Input**: `node_id: str`
- **Output**: `{ node: {full node dict}, parents: [node dicts], children: [node dicts] }`
- **Description**: Returns complete metadata for a node plus its immediate upstream parents and downstream children

---

## 6. API Endpoints

Base URL: `http://localhost:3000`
Start with: `uvicorn api.main:app --port 3000`

| Method | Path | Returns | Description |
|--------|------|---------|-------------|
| GET | `/` | HTML | Serves `ui/index.html` D3.js DAG visualization |
| GET | `/api/lineage` | JSON (full graph) | Full lineage graph: `{ run_id, started_at, completed_at, nodes: [...], edges: [...] }`. 404 if store missing. |
| GET | `/api/lineage/node/{node_id}` | JSON (node dict) | Single node by node_id. 404 if not found. |
| POST | `/api/run-pipeline` | JSON `{ status, metrics }` | Triggers pipeline programmatically. 500 on error. |
| GET | `/api/metrics-report` | JSON | Last pipeline metrics report from `data/metrics_report.json`. 404 if missing. |
| GET | `/metrics` | text/plain | Prometheus scrape endpoint. Returns all metrics from shared REGISTRY. |

---

## 7. Storage — Output Files and DuckDB Tables

### Output Files

| Path | Format | Description |
|------|--------|-------------|
| `data/raw/interest_rates.json` | JSON array | 50 mock records from mock_provider |
| `data/pipeline.duckdb` | DuckDB binary | Contains bronze_rates, silver_rates, gold_rates tables |
| `data/lineage_store.json` | JSON | Full lineage graph: `{ run_id, started_at, completed_at, nodes[], edges[] }` |
| `data/metrics_report.json` | JSON | Pipeline run stats: records counts, stage durations, lineage totals |
| `data/agent_session_log.json` | JSON array | Per-turn agent log: `[{ turn, user_input, tool_calls[], latency_seconds }]` |

### DuckDB Tables

**`bronze_rates`** (created by ingest stage):
```
record_id     VARCHAR PRIMARY KEY
source        VARCHAR          -- "MockFedProvider"
rate_type     VARCHAR          -- SOFR | LIBOR | FED_FUNDS_RATE
value         DOUBLE
currency      VARCHAR          -- USD
tenor         VARCHAR          -- overnight | 1M | 3M | 6M
effective_date VARCHAR
fetched_at    VARCHAR
ingested_at   VARCHAR
source_file   VARCHAR
```

**`silver_rates`** (created by validate, updated by transform):
```
record_id           VARCHAR PRIMARY KEY
source              VARCHAR
rate_type           VARCHAR
value               DOUBLE
currency            VARCHAR
tenor               VARCHAR
effective_date      VARCHAR
fetched_at          VARCHAR
ingested_at         VARCHAR
is_valid            BOOLEAN
validation_errors   VARCHAR          -- JSON array string
value_bps           DOUBLE           -- set by transform
value_pct           DOUBLE           -- set by transform
rate_category       VARCHAR          -- LOW | MEDIUM | HIGH (set by transform)
tenor_normalized    VARCHAR          -- ON | 1M | 3M | 6M (set by transform)
validated_at        VARCHAR
```

**`gold_rates`** (created by aggregate stage):
```
aggregation_id    VARCHAR PRIMARY KEY  -- UUID4
rate_type         VARCHAR
tenor_normalized  VARCHAR
effective_date    VARCHAR
avg_value         DOUBLE
min_value         DOUBLE
max_value         DOUBLE
record_count      INTEGER
source_record_ids VARCHAR              -- comma-separated UUIDs
computed_at       VARCHAR
```

---

## 8. Observability — Prometheus Metrics

All metrics defined in `observability/metrics.py` on a shared `CollectorRegistry`. Scrape endpoint: `GET http://localhost:3000/metrics`.

### Pipeline Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `pipeline_records_total` | Counter | `stage`, `status` (success\|invalid\|error) | Records processed per stage |
| `pipeline_stage_duration_seconds` | Histogram | `stage` | Duration of each stage. Buckets: 0.01, 0.05, 0.1, 0.5, 1.0, 2.0, 5.0s |
| `pipeline_runs_total` | Counter | `status` (success\|error) | Total pipeline run attempts |

### Data Quality Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `data_quality_valid_records` | Gauge | — | Count of valid records after validation |
| `data_quality_invalid_records` | Gauge | — | Count of invalid records after validation |
| `data_quality_validation_errors_total` | Counter | `error_type` | Validation errors by type (invalid_rate_type, invalid_tenor, value_out_of_range, missing_value, missing_effective_date, missing_record_id) |

### Lineage Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `lineage_nodes_total` | Gauge | — | Total lineage nodes in last run |
| `lineage_edges_total` | Gauge | — | Total lineage edges in last run |
| `lineage_orphan_nodes` | Gauge | — | Nodes with no incoming edges (broken lineage indicator) |

### Agent Metrics

| Metric | Type | Labels | Description |
|--------|------|--------|-------------|
| `agent_tool_calls_total` | Counter | `tool_name`, `status` (success\|error) | Tool calls made by the agent |
| `agent_response_latency_seconds` | Histogram | — | Latency from user input to response. Buckets: 0.5, 1.0, 2.0, 5.0, 10.0, 30.0s |
| `agent_questions_total` | Counter | — | Total questions answered by the agent |

---

## 9. How to Run

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Configure environment
cp .env.example .env
# Edit .env and add: OPENAI_API_KEY=sk-...

# 3. Run pipeline (generates all data and lineage)
python -m pipeline.run_pipeline

# 4. Start API + UI
uvicorn api.main:app --port 3000
# Open http://localhost:3000 for D3.js DAG visualization
# API available at http://localhost:3000/api/lineage
# Prometheus metrics at http://localhost:3000/metrics

# 5. Start AI agent REPL (requires OPENAI_API_KEY)
python -m agent.agent

# 6. Run tests
python -m pytest tests/ -v
```

**Run order dependency**: Pipeline must run before API or agent (creates `data/lineage_store.json`).

---

## 10. Test Coverage Summary

**Total: 27 tests across 4 modules + 1 conftest**

### `tests/conftest.py`
Shared fixtures:
- `sample_graph` — minimal 8-node, 7-edge lineage graph with one record (`rec-001`) through all stages
- `sample_metrics` — matching metrics dict (50 produced, 45 valid, 5 invalid, 12 gold)
- `temp_lineage_store` — writes sample_graph to tmp_path, returns path
- `mem_db` — in-memory DuckDB connection

### `tests/test_lineage.py` (5 tests)
- `test_tracker_creates_node_on_event` — single event creates one node
- `test_tracker_creates_edge_on_second_event` — second event for same record_id auto-creates edge
- `test_no_orphan_nodes_after_chain` — 3-stage chain has zero orphans
- `test_store_roundtrip` — write then read produces identical graph
- `test_read_lineage_store_missing` — FileNotFoundError on missing file

### `tests/test_pipeline.py` (8 tests)
- `test_provider_generates_records` — 10 records generated with correct fields and rate_types
- `test_validation_passes_valid_record` — valid bronze record -> 1 valid, 0 invalid
- `test_validation_flags_invalid_value` — value > 0.30 -> invalid
- `test_transform_enriches_record` — value=0.05 -> bps=500.0, pct=5.0, category=MEDIUM, tenor=ON
- `test_transform_rate_categories` — 0.01=LOW, 0.03=MEDIUM, 0.06=HIGH
- `test_transform_tenor_normalization` — TENOR_MAP values correct
- *(2 additional pipeline stage tests)*

### `tests/test_agent_tools.py` (11 tests)
- `test_get_pipeline_summary` — returns run_id, lineage counts, pipeline metrics
- `test_get_pipeline_summary_no_metrics` — works without metrics (no pipeline key)
- `test_get_pipeline_summary_empty_graph` — returns error dict
- `test_get_record_lineage_returns_chain` — chain starts with RAW node
- `test_get_record_lineage_unknown_id` — returns error dict
- `test_get_downstream` — n1 downstream includes n2
- `test_get_downstream_unknown_node` — returns error dict
- `test_list_transformations` — 4 transformations with correct names
- `test_list_transformations_unknown_record` — returns error dict
- `test_get_node_details` — n2 has 1 parent (n1), correct stage=BRONZE
- `test_get_node_details_unknown` — returns error dict

### `tests/test_api.py` (5 tests)
- `test_get_lineage_returns_200` — nodes and edges present
- `test_get_node_details_endpoint` — returns correct node by ID
- `test_get_lineage_missing_store` — 404 when store absent
- `test_get_node_unknown_id` — 404 for unknown node_id
- `test_prometheus_metrics_endpoint` — 200, contains Prometheus text

---

## 11. Important Implementation Notes

1. **Tables dropped on each run**: `run_pipeline.py` drops `gold_rates`, `silver_rates`, `bronze_rates` before each run to avoid INSERT OR IGNORE silent skips.
2. **~10% bad records by design**: `mock_provider.py` injects ~10% out-of-range values (`0.31–0.50`) intentionally to exercise the validation path.
3. **Lineage for invalid records**: Validate stage emits VALIDATED nodes for ALL records (both valid and invalid). Only valid records proceed to transform and aggregate.
4. **Aggregate linege is N-to-1**: Multiple silver_transformed nodes (one per source record) each emit an edge to the same gold aggregation node.
5. **Agent reads graph as dict**: The agent loads `lineage_store.json` as a plain dict and passes it to tool functions — there is no LineageTracker instance in the agent; it operates purely on the serialized JSON.
6. **No static file mount**: The UI is served by `GET /` reading `ui/index.html` directly (not via `StaticFiles`). The D3.js script is loaded from CDN.
7. **Prometheus REGISTRY is shared**: All modules import from `observability/metrics.py`. The FastAPI `/metrics` endpoint uses this same shared REGISTRY via `generate_latest(REGISTRY)`.
