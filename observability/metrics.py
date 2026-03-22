"""Prometheus metric definitions — imported by pipeline stages and the agent."""
from prometheus_client import Counter, Gauge, Histogram, CollectorRegistry

REGISTRY = CollectorRegistry()

# --- Pipeline ---
pipeline_records_total = Counter(
    "pipeline_records_total",
    "Records processed per stage",
    ["stage", "status"],  # status: success | invalid | error
    registry=REGISTRY,
)

pipeline_stage_duration_seconds = Histogram(
    "pipeline_stage_duration_seconds",
    "Duration of each pipeline stage in seconds",
    ["stage"],
    buckets=[0.01, 0.05, 0.1, 0.5, 1.0, 2.0, 5.0],
    registry=REGISTRY,
)

pipeline_runs_total = Counter(
    "pipeline_runs_total",
    "Total pipeline run attempts",
    ["status"],  # status: success | error
    registry=REGISTRY,
)

# --- Data Quality ---
data_quality_valid_records = Gauge(
    "data_quality_valid_records",
    "Count of valid records after validation stage",
    registry=REGISTRY,
)

data_quality_invalid_records = Gauge(
    "data_quality_invalid_records",
    "Count of invalid records after validation stage",
    registry=REGISTRY,
)

data_quality_validation_errors_total = Counter(
    "data_quality_validation_errors_total",
    "Validation errors by type",
    ["error_type"],
    registry=REGISTRY,
)

# --- Lineage ---
lineage_nodes_total = Gauge(
    "lineage_nodes_total",
    "Total lineage nodes in last run",
    registry=REGISTRY,
)

lineage_edges_total = Gauge(
    "lineage_edges_total",
    "Total lineage edges in last run",
    registry=REGISTRY,
)

lineage_orphan_nodes = Gauge(
    "lineage_orphan_nodes",
    "Nodes with no incoming edges (broken lineage)",
    registry=REGISTRY,
)

# --- Agent ---
agent_tool_calls_total = Counter(
    "agent_tool_calls_total",
    "Tool calls made by the agent",
    ["tool_name", "status"],  # status: success | error
    registry=REGISTRY,
)

agent_response_latency_seconds = Histogram(
    "agent_response_latency_seconds",
    "Latency from user input to agent response",
    buckets=[0.5, 1.0, 2.0, 5.0, 10.0, 30.0],
    registry=REGISTRY,
)

agent_questions_total = Counter(
    "agent_questions_total",
    "Total questions answered by the agent",
    registry=REGISTRY,
)
