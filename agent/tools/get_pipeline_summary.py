"""Tool: get_pipeline_summary — returns high-level stats for the last pipeline run."""

SCHEMA = {
    "name": "get_pipeline_summary",
    "description": "Returns high-level statistics for the pipeline run: total records, valid/invalid counts, stage timings, lineage node/edge counts, and gold aggregation count.",
    "parameters": {
        "type": "object",
        "properties": {
            "run_id": {
                "type": "string",
                "description": "Optional run_id to filter by. If omitted, returns the most recent run.",
            }
        },
        "required": [],
    },
}


def execute(graph: dict, metrics: dict | None, run_id: str | None = None) -> dict:
    if not graph:
        return {"error": "No lineage data found. Run the pipeline first."}

    nodes = graph.get("nodes", [])
    edges = graph.get("edges", [])

    stage_counts: dict[str, int] = {}
    for node in nodes:
        s = node.get("stage", "UNKNOWN")
        stage_counts[s] = stage_counts.get(s, 0) + 1

    summary = {
        "run_id": graph.get("run_id"),
        "started_at": graph.get("started_at"),
        "completed_at": graph.get("completed_at"),
        "lineage": {
            "total_nodes": len(nodes),
            "total_edges": len(edges),
            "nodes_by_stage": stage_counts,
        },
    }

    if metrics:
        summary["pipeline"] = metrics.get("pipeline", {})

    return summary
