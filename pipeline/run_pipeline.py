"""Orchestrator: runs all pipeline stages in order, captures lineage, writes metrics."""
import json
import pathlib
import time
from datetime import datetime, timezone
import duckdb

from lineage.tracker import LineageTracker
from pipeline.lineage_emitter import LineageEmitter
from pipeline.stages import mock_provider, ingest, validate, transform, aggregate
from observability.metrics import (
    pipeline_runs_total, lineage_nodes_total, lineage_edges_total, lineage_orphan_nodes
)

DB_PATH = pathlib.Path(__file__).parent.parent / "data" / "pipeline.duckdb"
METRICS_PATH = pathlib.Path(__file__).parent.parent / "data" / "metrics_report.json"


def _count_orphans(graph: dict) -> int:
    target_ids = {e["target_node_id"] for e in graph["edges"]}
    return sum(1 for n in graph["nodes"] if n["node_id"] not in target_ids
               and n["data_type"] != "RAW")  # RAW nodes are legitimate roots


def run(record_count: int = 50, seed: int = 42) -> dict:
    started_at = time.time()
    print(f"\n{'='*60}")
    print(f"Pipeline run started at {datetime.now(timezone.utc).isoformat()}")
    print(f"{'='*60}")

    tracker = LineageTracker()
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(str(DB_PATH))

    try:
        # Drop tables so each run starts clean (prevents silent INSERT OR IGNORE skips)
        conn.execute("DROP TABLE IF EXISTS gold_rates")
        conn.execute("DROP TABLE IF EXISTS silver_rates")
        conn.execute("DROP TABLE IF EXISTS bronze_rates")

        # Stage 0: Provider
        t0 = time.time()
        provider_emitter = LineageEmitter(tracker, stage="PROVIDER")
        raw_records = mock_provider.run(provider_emitter, count=record_count, seed=seed)
        stage_times = {"provider": round(time.time() - t0, 3)}

        # Stage 1: Ingest
        t0 = time.time()
        ingest_emitter = LineageEmitter(tracker, stage="BRONZE")
        ingest.run(conn, ingest_emitter)
        stage_times["ingest"] = round(time.time() - t0, 3)

        # Stage 2: Validate
        t0 = time.time()
        validate_emitter = LineageEmitter(tracker, stage="SILVER")
        valid_records, invalid_records = validate.run(conn, validate_emitter)
        stage_times["validate"] = round(time.time() - t0, 3)

        # Stage 3: Transform
        t0 = time.time()
        transform_emitter = LineageEmitter(tracker, stage="SILVER")
        transformed_records = transform.run(conn, transform_emitter, valid_records)
        stage_times["transform"] = round(time.time() - t0, 3)

        # Stage 4: Aggregate
        t0 = time.time()
        aggregate_emitter = LineageEmitter(tracker, stage="GOLD")
        gold_records = aggregate.run(conn, aggregate_emitter)
        stage_times["aggregate"] = round(time.time() - t0, 3)

        conn.close()

        # Finalize lineage
        graph = tracker.complete()

        # Observability
        pipeline_runs_total.labels(status="success").inc()
        lineage_nodes_total.set(len(graph["nodes"]))
        lineage_edges_total.set(len(graph["edges"]))
        orphans = _count_orphans(graph)
        lineage_orphan_nodes.set(orphans)

        total_duration = round(time.time() - started_at, 3)

        metrics = {
            "run_id": tracker.run_id,
            "pipeline": {
                "total_records_produced": len(raw_records),
                "records_ingested": len(raw_records),
                "valid_record_count": len(valid_records),
                "invalid_record_count": len(invalid_records),
                "gold_aggregation_count": len(gold_records),
                "records_per_second": round(len(raw_records) / total_duration, 1),
                "total_duration_seconds": total_duration,
                "stage_duration_seconds": stage_times,
            },
            "lineage": {
                "total_nodes": len(graph["nodes"]),
                "total_edges": len(graph["edges"]),
                "orphan_nodes": orphans,
            },
        }

        METRICS_PATH.parent.mkdir(parents=True, exist_ok=True)
        with open(METRICS_PATH, "w", encoding="utf-8") as f:
            json.dump(metrics, f, indent=2)

        print(f"\n{'='*60}")
        print(f"Pipeline complete in {total_duration}s")
        print(f"  Records: {len(raw_records)} produced, {len(valid_records)} valid, {len(invalid_records)} invalid")
        print(f"  Gold:    {len(gold_records)} aggregations")
        print(f"  Lineage: {len(graph['nodes'])} nodes, {len(graph['edges'])} edges")
        print(f"  Metrics: {METRICS_PATH}")
        print(f"{'='*60}\n")
        return metrics

    except Exception as e:
        pipeline_runs_total.labels(status="error").inc()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    run()
