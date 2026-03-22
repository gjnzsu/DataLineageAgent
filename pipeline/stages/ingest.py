"""Stage 1: Ingest raw JSON records into DuckDB bronze layer."""
import json
import pathlib
from datetime import datetime, timezone
import duckdb
from pipeline.lineage_emitter import LineageEmitter
from observability.metrics import pipeline_records_total, pipeline_stage_duration_seconds

RAW_PATH = pathlib.Path(__file__).parent.parent.parent / "data" / "raw" / "interest_rates.json"


def run(conn: duckdb.DuckDBPyConnection, emitter: LineageEmitter) -> list[dict]:
    with pipeline_stage_duration_seconds.labels(stage="ingest").time():
        conn.execute("""
            CREATE TABLE IF NOT EXISTS bronze_rates (
                record_id     VARCHAR PRIMARY KEY,
                source        VARCHAR,
                rate_type     VARCHAR,
                value         DOUBLE,
                currency      VARCHAR,
                tenor         VARCHAR,
                effective_date VARCHAR,
                fetched_at    VARCHAR,
                ingested_at   VARCHAR,
                source_file   VARCHAR
            )
        """)

        with open(RAW_PATH, "r", encoding="utf-8") as f:
            records = json.load(f)

        ingested_at = datetime.now(timezone.utc).isoformat()
        source_file = str(RAW_PATH)
        rows = []

        for r in records:
            conn.execute("""
                INSERT OR IGNORE INTO bronze_rates VALUES (?,?,?,?,?,?,?,?,?,?)
            """, [
                r["record_id"], r["source"], r["rate_type"], r["value"],
                r["currency"], r["tenor"], r["effective_date"],
                r["fetched_at"], ingested_at, source_file
            ])

            emitter.emit(
                operation="INGEST",
                data_type="INGESTED",
                label=f"BRONZE:{r['rate_type']}:{r['tenor']}",
                record_id=r["record_id"],
                attributes={"ingested_at": ingested_at, "source_file": source_file},
            )
            pipeline_records_total.labels(stage="ingest", status="success").inc()
            rows.append({**r, "ingested_at": ingested_at, "source_file": source_file})

        print(f"[ingest] Loaded {len(rows)} records into bronze_rates")
        return rows
