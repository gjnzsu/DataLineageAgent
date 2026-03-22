"""Stage 4: Aggregate silver records into gold layer (daily avg/min/max per rate_type+tenor)."""
import uuid
from datetime import datetime, timezone
import duckdb
from pipeline.lineage_emitter import LineageEmitter
from observability.metrics import pipeline_records_total, pipeline_stage_duration_seconds


def run(conn: duckdb.DuckDBPyConnection, emitter: LineageEmitter) -> list[dict]:
    with pipeline_stage_duration_seconds.labels(stage="aggregate").time():
        conn.execute("""
            CREATE TABLE IF NOT EXISTS gold_rates (
                aggregation_id      VARCHAR PRIMARY KEY,
                rate_type           VARCHAR,
                tenor_normalized    VARCHAR,
                effective_date      VARCHAR,
                avg_value           DOUBLE,
                min_value           DOUBLE,
                max_value           DOUBLE,
                record_count        INTEGER,
                source_record_ids   VARCHAR,
                computed_at         VARCHAR
            )
        """)

        groups = conn.execute("""
            SELECT
                rate_type,
                COALESCE(tenor_normalized, tenor) AS tenor_norm,
                effective_date,
                AVG(value)   AS avg_value,
                MIN(value)   AS min_value,
                MAX(value)   AS max_value,
                COUNT(*)     AS record_count,
                LIST(record_id) AS source_ids
            FROM silver_rates
            WHERE is_valid = TRUE
            GROUP BY rate_type, tenor_norm, effective_date
        """).fetchall()

        computed_at = datetime.now(timezone.utc).isoformat()
        gold_records = []

        for row in groups:
            rate_type, tenor_norm, eff_date, avg_v, min_v, max_v, cnt, src_ids = row
            agg_id = str(uuid.uuid4())
            src_ids_str = ",".join(src_ids) if src_ids else ""

            conn.execute("""
                INSERT OR IGNORE INTO gold_rates VALUES (?,?,?,?,?,?,?,?,?,?)
            """, [agg_id, rate_type, tenor_norm, eff_date,
                  round(avg_v, 6), round(min_v, 6), round(max_v, 6),
                  cnt, src_ids_str, computed_at])

            # Emit one AGGREGATE lineage event per gold record
            # Parent nodes are the last TRANSFORM nodes for each source record
            emitter.emit(
                operation="AGGREGATE",
                data_type="AGGREGATED",
                label=f"GOLD:{rate_type}:{tenor_norm}:{eff_date}",
                record_id=agg_id,
                attributes={
                    "rate_type": rate_type,
                    "tenor_normalized": tenor_norm,
                    "effective_date": eff_date,
                    "avg_value": round(avg_v, 6),
                    "record_count": cnt,
                    "source_record_ids": src_ids,
                    "computed_at": computed_at,
                },
            )
            pipeline_records_total.labels(stage="aggregate", status="success").inc()
            gold_records.append({
                "aggregation_id": agg_id, "rate_type": rate_type,
                "tenor_normalized": tenor_norm, "effective_date": eff_date,
                "avg_value": round(avg_v, 6), "min_value": round(min_v, 6),
                "max_value": round(max_v, 6), "record_count": cnt,
                "source_record_ids": src_ids, "computed_at": computed_at,
            })

        print(f"[aggregate] Produced {len(gold_records)} gold records")
        return gold_records
