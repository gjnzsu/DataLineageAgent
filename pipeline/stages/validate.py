"""Stage 2: Validate bronze records and write to silver layer (partial — valid flag + errors)."""
import json
from datetime import datetime, timezone
import duckdb
from pipeline.lineage_emitter import LineageEmitter
from observability.metrics import (
    pipeline_records_total, pipeline_stage_duration_seconds,
    data_quality_valid_records, data_quality_invalid_records,
    data_quality_validation_errors_total,
)

KNOWN_RATE_TYPES = {"SOFR", "LIBOR", "FED_FUNDS_RATE"}
KNOWN_TENORS = {"overnight", "1M", "3M", "6M"}
MAX_RATE_VALUE = 0.30


def _validate(record: dict) -> list[str]:
    errors = []
    if record.get("rate_type") not in KNOWN_RATE_TYPES:
        errors.append("invalid_rate_type")
    if record.get("tenor") not in KNOWN_TENORS:
        errors.append("invalid_tenor")
    if not isinstance(record.get("value"), (int, float)):
        errors.append("missing_value")
    elif record["value"] > MAX_RATE_VALUE or record["value"] < 0:
        errors.append("value_out_of_range")
    if not record.get("effective_date"):
        errors.append("missing_effective_date")
    if not record.get("record_id"):
        errors.append("missing_record_id")
    return errors


def run(conn: duckdb.DuckDBPyConnection, emitter: LineageEmitter) -> tuple[list[dict], list[dict]]:
    with pipeline_stage_duration_seconds.labels(stage="validate").time():
        conn.execute("""
            CREATE TABLE IF NOT EXISTS silver_rates (
                record_id           VARCHAR PRIMARY KEY,
                source              VARCHAR,
                rate_type           VARCHAR,
                value               DOUBLE,
                currency            VARCHAR,
                tenor               VARCHAR,
                effective_date      VARCHAR,
                fetched_at          VARCHAR,
                ingested_at         VARCHAR,
                is_valid            BOOLEAN,
                validation_errors   VARCHAR,
                value_bps           DOUBLE,
                value_pct           DOUBLE,
                rate_category       VARCHAR,
                tenor_normalized    VARCHAR,
                validated_at        VARCHAR
            )
        """)

        rows = conn.execute("SELECT * FROM bronze_rates").fetchall()
        cols = [d[0] for d in conn.execute("DESCRIBE bronze_rates").fetchall()]
        records = [dict(zip(cols, row)) for row in rows]

        valid, invalid = [], []
        validated_at = datetime.now(timezone.utc).isoformat()

        for r in records:
            errors = _validate(r)
            is_valid = len(errors) == 0

            conn.execute("""
                INSERT OR IGNORE INTO silver_rates
                (record_id, source, rate_type, value, currency, tenor,
                 effective_date, fetched_at, ingested_at, is_valid,
                 validation_errors, value_bps, value_pct, rate_category,
                 tenor_normalized, validated_at)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,NULL,NULL,NULL,NULL,?)
            """, [
                r["record_id"], r["source"], r["rate_type"], r["value"],
                r["currency"], r["tenor"], r["effective_date"],
                r["fetched_at"], r["ingested_at"], is_valid,
                json.dumps(errors), validated_at
            ])

            for err in errors:
                data_quality_validation_errors_total.labels(error_type=err).inc()

            status = "success" if is_valid else "invalid"
            pipeline_records_total.labels(stage="validate", status=status).inc()

            emitter.emit(
                operation="VALIDATE",
                data_type="VALIDATED",
                label=f"SILVER:VALIDATED:{r['rate_type']}:{r['tenor']}",
                record_id=r["record_id"],
                attributes={"is_valid": is_valid, "errors": errors, "validated_at": validated_at},
            )

            enriched = {**r, "is_valid": is_valid, "validation_errors": errors, "validated_at": validated_at}
            (valid if is_valid else invalid).append(enriched)

        data_quality_valid_records.set(len(valid))
        data_quality_invalid_records.set(len(invalid))
        print(f"[validate] valid={len(valid)} invalid={len(invalid)}")
        return valid, invalid
