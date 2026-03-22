"""Stage 3: Enrich valid silver records with derived fields (4 transformations per record)."""
import duckdb
from pipeline.lineage_emitter import LineageEmitter
from observability.metrics import pipeline_records_total, pipeline_stage_duration_seconds

TENOR_MAP = {
    "overnight": "ON",
    "1M": "1M",
    "3M": "3M",
    "6M": "6M",
}


def _rate_category(value: float) -> str:
    if value < 0.02:
        return "LOW"
    elif value <= 0.05:
        return "MEDIUM"
    return "HIGH"


def run(conn: duckdb.DuckDBPyConnection, emitter: LineageEmitter, valid_records: list[dict]) -> list[dict]:
    with pipeline_stage_duration_seconds.labels(stage="transform").time():
        transformed = []

        for r in valid_records:
            rid = r["record_id"]
            value = r["value"]

            # 1. BPS conversion
            value_bps = round(value * 10000, 4)
            emitter.emit(
                operation="TRANSFORM",
                data_type="TRANSFORMED",
                label=f"TRANSFORM:BPS:{rid[:8]}",
                record_id=rid,
                attributes={"transformation": "BPS_CONVERSION", "value_bps": value_bps},
            )

            # 2. Percent conversion
            value_pct = round(value * 100, 6)
            emitter.emit(
                operation="TRANSFORM",
                data_type="TRANSFORMED",
                label=f"TRANSFORM:PCT:{rid[:8]}",
                record_id=rid,
                attributes={"transformation": "PCT_CONVERSION", "value_pct": value_pct},
            )

            # 3. Rate classification
            category = _rate_category(value)
            emitter.emit(
                operation="TRANSFORM",
                data_type="TRANSFORMED",
                label=f"TRANSFORM:CATEGORY:{rid[:8]}",
                record_id=rid,
                attributes={"transformation": "RATE_CLASSIFICATION", "rate_category": category},
            )

            # 4. Tenor normalization
            tenor_norm = TENOR_MAP.get(r["tenor"], r["tenor"].upper())
            emitter.emit(
                operation="TRANSFORM",
                data_type="TRANSFORMED",
                label=f"TRANSFORM:TENOR:{rid[:8]}",
                record_id=rid,
                attributes={"transformation": "TENOR_NORMALIZATION", "tenor_normalized": tenor_norm},
            )

            # Update DuckDB silver row with enriched fields
            conn.execute("""
                UPDATE silver_rates
                SET value_bps=?, value_pct=?, rate_category=?, tenor_normalized=?
                WHERE record_id=?
            """, [value_bps, value_pct, category, tenor_norm, rid])

            pipeline_records_total.labels(stage="transform", status="success").inc()
            transformed.append({**r, "value_bps": value_bps, "value_pct": value_pct,
                                 "rate_category": category, "tenor_normalized": tenor_norm})

        print(f"[transform] Enriched {len(transformed)} records")
        return transformed
