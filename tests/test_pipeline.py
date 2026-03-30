"""Unit tests for pipeline stages."""
import json
import pathlib
import pytest
from unittest.mock import MagicMock
from lineage.tracker import LineageTracker
from pipeline.lineage_emitter import LineageEmitter
from pipeline.stages import mock_provider, validate, transform, aggregate


def make_emitter(stage: str) -> LineageEmitter:
    tracker = LineageTracker(run_id="test")
    return LineageEmitter(tracker, stage=stage)


def test_provider_generates_records(tmp_path, monkeypatch):
    out = tmp_path / "raw" / "interest_rates.json"
    monkeypatch.setattr(mock_provider, "OUT_PATH", out)
    emitter = make_emitter("PROVIDER")
    records = mock_provider.run(emitter, count=10, seed=1)
    assert len(records) == 10
    for r in records:
        assert "record_id" in r
        assert "rate_type" in r
        assert "value" in r
        assert r["rate_type"] in {"SOFR", "LIBOR", "FED_FUNDS_RATE"}
    assert out.exists()


def test_validation_passes_valid_record(mem_db):
    # Insert a valid bronze record
    mem_db.execute("""
        CREATE TABLE bronze_rates (
            record_id VARCHAR, source VARCHAR, rate_type VARCHAR, value DOUBLE,
            currency VARCHAR, tenor VARCHAR, effective_date VARCHAR,
            fetched_at VARCHAR, ingested_at VARCHAR, source_file VARCHAR
        )
    """)
    mem_db.execute("INSERT INTO bronze_rates VALUES ('r1','src','SOFR',0.05,'USD','overnight','2024-01-15','t','t','f')")
    emitter = make_emitter("SILVER")
    valid, invalid = validate.run(mem_db, emitter)
    assert len(valid) == 1
    assert len(invalid) == 0
    assert valid[0]["is_valid"] is True


def test_validation_flags_invalid_value(mem_db):
    mem_db.execute("""
        CREATE TABLE bronze_rates (
            record_id VARCHAR, source VARCHAR, rate_type VARCHAR, value DOUBLE,
            currency VARCHAR, tenor VARCHAR, effective_date VARCHAR,
            fetched_at VARCHAR, ingested_at VARCHAR, source_file VARCHAR
        )
    """)
    mem_db.execute("INSERT INTO bronze_rates VALUES ('r2','src','SOFR',0.45,'USD','overnight','2024-01-15','t','t','f')")
    emitter = make_emitter("SILVER")
    valid, invalid = validate.run(mem_db, emitter)
    assert len(invalid) == 1
    assert "value_out_of_range" in invalid[0]["validation_errors"]


def test_transform_bps_and_pct(mem_db):
    mem_db.execute("""
        CREATE TABLE silver_rates (
            record_id VARCHAR PRIMARY KEY, source VARCHAR, rate_type VARCHAR,
            value DOUBLE, currency VARCHAR, tenor VARCHAR, effective_date VARCHAR,
            fetched_at VARCHAR, ingested_at VARCHAR, is_valid BOOLEAN,
            validation_errors VARCHAR, value_bps DOUBLE, value_pct DOUBLE,
            rate_category VARCHAR, tenor_normalized VARCHAR, validated_at VARCHAR
        )
    """)
    record = {
        "record_id": "r1", "source": "s", "rate_type": "SOFR", "value": 0.05,
        "currency": "USD", "tenor": "overnight", "effective_date": "2024-01-15",
        "fetched_at": "t", "ingested_at": "t", "is_valid": True,
        "validation_errors": [], "validated_at": "t"
    }
    emitter = make_emitter("SILVER")
    results = transform.run(mem_db, emitter, [record])
    assert len(results) == 1
    r = results[0]
    assert r["value_bps"] == pytest.approx(500.0)
    assert r["value_pct"] == pytest.approx(5.0)
    assert r["rate_category"] == "MEDIUM"
    assert r["tenor_normalized"] == "ON"


def test_transform_rate_categories(mem_db):
    from pipeline.stages.transform import _rate_category
    assert _rate_category(0.01) == "LOW"
    assert _rate_category(0.03) == "MEDIUM"
    assert _rate_category(0.06) == "HIGH"


def test_transform_tenor_normalization():
    from pipeline.stages.transform import TENOR_MAP
    assert TENOR_MAP["overnight"] == "ON"
    assert TENOR_MAP["1M"] == "1M"
    assert TENOR_MAP["3M"] == "3M"


# --- Fix 4: aggregate.py wires SILVER->GOLD lineage edges ---

def test_aggregate_wires_silver_gold_edges(mem_db):
    """After run_aggregate, lineage graph must contain SILVER->GOLD edges."""
    # Build silver_rates table with one valid record
    mem_db.execute("""
        CREATE TABLE silver_rates (
            record_id VARCHAR, source VARCHAR, rate_type VARCHAR, value DOUBLE,
            currency VARCHAR, tenor VARCHAR, effective_date VARCHAR,
            fetched_at VARCHAR, ingested_at VARCHAR, is_valid BOOLEAN,
            validation_errors VARCHAR, value_bps DOUBLE, value_pct DOUBLE,
            rate_category VARCHAR, tenor_normalized VARCHAR, validated_at VARCHAR
        )
    """)
    mem_db.execute("""
        INSERT INTO silver_rates VALUES
        ('rec-001','src','SOFR',0.05,'USD','overnight','2024-01-15',
         't','t',TRUE,'[]',500.0,5.0,'MEDIUM','ON','t')
    """)

    tracker = LineageTracker(run_id="test")
    emitter = LineageEmitter(tracker, stage="GOLD")

    # Emit a SILVER node for rec-001 so get_latest_node_id can resolve it
    silver_node_id = tracker.record_event(
        stage="SILVER", operation="TRANSFORM",
        data_type="TRANSFORMED", label="SILVER:SOFR:ON", record_id="rec-001"
    )

    aggregate.run(mem_db, emitter)

    graph = tracker.get_graph()
    gold_nodes = [n for n in graph["nodes"] if n["data_type"] == "AGGREGATED"]
    assert len(gold_nodes) >= 1, "aggregate stage should produce at least one GOLD node"

    gold_node_ids = {n["node_id"] for n in gold_nodes}
    silver_to_gold_edges = [
        e for e in graph["edges"]
        if e["source_node_id"] == silver_node_id
        and e["target_node_id"] in gold_node_ids
    ]
    assert len(silver_to_gold_edges) >= 1, (
        f"Expected SILVER->GOLD edge from {silver_node_id}, "
        f"but found none. Edges: {graph['edges']}"
    )
