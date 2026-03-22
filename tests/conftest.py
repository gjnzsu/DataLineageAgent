"""Shared pytest fixtures."""
import json
import pathlib
import tempfile
import pytest
import duckdb


@pytest.fixture
def sample_graph():
    """Minimal lineage graph for tool tests."""
    return {
        "run_id": "test-run-001",
        "started_at": "2024-01-15T08:00:00+00:00",
        "completed_at": "2024-01-15T08:00:05+00:00",
        "nodes": [
            {"node_id": "n1", "stage": "PROVIDER", "data_type": "RAW",
             "label": "RAW:SOFR:overnight", "record_id": "rec-001",
             "attributes": {"rate_type": "SOFR", "value": 0.05}, "created_at": "2024-01-15T08:00:01+00:00"},
            {"node_id": "n2", "stage": "BRONZE", "data_type": "INGESTED",
             "label": "BRONZE:SOFR:overnight", "record_id": "rec-001",
             "attributes": {"ingested_at": "2024-01-15T08:00:02+00:00"}, "created_at": "2024-01-15T08:00:02+00:00"},
            {"node_id": "n3", "stage": "SILVER", "data_type": "VALIDATED",
             "label": "SILVER:VALIDATED:SOFR:overnight", "record_id": "rec-001",
             "attributes": {"is_valid": True, "errors": []}, "created_at": "2024-01-15T08:00:03+00:00"},
            {"node_id": "n4", "stage": "SILVER", "data_type": "TRANSFORMED",
             "label": "TRANSFORM:BPS:rec-001", "record_id": "rec-001",
             "attributes": {"transformation": "BPS_CONVERSION", "value_bps": 500.0}, "created_at": "2024-01-15T08:00:03+00:00"},
            {"node_id": "n5", "stage": "SILVER", "data_type": "TRANSFORMED",
             "label": "TRANSFORM:PCT:rec-001", "record_id": "rec-001",
             "attributes": {"transformation": "PCT_CONVERSION", "value_pct": 5.0}, "created_at": "2024-01-15T08:00:03+00:00"},
            {"node_id": "n6", "stage": "SILVER", "data_type": "TRANSFORMED",
             "label": "TRANSFORM:CATEGORY:rec-001", "record_id": "rec-001",
             "attributes": {"transformation": "RATE_CLASSIFICATION", "rate_category": "HIGH"}, "created_at": "2024-01-15T08:00:03+00:00"},
            {"node_id": "n7", "stage": "SILVER", "data_type": "TRANSFORMED",
             "label": "TRANSFORM:TENOR:rec-001", "record_id": "rec-001",
             "attributes": {"transformation": "TENOR_NORMALIZATION", "tenor_normalized": "ON"}, "created_at": "2024-01-15T08:00:03+00:00"},
            {"node_id": "n8", "stage": "GOLD", "data_type": "AGGREGATED",
             "label": "GOLD:SOFR:ON:2024-01-15", "record_id": "agg-001",
             "attributes": {"avg_value": 0.05, "record_count": 1}, "created_at": "2024-01-15T08:00:04+00:00"},
        ],
        "edges": [
            {"edge_id": "e1", "source_node_id": "n1", "target_node_id": "n2", "operation": "INGEST"},
            {"edge_id": "e2", "source_node_id": "n2", "target_node_id": "n3", "operation": "VALIDATE"},
            {"edge_id": "e3", "source_node_id": "n3", "target_node_id": "n4", "operation": "TRANSFORM"},
            {"edge_id": "e4", "source_node_id": "n4", "target_node_id": "n5", "operation": "TRANSFORM"},
            {"edge_id": "e5", "source_node_id": "n5", "target_node_id": "n6", "operation": "TRANSFORM"},
            {"edge_id": "e6", "source_node_id": "n6", "target_node_id": "n7", "operation": "TRANSFORM"},
            {"edge_id": "e7", "source_node_id": "n7", "target_node_id": "n8", "operation": "AGGREGATE"},
        ],
    }


@pytest.fixture
def sample_metrics():
    return {
        "run_id": "test-run-001",
        "pipeline": {
            "total_records_produced": 50,
            "records_ingested": 50,
            "valid_record_count": 45,
            "invalid_record_count": 5,
            "gold_aggregation_count": 12,
            "records_per_second": 25.0,
            "total_duration_seconds": 2.0,
            "stage_duration_seconds": {"provider": 0.1, "ingest": 0.3, "validate": 0.4, "transform": 0.8, "aggregate": 0.4},
        },
        "lineage": {"total_nodes": 8, "total_edges": 7, "orphan_nodes": 0},
    }


@pytest.fixture
def temp_lineage_store(tmp_path, sample_graph):
    """Write sample graph to a temp file and patch the store path."""
    store_file = tmp_path / "lineage_store.json"
    store_file.write_text(json.dumps(sample_graph))
    return store_file


@pytest.fixture
def mem_db():
    """In-memory DuckDB connection for pipeline stage tests."""
    conn = duckdb.connect(":memory:")
    yield conn
    conn.close()
