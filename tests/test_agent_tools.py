"""Unit tests for agent tool functions (no OpenAI API required)."""
import pytest
from agent.tools import (
    get_pipeline_summary,
    get_record_lineage,
    get_downstream,
    list_transformations,
    get_node_details,
)


def test_get_pipeline_summary(sample_graph, sample_metrics):
    result = get_pipeline_summary.execute(sample_graph, sample_metrics)
    assert result["run_id"] == "test-run-001"
    assert result["lineage"]["total_nodes"] == 8
    assert result["pipeline"]["valid_record_count"] == 45
    assert result["pipeline"]["invalid_record_count"] == 5


def test_get_pipeline_summary_no_metrics(sample_graph):
    result = get_pipeline_summary.execute(sample_graph, None)
    assert "lineage" in result
    assert "pipeline" not in result


def test_get_pipeline_summary_empty_graph():
    result = get_pipeline_summary.execute({}, None)
    assert "error" in result


def test_get_record_lineage_returns_chain(sample_graph):
    result = get_record_lineage.execute(sample_graph, "rec-001")
    assert result["record_id"] == "rec-001"
    assert result["chain_length"] >= 2
    # First node should be RAW
    assert result["lineage_chain"][0]["data_type"] == "RAW"


def test_get_record_lineage_unknown_id(sample_graph):
    result = get_record_lineage.execute(sample_graph, "nonexistent-id")
    assert "error" in result


def test_get_downstream(sample_graph):
    result = get_downstream.execute(sample_graph, "n1")
    assert result["node_id"] == "n1"
    assert result["downstream_count"] >= 1
    downstream_ids = [n["node_id"] for n in result["downstream_nodes"]]
    assert "n2" in downstream_ids


def test_get_downstream_unknown_node(sample_graph):
    result = get_downstream.execute(sample_graph, "nonexistent-node")
    assert "error" in result


def test_list_transformations(sample_graph):
    result = list_transformations.execute(sample_graph, "rec-001")
    assert result["record_id"] == "rec-001"
    assert result["transformation_count"] == 4
    names = [t["transformation"] for t in result["transformations"]]
    assert "BPS_CONVERSION" in names
    assert "PCT_CONVERSION" in names
    assert "RATE_CLASSIFICATION" in names
    assert "TENOR_NORMALIZATION" in names


def test_list_transformations_unknown_record(sample_graph):
    result = list_transformations.execute(sample_graph, "bad-id")
    assert "error" in result


def test_get_node_details(sample_graph):
    result = get_node_details.execute(sample_graph, "n2")
    assert result["node"]["node_id"] == "n2"
    assert result["node"]["stage"] == "BRONZE"
    assert len(result["parents"]) == 1
    assert result["parents"][0]["node_id"] == "n1"


def test_get_node_details_unknown(sample_graph):
    result = get_node_details.execute(sample_graph, "bad-node")
    assert "error" in result


# --- Fix 1: label-match fallback in get_downstream ---

def test_get_downstream_by_label_single_match(sample_graph):
    # "RAW:SOFR:overnight" is the full label of n1, unique in the fixture graph
    result = get_downstream.execute(sample_graph, "RAW:SOFR:overnight")
    assert "error" not in result
    assert result["node_id"] == "n1"
    downstream_ids = [n["node_id"] for n in result["downstream_nodes"]]
    assert "n2" in downstream_ids


def test_get_downstream_by_label_ambiguous(sample_graph):
    # "SOFR:overnight" matches n1, n2, n3 (multiple nodes) — disambiguation hint
    result = get_downstream.execute(sample_graph, "SOFR:overnight")
    assert "matches" in result
    assert "hint" in result
    assert len(result["matches"]) >= 2


def test_get_downstream_by_label_too_many_matches():
    nodes = [
        {"node_id": f"x{i}", "stage": "SILVER", "data_type": "TRANSFORMED",
         "label": f"foo:bar:{i}", "record_id": f"rec-{i}", "attributes": {}}
        for i in range(6)
    ]
    graph = {"run_id": "t", "nodes": nodes, "edges": []}
    result = get_downstream.execute(graph, "foo")
    assert "error" in result
    assert "too many" in result["error"].lower()
    assert "sample_labels" in result


# --- Fix 2: record_id disambiguation ---

def test_get_downstream_record_id_disambiguates():
    # Two records share the same label prefix; record_id should narrow to correct node
    nodes = [
        {"node_id": "a1", "stage": "PROVIDER", "data_type": "RAW",
         "label": "RAW:SOFR:overnight", "record_id": "rec-001", "attributes": {}},
        {"node_id": "a2", "stage": "PROVIDER", "data_type": "RAW",
         "label": "RAW:SOFR:overnight", "record_id": "rec-002", "attributes": {}},
        {"node_id": "a3", "stage": "BRONZE", "data_type": "INGESTED",
         "label": "BRONZE:SOFR:overnight", "record_id": "rec-001", "attributes": {}},
    ]
    edges = [
        {"edge_id": "e1", "source_node_id": "a1", "target_node_id": "a3", "operation": "INGEST"},
    ]
    graph = {"run_id": "t", "nodes": nodes, "edges": edges}
    result = get_downstream.execute(graph, "RAW:SOFR:overnight", record_id="rec-001")
    assert "error" not in result
    assert result["node_id"] == "a1"
    downstream_ids = [n["node_id"] for n in result["downstream_nodes"]]
    assert "a3" in downstream_ids


def test_get_downstream_record_id_no_match_falls_back_to_all():
    # record_id provided but no node matches it — falls back to unfiltered matches
    nodes = [
        {"node_id": "b1", "stage": "PROVIDER", "data_type": "RAW",
         "label": "RAW:SOFR:overnight", "record_id": "rec-001", "attributes": {}},
    ]
    graph = {"run_id": "t", "nodes": nodes, "edges": []}
    # record_id "rec-999" doesn't exist — should fall back to single match on rec-001
    result = get_downstream.execute(graph, "RAW:SOFR:overnight", record_id="rec-999")
    # Falls back to unfiltered: 1 match -> resolves to b1
    assert "error" not in result
    assert result["node_id"] == "b1"

