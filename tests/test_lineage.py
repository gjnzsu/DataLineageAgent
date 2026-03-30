"""Unit tests for LineageTracker and store."""
import json
import pytest
from lineage.tracker import LineageTracker


def test_tracker_creates_node_on_event():
    tracker = LineageTracker(run_id="test-run")
    node_id = tracker.record_event(
        stage="PROVIDER", operation="PRODUCE", data_type="RAW",
        label="RAW:SOFR:ON", record_id="rec-1"
    )
    graph = tracker.get_graph()
    assert len(graph["nodes"]) == 1
    assert graph["nodes"][0]["node_id"] == node_id


def test_tracker_creates_edge_on_second_event():
    tracker = LineageTracker(run_id="test-run")
    n1 = tracker.record_event(stage="PROVIDER", operation="PRODUCE",
                               data_type="RAW", label="RAW", record_id="rec-1")
    n2 = tracker.record_event(stage="BRONZE", operation="INGEST",
                               data_type="INGESTED", label="BRONZE", record_id="rec-1")
    graph = tracker.get_graph()
    assert len(graph["edges"]) == 1
    edge = graph["edges"][0]
    assert edge["source_node_id"] == n1
    assert edge["target_node_id"] == n2


def test_no_orphan_nodes_after_chain():
    tracker = LineageTracker(run_id="test-run")
    tracker.record_event(stage="PROVIDER", operation="PRODUCE",
                         data_type="RAW", label="RAW", record_id="r1")
    tracker.record_event(stage="BRONZE", operation="INGEST",
                         data_type="INGESTED", label="BRONZE", record_id="r1")
    tracker.record_event(stage="SILVER", operation="VALIDATE",
                         data_type="VALIDATED", label="SILVER", record_id="r1")
    graph = tracker.get_graph()
    target_ids = {e["target_node_id"] for e in graph["edges"]}
    non_root_nodes = [n for n in graph["nodes"] if n["data_type"] != "RAW"]
    orphans = [n for n in non_root_nodes if n["node_id"] not in target_ids]
    assert len(orphans) == 0


def test_store_roundtrip(tmp_path, monkeypatch, sample_graph):
    store_file = tmp_path / "lineage_store.json"
    import lineage.store as store_module
    monkeypatch.setattr(store_module, "STORE_PATH", store_file)
    store_module.write_lineage_store(sample_graph)
    loaded = store_module.read_lineage_store()
    assert loaded["run_id"] == sample_graph["run_id"]
    assert len(loaded["nodes"]) == len(sample_graph["nodes"])
    assert len(loaded["edges"]) == len(sample_graph["edges"])


def test_read_lineage_store_missing(tmp_path, monkeypatch):
    import lineage.store as store_module
    monkeypatch.setattr(store_module, "STORE_PATH", tmp_path / "missing.json")
    with pytest.raises(FileNotFoundError):
        store_module.read_lineage_store()


# --- Fix 3: parent_node_ids multi-parent edges ---

def test_tracker_parent_node_ids_creates_multiple_edges():
    tracker = LineageTracker(run_id="test-run")
    p1 = tracker.record_event(stage="SILVER", operation="TRANSFORM",
                               data_type="TRANSFORMED", label="S1", record_id="r1")
    p2 = tracker.record_event(stage="SILVER", operation="TRANSFORM",
                               data_type="TRANSFORMED", label="S2", record_id="r2")
    child = tracker.record_event(
        stage="GOLD", operation="AGGREGATE",
        data_type="AGGREGATED", label="GOLD:AGG",
        record_id="agg-1",
        parent_node_ids=[p1, p2],
    )
    graph = tracker.get_graph()
    # Two edges: p1->child and p2->child
    edges_to_child = [e for e in graph["edges"] if e["target_node_id"] == child]
    assert len(edges_to_child) == 2
    source_ids = {e["source_node_id"] for e in edges_to_child}
    assert source_ids == {p1, p2}


def test_tracker_parent_node_ids_takes_precedence_over_chain():
    # When parent_node_ids is supplied, the record-chain parent should NOT also be added
    tracker = LineageTracker(run_id="test-run")
    chain_parent = tracker.record_event(stage="BRONZE", operation="INGEST",
                                         data_type="INGESTED", label="B", record_id="r1")
    explicit_parent = tracker.record_event(stage="SILVER", operation="VALIDATE",
                                            data_type="VALIDATED", label="S", record_id="r2")
    child = tracker.record_event(
        stage="GOLD", operation="AGGREGATE",
        data_type="AGGREGATED", label="G",
        record_id="r1",  # same record_id as chain_parent
        parent_node_ids=[explicit_parent],
    )
    graph = tracker.get_graph()
    edges_to_child = [e for e in graph["edges"] if e["target_node_id"] == child]
    assert len(edges_to_child) == 1
    assert edges_to_child[0]["source_node_id"] == explicit_parent


def test_tracker_get_latest_node_id():
    tracker = LineageTracker(run_id="test-run")
    assert tracker.get_latest_node_id("rec-x") is None
    n1 = tracker.record_event(stage="BRONZE", operation="INGEST",
                               data_type="INGESTED", label="B", record_id="rec-x")
    assert tracker.get_latest_node_id("rec-x") == n1
    n2 = tracker.record_event(stage="SILVER", operation="VALIDATE",
                               data_type="VALIDATED", label="S", record_id="rec-x")
    assert tracker.get_latest_node_id("rec-x") == n2
