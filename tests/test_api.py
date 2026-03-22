"""FastAPI integration tests using TestClient."""
import json
import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch


@pytest.fixture
def client(temp_lineage_store, monkeypatch):
    import lineage.store as store_module
    monkeypatch.setattr(store_module, "STORE_PATH", temp_lineage_store)
    from api.main import app
    return TestClient(app)


def test_get_lineage_returns_200(client):
    response = client.get("/api/lineage")
    assert response.status_code == 200
    data = response.json()
    assert "nodes" in data
    assert "edges" in data
    assert len(data["nodes"]) > 0


def test_get_node_details_endpoint(client, sample_graph):
    node_id = sample_graph["nodes"][0]["node_id"]
    response = client.get(f"/api/lineage/node/{node_id}")
    assert response.status_code == 200
    assert response.json()["node_id"] == node_id


def test_get_lineage_missing_store(monkeypatch, tmp_path):
    import lineage.store as store_module
    monkeypatch.setattr(store_module, "STORE_PATH", tmp_path / "missing.json")
    from api.main import app
    client = TestClient(app)
    response = client.get("/api/lineage")
    assert response.status_code == 404


def test_get_node_unknown_id(client):
    response = client.get("/api/lineage/node/does-not-exist")
    assert response.status_code == 404


def test_prometheus_metrics_endpoint(client):
    response = client.get("/metrics")
    assert response.status_code == 200
    assert b"pipeline_records_total" in response.content or b"# HELP" in response.content
