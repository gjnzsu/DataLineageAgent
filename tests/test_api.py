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


def test_chat_returns_answer(client, monkeypatch):
    """POST /api/chat returns answer and updated messages."""
    from unittest.mock import MagicMock, patch

    mock_resp = MagicMock()
    mock_resp.choices[0].message.content = "The pipeline has 8 nodes."
    mock_resp.choices[0].message.tool_calls = None

    with patch("agent.chat.OpenAI") as MockOpenAI:
        instance = MockOpenAI.return_value
        instance.chat.completions.create.return_value = mock_resp
        response = client.post("/api/chat", json={
            "messages": [],
            "question": "How many nodes?"
        })

    assert response.status_code == 200
    data = response.json()
    assert "answer" in data
    assert "messages" in data
    assert data["answer"] == "The pipeline has 8 nodes."


def test_chat_empty_question_rejected(client):
    """POST /api/chat with empty question returns 422."""
    response = client.post("/api/chat", json={"messages": [], "question": ""})
    assert response.status_code == 422


def test_chat_missing_lineage_store(monkeypatch, tmp_path):
    """POST /api/chat returns 503 when lineage store is missing."""
    import lineage.store as store_module
    monkeypatch.setattr(store_module, "STORE_PATH", tmp_path / "missing.json")
    from api.main import app
    from fastapi.testclient import TestClient
    c = TestClient(app)
    response = c.post("/api/chat", json={"messages": [], "question": "Hello"})
    assert response.status_code == 503
