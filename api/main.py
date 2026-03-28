"""FastAPI server: serves lineage data, triggers pipeline, exposes Prometheus /metrics."""
import json
import pathlib

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, Response
from fastapi.staticfiles import StaticFiles
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST

from lineage.store import read_lineage_store
from observability.metrics import REGISTRY

app = FastAPI(title="Data Lineage Agent API", version="1.0.0")

ROOT = pathlib.Path(__file__).parent.parent
UI_PATH = ROOT / "ui" / "index.html"
METRICS_PATH = ROOT / "data" / "metrics_report.json"


@app.get("/", response_class=HTMLResponse)
def serve_ui():
    """Serve the D3.js lineage visualization."""
    if not UI_PATH.exists():
        raise HTTPException(status_code=404, detail="UI not found")
    return UI_PATH.read_text(encoding="utf-8")


@app.get("/health")
def health():
    """Liveness/readiness probe endpoint."""
    return {"status": "ok"}


@app.get("/api/lineage")
def get_lineage():
    """Return the full lineage graph as JSON."""
    try:
        return read_lineage_store()
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))


@app.get("/api/lineage/node/{node_id}")
def get_node(node_id: str):
    """Return a single lineage node by node_id."""
    try:
        graph = read_lineage_store()
    except FileNotFoundError as e:
        raise HTTPException(status_code=404, detail=str(e))

    node = next((n for n in graph.get("nodes", []) if n["node_id"] == node_id), None)
    if not node:
        raise HTTPException(status_code=404, detail=f"Node '{node_id}' not found")
    return node


@app.post("/api/run-pipeline")
def run_pipeline():
    """Trigger the data pipeline programmatically."""
    try:
        from pipeline.run_pipeline import run
        metrics = run()
        return {"status": "success", "metrics": metrics}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/metrics-report")
def get_metrics_report():
    """Return the last pipeline metrics report."""
    if not METRICS_PATH.exists():
        raise HTTPException(status_code=404, detail="No metrics report found. Run the pipeline first.")
    with open(METRICS_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


@app.get("/metrics")
def prometheus_metrics():
    """Prometheus scrape endpoint."""
    return Response(generate_latest(REGISTRY), media_type=CONTENT_TYPE_LATEST)
