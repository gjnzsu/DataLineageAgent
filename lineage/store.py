import json
import pathlib

STORE_PATH = pathlib.Path(__file__).parent.parent / "data" / "lineage_store.json"


def read_lineage_store() -> dict:
    if not STORE_PATH.exists():
        raise FileNotFoundError(f"Lineage store not found at {STORE_PATH}. Run the pipeline first.")
    with open(STORE_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def write_lineage_store(graph: dict) -> None:
    STORE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(STORE_PATH, "w", encoding="utf-8") as f:
        json.dump(graph, f, indent=2, default=str)
