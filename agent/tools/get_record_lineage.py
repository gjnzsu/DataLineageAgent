"""Tool: get_record_lineage — trace a record's full lineage chain upstream from its last node."""
from collections import defaultdict

SCHEMA = {
    "name": "get_record_lineage",
    "description": "Traces the complete lineage chain for a given record_id, from its final processed state back to the original RAW source. Returns an ordered list of lineage nodes.",
    "parameters": {
        "type": "object",
        "properties": {
            "record_id": {
                "type": "string",
                "description": "The record_id to trace. This is the UUID assigned to the record when it was produced by the mock provider.",
            }
        },
        "required": ["record_id"],
    },
}


def execute(graph: dict, record_id: str) -> dict:
    nodes_by_id = {n["node_id"]: n for n in graph.get("nodes", [])}
    edges = graph.get("edges", [])

    # Build parent map: child_node_id -> parent_node_id
    parent_map: dict[str, str] = {}
    for e in edges:
        parent_map[e["target_node_id"]] = e["source_node_id"]

    # Find all nodes for this record_id
    record_nodes = [n for n in graph.get("nodes", []) if n.get("record_id") == record_id]
    if not record_nodes:
        return {"error": f"No lineage found for record_id '{record_id}'. Check the ID is correct."}

    # Start from the last node (latest created_at)
    record_nodes_sorted = sorted(record_nodes, key=lambda n: n["created_at"])
    last_node = record_nodes_sorted[-1]

    # Walk upstream via parent_map
    chain = []
    current_id = last_node["node_id"]
    visited = set()

    while current_id and current_id not in visited:
        visited.add(current_id)
        node = nodes_by_id.get(current_id)
        if node:
            chain.append(node)
        current_id = parent_map.get(current_id)

    chain.reverse()  # chronological order: RAW → ... → last

    return {
        "record_id": record_id,
        "chain_length": len(chain),
        "lineage_chain": chain,
    }
