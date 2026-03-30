"""Tool: get_downstream — find all nodes downstream of a given node_id."""
from collections import deque

SCHEMA = {
    "name": "get_downstream",
    "description": "Finds all lineage nodes that are downstream (dependent on) a given node_id. Useful for impact analysis — e.g., what would be affected if this node changed.",
    "parameters": {
        "type": "object",
        "properties": {
            "node_id": {
                "type": "string",
                "description": "The node_id (UUID) or a partial label string such as a rate type, tenor, or stage prefix (e.g. 'FED_FUNDS_RATE/6M', 'SILVER:SOFR'). Label search is used if no exact UUID match is found.",
            }
        },
        "required": ["node_id"],
    },
}


def execute(graph: dict, node_id: str) -> dict:
    nodes = graph.get("nodes", [])
    nodes_by_id = {n["node_id"]: n for n in nodes}

    if node_id not in nodes_by_id:
        # Fall back to label/attribute search (case-insensitive, partial match)
        query = node_id.replace("/", ":").lower()
        matches = [
            n for n in nodes
            if query in n.get("label", "").lower()
            or query in n.get("data_type", "").lower()
            or query in str(n.get("attributes", {})).lower()
        ]
        if not matches:
            return {"error": f"Node '{node_id}' not found. No label or attribute match either."}
        if len(matches) > 5:
            return {
                "error": f"'{node_id}' matched {len(matches)} nodes — too many results. Be more specific.",
                "sample_labels": [m["label"] for m in matches[:10]],
            }
        if len(matches) > 1:
            return {
                "matches": [{"node_id": m["node_id"], "label": m["label"], "stage": m["stage"]} for m in matches],
                "hint": "Multiple nodes matched. Use the exact node_id UUID from this list.",
            }
        node_id = matches[0]["node_id"]

    # Build children map: parent_node_id -> [child_node_id, ...]
    children_map: dict[str, list[str]] = {}
    for e in graph.get("edges", []):
        src = e["source_node_id"]
        tgt = e["target_node_id"]
        children_map.setdefault(src, []).append(tgt)

    # BFS downstream
    visited = set()
    queue = deque([node_id])
    downstream = []

    while queue:
        current = queue.popleft()
        if current in visited:
            continue
        visited.add(current)
        if current != node_id:  # exclude the start node itself
            node = nodes_by_id.get(current)
            if node:
                downstream.append(node)
        for child in children_map.get(current, []):
            queue.append(child)

    return {
        "node_id": node_id,
        "start_node": nodes_by_id[node_id],
        "downstream_count": len(downstream),
        "downstream_nodes": downstream,
    }
