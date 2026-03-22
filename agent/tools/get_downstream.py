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
                "description": "The node_id to find downstream dependents for.",
            }
        },
        "required": ["node_id"],
    },
}


def execute(graph: dict, node_id: str) -> dict:
    nodes_by_id = {n["node_id"]: n for n in graph.get("nodes", [])}

    if node_id not in nodes_by_id:
        return {"error": f"Node '{node_id}' not found in lineage graph."}

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
