"""Tool: get_node_details — return full metadata for a specific lineage node_id."""

SCHEMA = {
    "name": "get_node_details",
    "description": "Returns the complete metadata for a specific lineage node, including its stage, data type, label, record_id, attributes, and timestamps. Also returns immediately connected parent and child nodes. The node_id can be a UUID or a partial label string (e.g. 'FED_FUNDS_RATE/6M', 'SOFR', 'GOLD:SOFR') — the tool will search by label if no exact UUID match is found.",
    "parameters": {
        "type": "object",
        "properties": {
            "node_id": {
                "type": "string",
                "description": "The node_id (UUID) or a partial label string such as a rate type, tenor, or stage prefix (e.g. 'FED_FUNDS_RATE/6M', 'SILVER:SOFR', 'GOLD').",
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

    node = nodes_by_id[node_id]
    edges = graph.get("edges", [])

    parents = [
        nodes_by_id[e["source_node_id"]]
        for e in edges
        if e["target_node_id"] == node_id and e["source_node_id"] in nodes_by_id
    ]
    children = [
        nodes_by_id[e["target_node_id"]]
        for e in edges
        if e["source_node_id"] == node_id and e["target_node_id"] in nodes_by_id
    ]

    return {
        "node": node,
        "parents": parents,
        "children": children,
    }
