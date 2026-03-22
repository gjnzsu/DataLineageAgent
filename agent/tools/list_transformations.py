"""Tool: list_transformations — list all TRANSFORM lineage nodes for a given record_id."""

SCHEMA = {
    "name": "list_transformations",
    "description": "Returns all transformation steps applied to a specific record, in chronological order. Each step shows which transformation was applied (BPS_CONVERSION, PCT_CONVERSION, RATE_CLASSIFICATION, TENOR_NORMALIZATION) and its output attributes.",
    "parameters": {
        "type": "object",
        "properties": {
            "record_id": {
                "type": "string",
                "description": "The record_id to list transformations for.",
            }
        },
        "required": ["record_id"],
    },
}


def execute(graph: dict, record_id: str) -> dict:
    transform_nodes = [
        n for n in graph.get("nodes", [])
        if n.get("record_id") == record_id and n.get("data_type") == "TRANSFORMED"
    ]

    if not transform_nodes:
        return {
            "error": f"No transformation nodes found for record_id '{record_id}'. "
                     "The record may not exist or may have failed validation."
        }

    transform_nodes_sorted = sorted(transform_nodes, key=lambda n: n["created_at"])

    return {
        "record_id": record_id,
        "transformation_count": len(transform_nodes_sorted),
        "transformations": [
            {
                "step": i + 1,
                "node_id": n["node_id"],
                "label": n["label"],
                "transformation": n["attributes"].get("transformation"),
                "attributes": n["attributes"],
                "applied_at": n["created_at"],
            }
            for i, n in enumerate(transform_nodes_sorted)
        ],
    }
