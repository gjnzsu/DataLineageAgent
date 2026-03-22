"""Collects all tool schemas and dispatch map for the OpenAI function-calling agent."""
from agent.tools import (
    get_pipeline_summary,
    get_record_lineage,
    get_downstream,
    list_transformations,
    get_node_details,
)

# OpenAI tools format
TOOLS = [
    {"type": "function", "function": get_pipeline_summary.SCHEMA},
    {"type": "function", "function": get_record_lineage.SCHEMA},
    {"type": "function", "function": get_downstream.SCHEMA},
    {"type": "function", "function": list_transformations.SCHEMA},
    {"type": "function", "function": get_node_details.SCHEMA},
]


def dispatch(tool_name: str, args: dict, graph: dict, metrics: dict | None) -> dict:
    """Route a tool call to the matching function and return its result."""
    if tool_name == "get_pipeline_summary":
        return get_pipeline_summary.execute(graph, metrics, **args)
    elif tool_name == "get_record_lineage":
        return get_record_lineage.execute(graph, **args)
    elif tool_name == "get_downstream":
        return get_downstream.execute(graph, **args)
    elif tool_name == "list_transformations":
        return list_transformations.execute(graph, **args)
    elif tool_name == "get_node_details":
        return get_node_details.execute(graph, **args)
    else:
        return {"error": f"Unknown tool: {tool_name}"}
