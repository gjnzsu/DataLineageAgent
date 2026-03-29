"""Stateless chat logic for one conversation turn — used by CLI and API."""
import json
import os

from openai import OpenAI

from agent.tool_registry import TOOLS, dispatch

MODEL = "gpt-4o"
MAX_TOOL_ITERATIONS = 5

SYSTEM_PROMPT = """You are a Data Lineage Agent for a finance interest rate data pipeline.
You have access to tools that let you query the pipeline's lineage graph.

The pipeline processes SOFR, LIBOR, and FED_FUNDS_RATE data through these stages:
  PROVIDER (RAW) → BRONZE (INGESTED) → SILVER (VALIDATED → TRANSFORMED) → GOLD (AGGREGATED)

Use your tools to answer questions about:
- Where data came from (upstream lineage)
- What transformations were applied
- What is downstream of a given dataset
- Pipeline statistics and data quality
- Specific node details

When presenting results, identify nodes using human-readable identifiers in the format RATE_TYPE:TENOR (e.g. SOFR:overnight, FED_FUNDS_RATE:6M, LIBOR:3M) combined with the stage name. Only show UUIDs (node_id, record_id) if the user explicitly asks for them or they are needed for disambiguation.

Example — instead of:
  Node ID: a00a5f86-b5c1-430f-a3ff-b8d31fc088f6 | SOFR overnight RAW
Say:
  RAW stage — SOFR:overnight | value: 0.0861
"""


def chat_turn(
    messages: list[dict],
    question: str,
    graph: dict,
    metrics: dict | None,
) -> tuple[str, list[dict]]:
    """Run one conversation turn. Returns (answer, updated_messages).

    No side effects: no file I/O, no Prometheus metrics.
    The caller is responsible for those concerns.
    """
    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

    messages = list(messages)  # defensive copy
    messages.append({"role": "user", "content": question})

    full_messages = [{"role": "system", "content": SYSTEM_PROMPT}] + messages

    answer = "I was unable to produce an answer. Please rephrase your question."

    for _ in range(MAX_TOOL_ITERATIONS):
        response = client.chat.completions.create(
            model=MODEL,
            messages=full_messages,
            tools=TOOLS,
            tool_choice="auto",
        )
        msg = response.choices[0].message

        if not msg.tool_calls:
            answer = msg.content or answer
            messages.append({"role": "assistant", "content": answer})
            break

        # Process tool calls
        full_messages.append(msg)
        for tc in msg.tool_calls:
            tool_name = tc.function.name
            try:
                args = json.loads(tc.function.arguments)
                result = dispatch(tool_name, args, graph, metrics)
            except Exception as e:
                result = {"error": str(e)}
            full_messages.append({
                "role": "tool",
                "tool_call_id": tc.id,
                "content": json.dumps(result),
            })

    return answer, messages
