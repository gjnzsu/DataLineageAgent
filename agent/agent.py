"""Data Lineage Agent — OpenAI gpt-4o REPL with function calling."""
import json
import os
import pathlib
import time

from dotenv import load_dotenv
from openai import OpenAI

from lineage.store import read_lineage_store
from agent.tool_registry import TOOLS, dispatch
from observability.metrics import (
    agent_tool_calls_total,
    agent_response_latency_seconds,
    agent_questions_total,
)

load_dotenv()

METRICS_PATH = pathlib.Path(__file__).parent.parent / "data" / "metrics_report.json"
SESSION_LOG_PATH = pathlib.Path(__file__).parent.parent / "data" / "agent_session_log.json"
MODEL = "gpt-4o"

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


def _load_metrics() -> dict | None:
    if METRICS_PATH.exists():
        with open(METRICS_PATH, "r", encoding="utf-8") as f:
            return json.load(f)
    return None


def _save_session_log(session_log: list) -> None:
    SESSION_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(SESSION_LOG_PATH, "w", encoding="utf-8") as f:
        json.dump(session_log, f, indent=2)


def run() -> None:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        print("ERROR: OPENAI_API_KEY not set. Add it to your .env file.")
        return

    print("Loading lineage store...")
    try:
        graph = read_lineage_store()
    except FileNotFoundError:
        print("ERROR: No lineage data found. Run the pipeline first: python -m pipeline.run_pipeline")
        return
    metrics = _load_metrics()

    node_count = len(graph.get("nodes", []))
    edge_count = len(graph.get("edges", []))
    print(f"Lineage graph loaded: {node_count} nodes, {edge_count} edges")
    if metrics:
        p = metrics.get("pipeline", {})
        print(f"Last run: {p.get('total_records_produced',0)} records, "
              f"{p.get('valid_record_count',0)} valid, "
              f"{p.get('invalid_record_count',0)} invalid")

    print(f"\nData Lineage Agent ready (model: {MODEL})")
    print("Type your question, or 'quit' to exit.\n")

    client = OpenAI(api_key=api_key)
    messages = [{"role": "system", "content": SYSTEM_PROMPT}]
    session_log = []

    while True:
        try:
            user_input = input("You: ").strip()
        except (EOFError, KeyboardInterrupt):
            break

        if not user_input or user_input.lower() in ("quit", "exit", "q"):
            break

        messages.append({"role": "user", "content": user_input})
        start_time = time.time()
        turn_log = {"question": user_input, "tool_calls": [], "latency_seconds": 0}

        # Agentic loop: keep calling until no more tool_calls (max 10 iterations)
        max_iterations = 10
        for _iteration in range(max_iterations):
            response = client.chat.completions.create(
                model=MODEL,
                messages=messages,
                tools=TOOLS,
                tool_choice="auto",
            )
            msg = response.choices[0].message
            messages.append(msg)

            if not msg.tool_calls:
                # Final text response
                print(f"\nAgent: {msg.content}\n")
                break

            # Process all tool calls in this response
            for tc in msg.tool_calls:
                tool_name = tc.function.name
                args = {}
                try:
                    args = json.loads(tc.function.arguments)
                    result = dispatch(tool_name, args, graph, metrics)
                    status = "success"
                except Exception as e:
                    result = {"error": str(e)}
                    status = "error"

                agent_tool_calls_total.labels(tool_name=tool_name, status=status).inc()
                turn_log["tool_calls"].append({"tool": tool_name, "args": args, "status": status})

                messages.append({
                    "role": "tool",
                    "tool_call_id": tc.id,
                    "content": json.dumps(result),
                })
        else:
            print("\n[Agent] Max tool call iterations reached. Please rephrase your question.\n")

        latency = round(time.time() - start_time, 3)
        turn_log["latency_seconds"] = latency
        session_log.append(turn_log)
        agent_questions_total.inc()

        with agent_response_latency_seconds.time():
            pass  # already measured above; record histogram

        _save_session_log(session_log)

    print("Session ended. Log saved to data/agent_session_log.json")


if __name__ == "__main__":
    run()
