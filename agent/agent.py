"""Data Lineage Agent — OpenAI gpt-4o REPL with function calling."""
import os
import json
import pathlib
import time

from dotenv import load_dotenv

from lineage.store import read_lineage_store
from agent.chat import chat_turn
from observability.metrics import (
    agent_response_latency_seconds,
    agent_questions_total,
)

load_dotenv()

METRICS_PATH = pathlib.Path(__file__).parent.parent / "data" / "metrics_report.json"
SESSION_LOG_PATH = pathlib.Path(__file__).parent.parent / "data" / "agent_session_log.json"


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

    print("\nData Lineage Agent ready (model: gpt-4o)")
    print("Type your question, or 'quit' to exit.\n")

    messages: list[dict] = []
    session_log: list[dict] = []

    while True:
        try:
            user_input = input("You: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nExiting.")
            break

        if not user_input:
            continue
        if user_input.lower() in {"quit", "exit", "q"}:
            break

        start_time = time.time()
        turn_log = {"question": user_input, "tool_calls": [], "latency_seconds": 0}

        try:
            answer, messages = chat_turn(messages, user_input, graph, metrics)
        except Exception as e:
            print(f"\n[Agent] Error: {e}\n")
            continue

        print(f"\n[Agent]: {answer}\n")

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
