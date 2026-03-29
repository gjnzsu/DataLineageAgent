# Chatbot Feature Design — Data Lineage Agent

**Date:** 2026-03-29
**Status:** Approved
**Topic:** Expose the gpt-4o lineage agent as an in-browser chat panel

---

## Problem

The OpenAI gpt-4o lineage agent (`agent/agent.py`) is CLI-only. The GKE deployment runs only `uvicorn api.main:app`, so the chatbot is unreachable from the browser. Users cannot query data lineage or data governance information via the UI.

---

## Goals

- Expose the existing 5-tool lineage agent as a `POST /api/chat` HTTP endpoint
- Add a bottom-drawer chat panel to `ui/index.html`
- Maintain multi-turn conversation history in the browser (session lifetime)
- No changes to the CLI REPL (`agent/agent.py`)
- No new dependencies beyond what is already installed

---

## Architecture

### Approach: Stateless backend, frontend-owned history

The browser holds the full `messages` array and sends it with every request. The server is stateless — no session state, no memory leaks, compatible with GKE Recreate deployment strategy.

```
Browser                          FastAPI (api/main.py)
  │                                      │
  │  POST /api/chat                      │
  │  { messages: [...], question: "..." }│
  │ ─────────────────────────────────▶  │
  │                                      │  agent/chat.py
  │                                      │  ├─ append question to messages
  │                                      │  ├─ load lineage graph + metrics
  │                                      │  ├─ call OpenAI gpt-4o (tool loop)
  │                                      │  └─ return answer + updated messages
  │  { answer: "...", messages: [...] }  │
  │ ◀─────────────────────────────────  │
  │                                      │
  │  (browser stores updated messages)   │
```

---

## Components

### 1. `agent/chat.py` (new)

Extracts the stateless chat logic shared between the CLI and the API:

```python
def chat_turn(messages: list[dict], question: str, graph: dict, metrics: dict | None) -> tuple[str, list[dict]]:
    """Run one conversation turn. Returns (answer, updated_messages)."""
```

- Appends the user question to messages
- Runs the OpenAI tool-call loop (up to 5 iterations)
- Returns the assistant's final answer and the full updated messages list
- Uses the same `SYSTEM_PROMPT`, `TOOLS`, `dispatch()`, and `MODEL` as `agent.py`
- No side effects (no file I/O, no Prometheus metrics — those stay in `agent.py`)

### 2. `api/main.py` — new endpoint

```
POST /api/chat
Content-Type: application/json

Request:  { "messages": [...], "question": "What is downstream of SOFR:overnight?" }
Response: { "answer": "...", "messages": [...] }
Error:    { "error": "..." }  (HTTP 4xx/5xx)
```

- Validates `question` is non-empty (HTTP 422 if blank)
- Loads lineage graph; returns HTTP 503 if pipeline data not found
- Calls `chat_turn()` from `agent/chat.py`
- Returns updated messages so the browser can persist them

### 3. `ui/index.html` — bottom chat drawer

Layout:
```
┌─────────────────────────────────────────────────────────┐
│  [sidebar]  │           [D3 graph]                       │
│             │                                            │
├─────────────┴────────────────────────────────────────────┤
│ ▲ Ask the Lineage Agent                         [Clear]  │
│┌────────────────────────────────────────────────────────┐│
││ Agent: What can I help you with?                       ││
││ You: What is downstream of SOFR:overnight in SILVER?   ││
││ Agent: The downstream nodes are...                     ││
│└────────────────────────────────────────────────────────┘│
│ [Ask a question about the data lineage...      ] [Send]  │
└──────────────────────────────────────────────────────────┘
```

- Collapsible drawer (▲/▼ toggle), expanded by default
- Fixed height ~220px for message area, scrollable
- User messages right-aligned, agent messages left-aligned
- Dark theme matching existing UI (`#0f1117` / `#1a1d2e`)
- Enter key or Send button submits
- Input + Send disabled while request in flight, shows `...` thinking indicator
- Clear button resets `messages` array and clears the display
- On load: greeting message "What can I help you with?"

---

## Data Flow

1. User types question, presses Send or Enter
2. Browser appends question to local `messages` array
3. `POST /api/chat` with full `messages` + `question`
4. Server calls `chat_turn()` → OpenAI tool loop → answer
5. Browser receives `{ answer, messages }`, replaces local `messages`, renders answer
6. Input re-enabled

---

## Error Handling

| Scenario | Server response | UI behavior |
|----------|----------------|-------------|
| OpenAI API error | HTTP 500 `{ error: "..." }` | Red inline message in chat, input re-enabled |
| Lineage data missing | HTTP 503 `{ error: "Pipeline data not available..." }` | Red inline message |
| Empty question | Client-side validation | Send button stays disabled |
| Double-submit | Input disabled during request | Prevented |
| OPENAI_API_KEY missing | HTTP 500 | Red inline message |
| OpenAI context limit | HTTP 500 from OpenAI | User sees error, can click Clear to reset |

---

## Out of Scope

- Streaming / WebSocket (stateless request-response is sufficient)
- Server-side session persistence (browser session is sufficient)
- New data governance tools beyond the existing 5 lineage tools
- Authentication / access control
- Conversation history persisted across page refreshes

---

## Files Changed

| File | Change |
|------|--------|
| `agent/chat.py` | New — stateless chat logic extracted from `agent.py` |
| `api/main.py` | Add `POST /api/chat` endpoint |
| `ui/index.html` | Add bottom chat drawer |
| `agent/agent.py` | Minor refactor to use `chat.py` (no behavior change) |
