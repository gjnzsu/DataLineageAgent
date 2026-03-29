"""Unit tests for agent/chat.py stateless chat logic."""
import pytest
from unittest.mock import MagicMock, patch


@pytest.fixture
def graph(sample_graph):
    return sample_graph


@pytest.fixture
def metrics(sample_metrics):
    return sample_metrics


def _make_openai_response(content: str):
    """Build a minimal mock OpenAI chat completion with no tool calls."""
    msg = MagicMock()
    msg.content = content
    msg.tool_calls = None
    choice = MagicMock()
    choice.message = msg
    resp = MagicMock()
    resp.choices = [choice]
    return resp


def test_chat_turn_returns_answer(graph, metrics):
    """chat_turn returns a non-empty answer string."""
    from agent.chat import chat_turn
    mock_resp = _make_openai_response("The pipeline has 8 nodes.")
    with patch("agent.chat.OpenAI") as MockOpenAI:
        instance = MockOpenAI.return_value
        instance.chat.completions.create.return_value = mock_resp
        answer, updated = chat_turn([], "How many nodes?", graph, metrics)
    assert answer == "The pipeline has 8 nodes."
    assert isinstance(updated, list)


def test_chat_turn_appends_messages(graph, metrics):
    """chat_turn appends user + assistant messages to the thread."""
    from agent.chat import chat_turn
    mock_resp = _make_openai_response("8 nodes total.")
    with patch("agent.chat.OpenAI") as MockOpenAI:
        instance = MockOpenAI.return_value
        instance.chat.completions.create.return_value = mock_resp
        _, updated = chat_turn([], "How many nodes?", graph, metrics)
    roles = [m["role"] for m in updated]
    assert "user" in roles
    assert "assistant" in roles


def test_chat_turn_preserves_prior_messages(graph, metrics):
    """chat_turn appends to existing history without losing prior messages."""
    from agent.chat import chat_turn
    prior = [{"role": "user", "content": "Hello"}, {"role": "assistant", "content": "Hi"}]
    mock_resp = _make_openai_response("8 nodes.")
    with patch("agent.chat.OpenAI") as MockOpenAI:
        instance = MockOpenAI.return_value
        instance.chat.completions.create.return_value = mock_resp
        _, updated = chat_turn(prior, "How many nodes?", graph, metrics)
    assert updated[0] == prior[0]
    assert updated[1] == prior[1]
    assert len(updated) == 4  # prior 2 + user + assistant


def test_chat_turn_executes_tool_call(graph, metrics):
    """chat_turn resolves a tool call and returns final assistant answer."""
    from agent.chat import chat_turn
    import json

    # First response: tool call
    tool_call = MagicMock()
    tool_call.id = "call_abc"
    tool_call.function.name = "get_pipeline_summary"
    tool_call.function.arguments = json.dumps({})
    msg1 = MagicMock()
    msg1.content = None
    msg1.tool_calls = [tool_call]
    msg1.role = "assistant"
    resp1 = MagicMock()
    resp1.choices = [MagicMock(message=msg1)]

    # Second response: final answer
    resp2 = _make_openai_response("Summary: 8 nodes, 7 edges.")

    with patch("agent.chat.OpenAI") as MockOpenAI:
        instance = MockOpenAI.return_value
        instance.chat.completions.create.side_effect = [resp1, resp2]
        answer, updated = chat_turn([], "Summarize the pipeline.", graph, metrics)

    assert "8 nodes" in answer or answer == "Summary: 8 nodes, 7 edges."
    # messages must include: user, assistant tool-call, tool result, final assistant
    assert len(updated) > 2
    roles = [m["role"] for m in updated]
    assert roles.count("tool") >= 1
    assert any(m["role"] == "assistant" and m.get("tool_calls") for m in updated)
