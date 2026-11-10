from __future__ import annotations

from typing import Annotated, Any, TypedDict

from langchain_core.messages import AnyMessage
from langgraph.graph.message import add_messages


def _last_nonempty(old: str, new: str) -> str:
    """Keep the latest non-empty update when parallel writes target the same key."""

    return new if new else old


class OverrideAgentState(TypedDict, total=False):
    """State container for the override supervisor workflow."""

    messages: Annotated[list[AnyMessage], add_messages]
    base_override_token: Annotated[str, _last_nonempty]
    live_override_toml: Annotated[str, _last_nonempty]
    # Single-stanza preview from generate_and_validate_toml; distinct from the merged draft_toml.
    generated_stanza_toml: str
    draft_toml: Annotated[str, _last_nonempty]
    conflict_report: dict[str, Any] | str

    override_token: Annotated[str, _last_nonempty]
    git_leroy_output_token: Annotated[str, _last_nonempty]

    # Written by code (never by the model) once a diff report was actually
    # rendered for the user. Gates the production deploy tool.
    offline_diff_report_shown: bool


if __name__ == "__main__":
    from langgraph.graph import END, START, StateGraph

    def node_a(_: OverrideAgentState) -> dict[str, str]:
        return {"draft_toml": "toml_A"}

    def node_b(_: OverrideAgentState) -> dict[str, str]:
        return {"draft_toml": "toml_B"}

    graph = StateGraph(OverrideAgentState)
    graph.add_node("node_a", node_a)
    graph.add_node("node_b", node_b)
    graph.add_edge(START, "node_a")
    graph.add_edge(START, "node_b")
    graph.add_edge("node_a", END)
    graph.add_edge("node_b", END)

    app = graph.compile()
    final_state = app.invoke({})
    print(final_state)
