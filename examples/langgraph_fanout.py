"""
LangGraph fan-out with a rate limit.

One batch of questions becomes one agent per question. Agents run in
parallel, five at a time, so a long list cannot fire every model call
at once. ThrottlePolicy and ConcurrencyPolicy gate the graph itself:
the next batch waits until the window and a free slot allow it.

Run:
    pip install "graphingest[langgraph]"
    python langgraph_fanout.py

Swap `reason` for a real ChatOpenAI call when you are ready to spend tokens.
The fan-out and the limits stay the same.
"""

from graphingest import graph, deploy, ThrottlePolicy, ConcurrencyPolicy
from graphingest.langgraph import agent_node, AgentConfig


def build_researcher(config: AgentConfig):
    from langgraph.graph import StateGraph, END
    from typing import TypedDict

    class State(TypedDict):
        messages: list

    def reason(state: State) -> dict:
        question = state["messages"][-1]["content"]
        # Replace this with ChatOpenAI(model=config.model).invoke(...)
        answer = f"[{config.model}] notes on: {question}"
        return {"messages": [{"role": "assistant", "content": answer}]}

    builder = StateGraph(State)
    builder.add_node("reason", reason)
    builder.set_entry_point("reason")
    builder.add_edge("reason", END)
    return builder.compile()


researcher = agent_node(
    name="researcher",
    graph_builder=build_researcher,
    config=AgentConfig(
        model="gpt-4o-mini",
        system_prompt="Answer in one paragraph.",
        max_iterations=6,
        stream_steps=False,
    ),
)


@graph(
    name="research-fanout",
    timeout_seconds=3600,
    # Next batch: at most 20 starts of this graph per minute.
    throttle=ThrottlePolicy(limit=20, period_seconds=60),
    # And at most 5 of those graphs in flight. A sixth waits up to 3 minutes.
    concurrency=ConcurrencyPolicy(limit=5, wait_timeout_seconds=180),
)
def research_fanout(queries: list[str]):
    width = 5  # agents in flight inside this run
    results = []
    for start in range(0, len(queries), width):
        results.extend(researcher.map(queries[start : start + width]))
    return results


if __name__ == "__main__":
    deploy()
    answers = research_fanout([
        "What is durable execution?",
        "How do serverless function timeouts work?",
        "What is a sliding-window rate limit?",
    ])
    for answer in answers:
        print(answer)
