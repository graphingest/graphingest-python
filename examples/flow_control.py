"""
GraphIngest Flow Control — Concurrency, Throttling, Priority

Scenario: You run a SaaS platform where users upload documents for AI processing.
Without flow control, one user uploading 500 docs starves everyone else.

This example shows:
    1. Per-user concurrency limits (max 5 parallel runs per user)
    2. Throttling (max 100 runs per minute globally)
    3. Priority queuing (paid users go first)

Run:
    pip install graphingest
    python flow_control.py
"""

from graphingest import node, graph, deploy, RetryPolicy, ConcurrencyPolicy, ThrottlePolicy


# ── Nodes ──

@node(name="process-document", max_retries=3, timeout_seconds=120)
def process_document(doc: dict) -> dict:
    """Process a single document with AI."""
    # In production: call OpenAI, extract entities, classify, etc.
    return {
        "doc_id": doc["id"],
        "title": doc.get("title", "Untitled"),
        "word_count": len(doc.get("content", "").split()),
        "status": "processed",
    }


@node(name="notify-user")
def notify_user(result: dict) -> dict:
    """Send notification to user when processing is done."""
    return {"notified": True, "doc_id": result["doc_id"]}


# ── Pipeline with flow control ──

@graph(
    name="process-upload",
    # Concurrency: max 5 parallel runs per user
    concurrency=ConcurrencyPolicy(
        limit=5,
        key="user_id",  # each user gets their own 5-slot pool
        wait_timeout_seconds=30,  # wait up to 30s for a slot
    ),
    # Throttle: max 100 runs per minute (protects your OpenAI budget)
    throttle=ThrottlePolicy(
        limit=100,
        period_seconds=60,
    ),
    # Priority: higher number = processed first
    priority=10,  # paid users get priority=10, free users get priority=1
    retry_policy=RetryPolicy(max_retries=2, delay_seconds=1),
    timeout_seconds=300,
)
def process_upload(user_id: str, documents: list[dict]):
    """
    Process uploaded documents for a user.

    Flow control ensures:
    - No user can run more than 5 pipelines at once
    - Total system throughput is capped at 100/min
    - Paid users' jobs run before free users' jobs
    """
    # Fan-out: process all documents in parallel
    results = process_document.map(documents)

    # Notify user
    for result in results:
        notify_user(result)

    return {
        "user_id": user_id,
        "processed": len(results),
        "results": results,
    }


# ── Run ──

if __name__ == "__main__":
    deploy()

    # Simulate a user uploading 10 documents
    docs = [
        {"id": f"doc-{i}", "title": f"Document {i}", "content": f"Content of document {i} " * 50}
        for i in range(10)
    ]

    result = process_upload(user_id="user-123", documents=docs)
    print(f"Processed {result['processed']} documents for {result['user_id']}")
