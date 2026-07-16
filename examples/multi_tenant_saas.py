"""
GraphIngest Multi-Tenant SaaS — Complete Example

Scenario: You run an AI content platform. Users submit content for:
    - Summarization
    - Translation
    - SEO optimization

Problems without flow control:
    - Enterprise customer submits 500 articles → starves free users
    - OpenAI rate limits hit → all users fail
    - No way to prioritize paid users

This example solves all three with ConcurrencyPolicy + ThrottlePolicy + priority.

Run:
    pip install graphingest
    python multi_tenant_saas.py
"""

from graphingest import (
    node, graph, deploy,
    RetryPolicy, ConcurrencyPolicy, ThrottlePolicy,
    GraphRunContext,
)
import logging

logger = logging.getLogger(__name__)


# ── Nodes ──

@node(name="summarize-article", cache_ttl=3600, max_retries=3)
def summarize(article: dict) -> dict:
    """Summarize an article using AI. Cached for 1 hour."""
    # In production: call OpenAI/Anthropic
    content = article.get("content", "")
    return {
        "article_id": article["id"],
        "summary": content[:200] + "...",
        "word_count": len(content.split()),
    }


@node(name="translate-article", max_retries=2)
def translate(article: dict) -> dict:
    """Translate an article to target language."""
    return {
        "article_id": article["id"],
        "language": article.get("target_lang", "es"),
        "translated": True,
    }


@node(name="optimize-seo")
def optimize_seo(article: dict) -> dict:
    """Optimize article for SEO."""
    return {
        "article_id": article["id"],
        "seo_score": 85,
        "suggestions": ["Add meta description", "Use more headers"],
    }


# ── Pipeline for free users (low priority, strict limits) ──

@graph(
    name="process-content-free",
    concurrency=ConcurrencyPolicy(limit=2, key="user_id"),  # max 2 parallel
    throttle=ThrottlePolicy(limit=10, period_seconds=60),     # max 10/min
    priority=1,  # lowest priority
    retry_policy=RetryPolicy(max_retries=1, delay_seconds=2),
    timeout_seconds=120,
)
def process_free(user_id: str, articles: list[dict]):
    """Process content for free-tier users."""
    ctx = GraphRunContext.get()
    logger.info(f"Free user {user_id}: processing {len(articles)} articles (run={ctx.graph_run_id})")

    summaries = summarize.map(articles)
    return {"user_id": user_id, "tier": "free", "processed": len(summaries), "results": summaries}


# ── Pipeline for paid users (high priority, generous limits) ──

@graph(
    name="process-content-pro",
    concurrency=ConcurrencyPolicy(limit=10, key="user_id"),  # max 10 parallel
    throttle=ThrottlePolicy(limit=100, period_seconds=60),    # max 100/min
    priority=10,  # highest priority — runs before free users
    retry_policy=RetryPolicy(max_retries=3, delay_seconds=1, backoff_factor=2),
    timeout_seconds=600,
)
def process_pro(user_id: str, articles: list[dict], operations: list[str]):
    """Process content for pro-tier users with all operations."""
    ctx = GraphRunContext.get()
    logger.info(f"Pro user {user_id}: processing {len(articles)} articles (run={ctx.graph_run_id})")

    results = []
    for article in articles:
        result = {"article_id": article["id"]}

        if "summarize" in operations:
            result["summary"] = summarize(article)
        if "translate" in operations:
            result["translation"] = translate(article)
        if "seo" in operations:
            result["seo"] = optimize_seo(article)

        results.append(result)

    return {"user_id": user_id, "tier": "pro", "processed": len(results), "results": results}


# ── Run ──

if __name__ == "__main__":
    deploy()

    # Free user: 3 articles, summarize only
    free_articles = [
        {"id": f"free-{i}", "content": f"Article content {i} " * 100}
        for i in range(3)
    ]
    free_result = process_free(user_id="free-user-1", articles=free_articles)
    print(f"Free: {free_result['processed']} articles processed")

    # Pro user: 10 articles, all operations
    pro_articles = [
        {"id": f"pro-{i}", "content": f"Premium article {i} " * 200, "target_lang": "es"}
        for i in range(10)
    ]
    pro_result = process_pro(
        user_id="pro-user-1",
        articles=pro_articles,
        operations=["summarize", "translate", "seo"],
    )
    print(f"Pro: {pro_result['processed']} articles processed with all operations")
