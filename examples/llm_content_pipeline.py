"""
Problem: LLM API Rate Limits Kill Your Content Pipeline

You're building an AI content tool that generates blog posts, social media captions,
and email newsletters. You call OpenAI 500 times in a loop. After 100 calls,
you hit the rate limit. The whole pipeline crashes. You restart from scratch.
Your OpenAI bill is $200 because nothing was cached.

Solution: GraphIngest handles all of this with 3 decorators:
    - cache_ttl: same prompt → cached result (no duplicate API calls)
    - RetryPolicy: rate limit hit → wait and retry with backoff
    - ThrottlePolicy: cap at 50 calls/min so you never hit the limit
    - .map(): process all content in parallel (within throttle limits)

Run:
    pip install graphingest
    python llm_content_pipeline.py
"""

from graphingest import (
    node, graph, deploy,
    RetryPolicy, ConcurrencyPolicy, ThrottlePolicy,
)
import time


@node(name="generate-blog-post", cache_ttl=86400, max_retries=5)
def generate_blog_post(topic: dict) -> dict:
    """
    Generate a blog post using an LLM.

    cache_ttl=86400: same topic → returns cached result for 24 hours.
    max_retries=5: if OpenAI returns 429 (rate limit), retry up to 5 times.
    """
    # In production: call OpenAI API
    # response = openai.chat.completions.create(
    #     model="gpt-4o",
    #     messages=[{"role": "user", "content": f"Write a blog post about {topic['title']}"}],
    # )
    time.sleep(0.5)  # simulate API call
    return {
        "topic": topic["title"],
        "content": f"Blog post about {topic['title']}...",
        "word_count": 800,
        "seo_score": 85,
    }


@node(name="generate-social-posts", cache_ttl=86400, max_retries=5)
def generate_social_posts(blog: dict) -> dict:
    """Generate social media posts from a blog post."""
    time.sleep(0.3)
    return {
        "topic": blog["topic"],
        "twitter": f"🧵 {blog['topic']} — a thread...",
        "linkedin": f"I just published: {blog['topic']}...",
        "instagram": f"New post! {blog['topic']} 📝",
    }


@node(name="generate-email-newsletter", cache_ttl=86400, max_retries=5)
def generate_newsletter(blog: dict) -> dict:
    """Generate an email newsletter from a blog post."""
    time.sleep(0.3)
    return {
        "topic": blog["topic"],
        "subject": f"This week: {blog['topic']}",
        "preview": blog["content"][:100],
    }


@graph(
    name="content-pipeline",
    # Throttle: max 50 LLM calls per minute (stay under OpenAI rate limits)
    throttle=ThrottlePolicy(limit=50, period_seconds=60),
    # Concurrency: max 10 parallel pipelines per user
    concurrency=ConcurrencyPolicy(limit=10, key="user_id"),
    retry_policy=RetryPolicy(max_retries=3, delay_seconds=2, backoff_factor=3),
    timeout_seconds=1800,  # 30 minutes for large batches
)
def content_pipeline(user_id: str, topics: list[dict]):
    """
    Generate content for multiple topics.

    Without GraphIngest:
        - 50 topics × 3 LLM calls each = 150 API calls
        - Hit rate limit at call #100 → crash → restart from scratch
        - No caching → re-generate everything = $$$
        - One user's 500-topic batch blocks everyone else

    With GraphIngest:
        - Throttled to 50 calls/min → never hit rate limits
        - Cached for 24h → re-runs are free
        - Per-user concurrency → fair sharing
        - Each topic retries independently
    """
    # Fan-out: generate all blog posts in parallel (throttled)
    blogs = generate_blog_post.map(topics)

    # Fan-out: generate social posts for each blog
    social = generate_social_posts.map(blogs)

    # Fan-out: generate newsletters for each blog
    newsletters = generate_newsletter.map(blogs)

    return {
        "user_id": user_id,
        "topics_processed": len(topics),
        "blogs": len(blogs),
        "social_posts": len(social),
        "newsletters": len(newsletters),
    }


if __name__ == "__main__":
    deploy()

    topics = [
        {"title": "AI in Healthcare"},
        {"title": "Remote Work Best Practices"},
        {"title": "Sustainable Energy Trends"},
        {"title": "Web3 and Decentralization"},
        {"title": "Machine Learning for Beginners"},
    ]

    result = content_pipeline(user_id="user-123", topics=topics)
    print(f"Generated content for {result['topics_processed']} topics:")
    print(f"  {result['blogs']} blog posts")
    print(f"  {result['social_posts']} social media post sets")
    print(f"  {result['newsletters']} email newsletters")
