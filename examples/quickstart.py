"""
GraphIngest Quick Start — Web Scraper Pipeline

Save this file and run:
    pip install graphingest requests
    export GRAPHINGEST_API_URL=https://graphingest.io
    export GRAPHINGEST_API_KEY=your-api-key
    python quickstart.py

What it does:
    1. Fetches 3 web pages in parallel (.map fan-out)
    2. Summarizes each page
    3. Returns all summaries
    4. Retries on failure with exponential backoff
    5. Caches results for 1 hour
"""

from graphingest import node, graph, deploy, RetryPolicy
import requests


# ── Step 1: Define your nodes (individual tasks) ──

@node(name="fetch-page", cache_ttl=3600, max_retries=3)
def fetch_page(url: str) -> dict:
    """Fetch a web page. Cached for 1 hour. Retries 3x on failure."""
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    return {"url": url, "status": resp.status_code, "length": len(resp.text)}


@node(name="summarize")
def summarize(page: dict) -> str:
    """Summarize a fetched page."""
    return f"Page {page['url']}: {page['length']} chars, status {page['status']}"


# ── Step 2: Compose nodes into a graph (pipeline) ──

@graph(
    name="web-scraper",
    retry_policy=RetryPolicy(
        max_retries=2,
        delay_seconds=1,
        backoff_factor=2,
        jitter=True,
    ),
    timeout_seconds=300,
)
def scrape_pipeline(urls: list[str]):
    # Fan-out: fetch all URLs in parallel
    pages = fetch_page.map(urls)

    # Process each result
    summaries = [summarize(page) for page in pages]

    return {"total": len(summaries), "summaries": summaries}


# ── Step 3: Deploy and run ──

if __name__ == "__main__":
    deploy()  # push code to platform

    result = scrape_pipeline([
        "https://example.com",
        "https://httpbin.org/get",
        "https://jsonplaceholder.typicode.com/posts/1",
    ])
    print(result)
