"""
GraphIngest AI Agent — Research Assistant

A ReAct agent that searches the web, scrapes pages, and summarizes findings.
Uses built-in platform LLM (no API key needed) or bring your own.

Run:
    pip install graphingest[react] requests beautifulsoup4
    python ai_agent.py
"""

from graphingest import node, deploy
from graphingest.react import agent


# ── Tool nodes (the agent picks which ones to use) ──

@node(name="web-search", cache_ttl=300, max_retries=2)
def search(query: str) -> list[dict]:
    """Search the web for information. Returns top results."""
    # In production, use Google Custom Search, Serper, or SerpAPI
    # This is a mock for demonstration
    return [
        {"title": f"Result 1 for: {query}", "url": "https://example.com/1", "snippet": f"Information about {query}..."},
        {"title": f"Result 2 for: {query}", "url": "https://example.com/2", "snippet": f"More details on {query}..."},
        {"title": f"Result 3 for: {query}", "url": "https://example.com/3", "snippet": f"Latest news about {query}..."},
    ]


@node(name="scrape-page", cache_ttl=600, max_retries=3)
def scrape(url: str) -> str:
    """Fetch and extract text content from a web page."""
    import requests
    try:
        resp = requests.get(url, timeout=10, headers={"User-Agent": "GraphIngest/1.0"})
        resp.raise_for_status()
        # In production, use BeautifulSoup to extract clean text
        return resp.text[:2000]  # first 2000 chars
    except Exception as e:
        return f"Failed to scrape {url}: {e}"


@node(name="summarize-text")
def summarize(text: str) -> str:
    """Summarize a block of text into key points."""
    # In production, this could call an LLM or use extractive summarization
    lines = text.split("\n")
    key_lines = [l.strip() for l in lines if len(l.strip()) > 50][:5]
    return "\n".join(key_lines) if key_lines else text[:500]


# ── Agent: combines tools with LLM reasoning ──

@agent(
    name="researcher",
    tools=[search, scrape, summarize],
    model="standard",  # platform-managed LLM (no API key needed)
    # model="gpt-4o",  # or bring your own (set OPENAI_API_KEY)
    max_iterations=5,
    timeout_seconds=120,
)
def research(query: str) -> str:
    """You are a research assistant. Given a question, search the web,
    scrape relevant pages, and provide a comprehensive answer with sources."""
    ...


# ── Run ──

if __name__ == "__main__":
    deploy()

    # Single query
    answer = research.run("What are the latest advances in fusion energy?")
    print("Answer:", answer)
    print()

    # Fan-out: research multiple topics in parallel
    questions = [
        "What is quantum computing?",
        "How does CRISPR gene editing work?",
        "What are the benefits of GraphQL over REST?",
    ]
    answers = research.map(questions)
    for q, a in zip(questions, answers):
        print(f"Q: {q}")
        print(f"A: {a}")
        print()
