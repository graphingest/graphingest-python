"""
Problem: PDF Report Generation Takes Too Long

Your users click "Generate Report" and stare at a loading spinner for 3 minutes.
If they close the tab, the report is lost. If the server restarts, it starts over.

Solution: GraphIngest dispatches the report generation as a background job.
Users get a job ID instantly, poll for status, and download when ready.
If it fails, it retries automatically. If it crashes, it resumes from where it left off.

Run:
    pip install graphingest
    python pdf_report_generation.py
"""

from graphingest import node, graph, deploy, RetryPolicy, get_status
import time


@node(name="query-database", cache_ttl=300, max_retries=2)
def query_database(params: dict) -> dict:
    """Query database for report data. Cached for 5 minutes."""
    # In production: run SQL queries
    time.sleep(1)  # simulate query time
    return {
        "report_type": params["type"],
        "rows": 15000,
        "date_range": params.get("date_range", "last_30_days"),
    }


@node(name="generate-charts", max_retries=2)
def generate_charts(data: dict) -> dict:
    """Generate charts from data. CPU-intensive."""
    time.sleep(2)  # simulate chart generation
    return {
        "charts": [
            {"type": "bar", "title": "Revenue by Month"},
            {"type": "line", "title": "User Growth"},
            {"type": "pie", "title": "Traffic Sources"},
        ],
        "rows_processed": data["rows"],
    }


@node(name="render-pdf", max_retries=1)
def render_pdf(charts: dict) -> dict:
    """Render charts into a PDF file."""
    time.sleep(1)  # simulate PDF rendering
    return {
        "url": "https://storage.example.com/reports/quarterly-2025.pdf",
        "pages": 24,
        "charts_included": len(charts["charts"]),
        "size_mb": 2.4,
    }


@graph(
    name="generate-report",
    retry_policy=RetryPolicy(max_retries=2, delay_seconds=5),
    timeout_seconds=600,
)
def generate_report(params: dict):
    """
    Generate a PDF report. Takes 3-5 minutes.

    Without GraphIngest: user waits, tab closes = lost work, no retries.
    With GraphIngest: instant job ID, background processing, auto-retry.
    """
    data = query_database(params)
    charts = generate_charts(data)
    pdf = render_pdf(charts)
    return pdf


if __name__ == "__main__":
    deploy()

    # ── Pattern: dispatch and poll (what your backend does) ──

    # 1. User clicks "Generate Report" → your API dispatches the job
    future = generate_report.arun({"type": "quarterly", "date_range": "Q3_2025"})
    job_id = future.task_run_id
    print(f"Report dispatched! Job ID: {job_id}")
    print("User can close the tab — the report keeps generating.")
    print()

    # 2. Frontend polls your backend → your backend calls get_status()
    while True:
        status = get_status(job_id)
        print(f"  Status: {status['state']}")
        if status["state"] == "COMPLETED":
            print(f"  Download: {status['result']['url']}")
            print(f"  Pages: {status['result']['pages']}")
            break
        elif status["state"] == "FAILED":
            print(f"  Error: {status['error']}")
            break
        time.sleep(2)
