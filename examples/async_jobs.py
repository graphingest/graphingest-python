"""
GraphIngest Async Jobs — Background Processing with Polling

Shows how to dispatch long-running jobs and poll for results.
Perfect for: report generation, video processing, ML training.

Run:
    pip install graphingest
    python async_jobs.py
"""

from graphingest import node, graph, deploy, get_status
import time


# ── Nodes ──

@node(name="generate-report", max_retries=2, timeout_seconds=300)
def generate_report(params: dict) -> dict:
    """Generate a report (simulates a long-running task)."""
    report_type = params.get("type", "summary")
    date_range = params.get("date_range", "last_30_days")

    # In production: query database, build charts, generate PDF
    time.sleep(2)  # simulate work

    return {
        "type": report_type,
        "date_range": date_range,
        "pages": 12,
        "charts": 5,
        "url": f"https://storage.example.com/reports/{report_type}_{date_range}.pdf",
    }


@node(name="send-email")
def send_email(params: dict) -> dict:
    """Send an email notification."""
    # In production: use SendGrid, SES, etc.
    return {"sent_to": params["to"], "subject": params["subject"], "status": "sent"}


# ── Pipeline ──

@graph(name="report-pipeline", timeout_seconds=600)
def report_pipeline(request: dict):
    """Generate a report and email it to the user."""
    # Generate report (may take minutes)
    report = generate_report(request)

    # Send notification
    send_email({
        "to": request["email"],
        "subject": f"Your {report['type']} report is ready",
        "body": f"Download: {report['url']}",
    })

    return report


# ── Usage patterns ──

if __name__ == "__main__":
    deploy()

    # ── Pattern 1: Fire-and-forget with future ──
    print("=== Pattern 1: Async dispatch with future ===")
    future = generate_report.arun({
        "type": "quarterly",
        "date_range": "Q3_2025",
    })
    print(f"Job dispatched: {future.task_run_id}")

    # Do other work while report generates...
    print("Doing other work...")

    # Block until ready
    result = future.result(timeout=120)
    print(f"Report ready: {result['url']}")
    print()

    # ── Pattern 2: Poll from your backend ──
    print("=== Pattern 2: Status polling (for frontend integration) ===")
    future2 = generate_report.arun({
        "type": "annual",
        "date_range": "2025",
    })
    job_id = future2.task_run_id
    print(f"Job ID: {job_id}")

    # This is what your backend API would do:
    while True:
        status = get_status(job_id)
        print(f"  Status: {status['state']}")
        if status["state"] == "COMPLETED":
            print(f"  Result: {status['result']}")
            break
        elif status["state"] == "FAILED":
            print(f"  Error: {status['error']}")
            break
        time.sleep(2)
    print()

    # ── Pattern 3: Fan-out multiple reports ──
    print("=== Pattern 3: Fan-out (parallel report generation) ===")
    reports = generate_report.map([
        {"type": "sales", "date_range": "Q3_2025"},
        {"type": "marketing", "date_range": "Q3_2025"},
        {"type": "engineering", "date_range": "Q3_2025"},
    ])
    for r in reports:
        print(f"  {r['type']}: {r['url']}")
