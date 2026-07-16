"""
GraphIngest + FastAPI — Backend Integration Pattern

Scenario: Your web app needs to generate PDF reports that take 2-3 minutes.
Users click "Generate Report", get a job ID, and poll for status.

This example shows:
    1. Dispatch a long-running job from your API endpoint
    2. Return a job ID to the frontend immediately
    3. Frontend polls your backend for status
    4. Backend uses get_status() to check the job

Run:
    pip install graphingest fastapi uvicorn
    uvicorn webhook_backend:app --reload
"""

from fastapi import FastAPI
from graphingest import node, deploy, get_status

app = FastAPI()


# ── GraphIngest node (runs on managed infrastructure) ──

@node(name="generate-report", max_retries=2, timeout_seconds=300)
def generate_report(params: dict) -> dict:
    """Generate a PDF report. Takes 2-3 minutes."""
    import time
    time.sleep(5)  # simulate work

    return {
        "type": params["type"],
        "pages": 24,
        "url": f"https://storage.example.com/reports/{params['type']}.pdf",
    }


# ── API endpoints ──

@app.post("/api/reports/generate")
async def start_report(params: dict):
    """
    Start generating a report. Returns immediately with a job ID.

    Request:  POST /api/reports/generate {"type": "quarterly", "year": 2025}
    Response: {"jobId": "uuid-...", "status": "dispatched"}
    """
    future = generate_report.arun(params)
    return {
        "jobId": future.task_run_id,
        "status": "dispatched",
    }


@app.get("/api/reports/status/{job_id}")
async def check_status(job_id: str):
    """
    Check the status of a report generation job.

    Response (pending):   {"state": "RUNNING", "result": null, "error": null}
    Response (done):      {"state": "COMPLETED", "result": {"url": "..."}, "error": null}
    Response (failed):    {"state": "FAILED", "result": null, "error": "timeout"}
    """
    return get_status(job_id)


# ── Deploy on startup ──

@app.on_event("startup")
async def startup():
    deploy()


# ── Frontend integration (JavaScript) ──
#
# const { jobId } = await fetch("/api/reports/generate", {
#     method: "POST",
#     body: JSON.stringify({ type: "quarterly", year: 2025 }),
# }).then(r => r.json());
#
# // Poll every 2 seconds
# const poll = setInterval(async () => {
#     const { state, result } = await fetch(`/api/reports/status/${jobId}`).then(r => r.json());
#     if (state === "COMPLETED") {
#         clearInterval(poll);
#         window.open(result.url);  // download the PDF
#     }
# }, 2000);
