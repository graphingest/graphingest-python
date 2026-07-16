"""
Problem: Vercel Cron Job Timeouts

Your Next.js app has a nightly sync job triggered by Vercel cron.
But Vercel serverless functions timeout at 10s (Hobby) or 60s (Pro).
Your sync takes 30 minutes. It fails every night at 3 AM.

Solution: GraphIngest dispatches the work to Cloud Run workers asynchronously.
Your cron route returns in <1 second. The pipeline runs for hours in the background.

Run:
    pip install graphingest requests
    python vercel_cron_timeout.py
"""

from graphingest import node, graph, deploy, RetryPolicy, GraphIngestClient
import requests


@node(name="sync-source", cache_ttl=1800, max_retries=3)
def sync_source(source: dict) -> dict:
    """Sync data from one external source. Takes 5-10 minutes per source."""
    resp = requests.get(source["api_url"], timeout=30)
    data = resp.json()
    # In production: write to your database
    return {
        "source": source["name"],
        "records_synced": len(data) if isinstance(data, list) else 1,
    }


@graph(
    name="nightly-sync",
    timeout_seconds=7200,  # 2 hours — impossible on Vercel, easy on GraphIngest
    retry_policy=RetryPolicy(max_retries=2, delay_seconds=30, backoff_factor=2),
)
def nightly_sync(sources: list[dict]):
    """
    Sync all data sources. This takes 30+ minutes.
    On Vercel, this would timeout. On GraphIngest, it runs on managed infra.
    """
    results = sync_source.map(sources)  # all sources in parallel
    total = sum(r["records_synced"] for r in results)
    return {"sources_synced": len(results), "total_records": total}


# ── How to trigger from your Next.js API route ──
#
# // app/api/cron/sync/route.ts (your Vercel cron handler)
# import { GraphIngestClient } from "graphingest";
#
# export async function GET() {
#   const client = new GraphIngestClient();
#   // This returns in <1 second — no timeout!
#   const run = await client.triggerFlowRun("nightly-sync", {
#     sources: [
#       { name: "stripe", api_url: "https://api.stripe.com/v1/charges" },
#       { name: "hubspot", api_url: "https://api.hubspot.com/contacts" },
#       { name: "postgres", api_url: "https://your-db-api.com/export" },
#     ],
#   });
#   return Response.json({ runId: run.id, status: "dispatched" });
# }
#
# // vercel.json
# { "crons": [{ "path": "/api/cron/sync", "schedule": "0 3 * * *" }] }


if __name__ == "__main__":
    deploy()

    result = nightly_sync([
        {"name": "users", "api_url": "https://jsonplaceholder.typicode.com/users"},
        {"name": "posts", "api_url": "https://jsonplaceholder.typicode.com/posts"},
        {"name": "comments", "api_url": "https://jsonplaceholder.typicode.com/comments"},
    ])
    print(f"Synced {result['total_records']} records from {result['sources_synced']} sources")
