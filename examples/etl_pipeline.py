"""
GraphIngest ETL Pipeline — Extract, Transform, Load

A real-world ETL pipeline that:
    1. Extracts data from multiple API sources in parallel
    2. Transforms/cleans each record
    3. Loads results into a database
    4. Uses subgraphs for reusable pipeline components
    5. Async dispatch for non-blocking execution

Run:
    pip install graphingest requests
    python etl_pipeline.py
"""

from graphingest import node, graph, deploy, RetryPolicy, GraphRunContext
import requests
import json
import logging

logger = logging.getLogger(__name__)


# ── Nodes ──

@node(name="extract-api", cache_ttl=1800, max_retries=3)
def extract_from_api(source: dict) -> dict:
    """Extract data from an API endpoint. Cached for 30 minutes."""
    logger.info(f"Extracting from {source['url']}")
    resp = requests.get(source["url"], timeout=15)
    resp.raise_for_status()
    data = resp.json()

    # Handle different API response shapes
    if isinstance(data, list):
        records = data
    elif "results" in data:
        records = data["results"]
    elif "data" in data:
        records = data["data"]
    else:
        records = [data]

    return {
        "source": source["name"],
        "record_count": len(records),
        "records": records[:100],  # cap at 100 records
    }


@node(name="transform-record")
def transform_record(record: dict) -> dict:
    """Clean and normalize a single record."""
    cleaned = {}
    for key, value in record.items():
        # Normalize keys to snake_case
        clean_key = key.lower().replace(" ", "_").replace("-", "_")
        # Strip whitespace from strings
        if isinstance(value, str):
            value = value.strip()
        cleaned[clean_key] = value

    cleaned["_processed"] = True
    return cleaned


@node(name="load-to-db", max_retries=2)
def load_to_db(batch: dict) -> dict:
    """Load a batch of records to the database."""
    source = batch["source"]
    records = batch["records"]
    logger.info(f"Loading {len(records)} records from {source}")

    # In production, this would write to your database
    # db.insert_many("raw_data", records)

    return {
        "source": source,
        "loaded_count": len(records),
        "status": "success",
    }


# ── Subgraph: reusable ETL for a single source ──

@graph(
    name="single-source-etl",
    retry_policy=RetryPolicy(max_retries=2, delay_seconds=1),
    timeout_seconds=120,
)
def etl_single_source(source: dict) -> dict:
    """ETL pipeline for a single data source. Can run standalone or as a subgraph."""
    # Extract
    raw = extract_from_api(source)

    # Transform each record
    transformed_records = [transform_record(r) for r in raw["records"]]

    # Load
    result = load_to_db({
        "source": raw["source"],
        "records": transformed_records,
    })

    return result


# ── Main pipeline ──

@graph(
    name="multi-source-etl",
    retry_policy=RetryPolicy(
        max_retries=3,
        delay_seconds=2,
        backoff_factor=3,
        jitter=True,
    ),
    timeout_seconds=600,
    on_completion=[lambda ctx, result: logger.info(f"Pipeline completed: {result}")],
    on_failure=[lambda ctx, err: logger.error(f"Pipeline FAILED: {err}")],
)
def etl_pipeline(sources: list[dict]):
    """
    Multi-source ETL pipeline.

    Extracts from multiple APIs in parallel, transforms, and loads.
    Each source runs as a subgraph with its own retries and timeout.
    """
    ctx = GraphRunContext.get()
    logger.info(f"Starting ETL pipeline (run={ctx.graph_run_id})")
    logger.info(f"Processing {len(sources)} sources")

    # Fan-out: extract from all sources in parallel
    raw_data = extract_from_api.map([s for s in sources])

    # Run each source through the subgraph
    results = []
    for source in sources:
        result = etl_single_source(source)
        results.append(result)

    total_loaded = sum(r["loaded_count"] for r in results)

    return {
        "sources_processed": len(results),
        "total_records_loaded": total_loaded,
        "results": results,
    }


# ── Run ──

if __name__ == "__main__":
    deploy()

    sources = [
        {"name": "users", "url": "https://jsonplaceholder.typicode.com/users"},
        {"name": "posts", "url": "https://jsonplaceholder.typicode.com/posts"},
        {"name": "todos", "url": "https://jsonplaceholder.typicode.com/todos"},
    ]

    result = etl_pipeline(sources)
    print(json.dumps(result, indent=2))
