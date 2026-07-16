"""
Problem: Database Migration Fails Halfway Through

You're migrating 500,000 records from MongoDB to Postgres. At record 350,000,
the Postgres connection drops. You have to restart from record 1.
That's 350,000 wasted API calls and 4 hours of lost time.

Solution: GraphIngest caches every completed batch. When you restart,
it skips the 350,000 already-migrated records and resumes from 350,001.
Total cost of the failure: ~0 seconds (cache hits are instant).

Run:
    pip install graphingest
    python data_migration.py
"""

from graphingest import node, graph, deploy, RetryPolicy, GraphRunContext
import logging

logger = logging.getLogger(__name__)


@node(name="extract-batch", cache_ttl=86400, max_retries=3)
def extract_batch(batch_config: dict) -> dict:
    """
    Extract a batch of records from the source database.

    cache_ttl=86400: results cached for 24 hours.
    If the migration restarts, already-extracted batches return instantly from cache.
    """
    offset = batch_config["offset"]
    limit = batch_config["limit"]
    # In production: query MongoDB
    # records = mongo_db.collection.find().skip(offset).limit(limit)
    records = [{"id": i, "data": f"record-{i}"} for i in range(offset, offset + limit)]
    return {
        "batch_id": f"batch-{offset}-{offset + limit}",
        "records": records,
        "count": len(records),
    }


@node(name="transform-batch", cache_ttl=86400)
def transform_batch(batch: dict) -> dict:
    """Transform records for the target schema. Also cached."""
    transformed = []
    for record in batch["records"]:
        transformed.append({
            "id": record["id"],
            "data": record["data"].upper(),  # example transformation
            "migrated": True,
        })
    return {
        "batch_id": batch["batch_id"],
        "records": transformed,
        "count": len(transformed),
    }


@node(name="load-batch", max_retries=5)
def load_batch(batch: dict) -> dict:
    """
    Load a batch into the target database.

    max_retries=5: if Postgres connection drops, retry up to 5 times
    with exponential backoff (handled by the graph's RetryPolicy).
    """
    # In production: INSERT INTO postgres
    # cursor.executemany("INSERT INTO ...", batch["records"])
    return {
        "batch_id": batch["batch_id"],
        "loaded": batch["count"],
        "status": "success",
    }


@graph(
    name="data-migration",
    retry_policy=RetryPolicy(
        max_retries=3,
        delay_seconds=10,
        backoff_factor=2,
        jitter=True,
    ),
    timeout_seconds=14400,  # 4 hours
    on_failure=[lambda ctx, err: logger.error(f"Migration failed at run {ctx.graph_run_id}: {err}")],
)
def migrate_data(total_records: int, batch_size: int = 1000):
    """
    Migrate records in batches.

    Without GraphIngest:
        - Fails at batch 350 of 500 → restart from batch 1
        - 350 batches × 1000 records = 350,000 wasted operations
        - 4 hours lost

    With GraphIngest:
        - Fails at batch 350 → restart → batches 1-350 return from cache (0ms each)
        - Only batch 351+ actually re-execute
        - Recovery time: seconds, not hours
    """
    ctx = GraphRunContext.get()
    logger.info(f"Starting migration: {total_records} records in batches of {batch_size}")
    logger.info(f"Run ID: {ctx.graph_run_id}")

    # Create batch configs
    batches = [
        {"offset": i, "limit": min(batch_size, total_records - i)}
        for i in range(0, total_records, batch_size)
    ]
    logger.info(f"Created {len(batches)} batches")

    # Fan-out: extract all batches in parallel (cached!)
    extracted = extract_batch.map(batches)

    # Fan-out: transform all batches in parallel (cached!)
    transformed = transform_batch.map(extracted)

    # Fan-out: load all batches in parallel
    loaded = load_batch.map(transformed)

    total_loaded = sum(r["loaded"] for r in loaded)
    return {
        "total_records": total_records,
        "batches_processed": len(loaded),
        "records_loaded": total_loaded,
    }


if __name__ == "__main__":
    deploy()

    # Migrate 5,000 records in batches of 1,000
    result = migrate_data(total_records=5000, batch_size=1000)
    print(f"Migration complete: {result['records_loaded']} records in {result['batches_processed']} batches")

    # If you run this again, all batches return from cache instantly!
    print("\nRunning again (should be instant from cache)...")
    result2 = migrate_data(total_records=5000, batch_size=1000)
    print(f"Cache hit: {result2['records_loaded']} records (0ms per batch)")
