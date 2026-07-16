"""
Problem: Processing 10,000 Images Sequentially Takes Hours

Your e-commerce platform needs to resize, watermark, and optimize 10,000 product
images after a bulk upload. Processing them one-by-one takes 8 hours.
AWS Lambda has a 15-minute timeout. Step Functions has a 256KB state limit.

Solution: GraphIngest fans out all 10,000 images in parallel with .map().
Each image gets its own timeout, retries, and caching.
The whole batch finishes in minutes, not hours.

Run:
    pip install graphingest
    python image_processing_pipeline.py
"""

from graphingest import node, graph, deploy, RetryPolicy
import time


@node(name="resize-image", max_retries=3, timeout_seconds=30)
def resize_image(image: dict) -> dict:
    """Resize an image to multiple sizes."""
    # In production: use Pillow, sharp, or ImageMagick
    time.sleep(0.1)  # simulate processing
    return {
        "id": image["id"],
        "original": image["url"],
        "sizes": {
            "thumb": f"https://cdn.example.com/{image['id']}_thumb.webp",
            "medium": f"https://cdn.example.com/{image['id']}_medium.webp",
            "large": f"https://cdn.example.com/{image['id']}_large.webp",
        },
    }


@node(name="add-watermark", max_retries=2)
def add_watermark(image: dict) -> dict:
    """Add a watermark to the large version."""
    time.sleep(0.05)
    image["watermarked"] = True
    return image


@node(name="optimize-for-web", max_retries=2)
def optimize_for_web(image: dict) -> dict:
    """Compress and optimize for web delivery."""
    time.sleep(0.05)
    image["optimized"] = True
    image["savings_percent"] = 42
    return image


@node(name="update-database")
def update_database(image: dict) -> dict:
    """Update the product database with new image URLs."""
    # In production: UPDATE products SET images = ... WHERE id = ...
    return {"id": image["id"], "status": "updated"}


@graph(
    name="image-pipeline",
    retry_policy=RetryPolicy(max_retries=2, delay_seconds=1),
    timeout_seconds=3600,  # 1 hour for the whole batch
)
def process_images(images: list[dict]):
    """
    Process a batch of product images.

    Without GraphIngest:
        - 10,000 images × 0.5s each = 83 minutes (sequential)
        - Lambda timeout at 15 min = fails
        - No retries = one failure kills the batch

    With GraphIngest:
        - 10,000 images in parallel = ~2 minutes
        - Each image retries independently
        - Resume from failure point if batch crashes
    """
    # Fan-out: resize all images in parallel
    resized = resize_image.map(images)

    # Fan-out: watermark all in parallel
    watermarked = add_watermark.map(resized)

    # Fan-out: optimize all in parallel
    optimized = optimize_for_web.map(watermarked)

    # Fan-out: update database for all
    db_results = update_database.map(optimized)

    return {
        "total_processed": len(db_results),
        "sample": db_results[:3],
    }


if __name__ == "__main__":
    deploy()

    # Simulate 100 product images (use 10,000 in production)
    images = [
        {"id": f"product-{i}", "url": f"https://uploads.example.com/raw/{i}.jpg"}
        for i in range(100)
    ]

    result = process_images(images)
    print(f"Processed {result['total_processed']} images")
    print(f"Sample: {result['sample']}")
