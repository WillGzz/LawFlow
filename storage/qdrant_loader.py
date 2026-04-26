import logging

from config.settings import (
    QDRANT_HOST,
    QDRANT_PORT,
    QDRANT_COLLECTION,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def create_collection_if_not_exists(client) -> None:
 
    from qdrant_client.models import VectorParams, Distance

    existing = [c.name for c in client.get_collections().collections]
    if QDRANT_COLLECTION not in existing:
        client.create_collection(
            collection_name=QDRANT_COLLECTION,
            vectors_config=VectorParams(size=1024, distance=Distance.COSINE),
        )
        logger.info(f"Created Qdrant collection: {QDRANT_COLLECTION}")
    else:
        logger.info(f"Qdrant collection {QDRANT_COLLECTION} already exists")


def build_points(rows: list) -> list:
    """
    Convert DataFrame rows into Qdrant PointStruct objects.
    Each point represents one chunk of a regulatory document.
    id — Qdrant requires an integer or UUID. We hash chunk_id string
    to a positive integer since chunk_id is a human readable string.
    payload — metadata stored alongside the vector, returned with
    search results so the agent has full context to synthesize answers.
    Skips rows with missing embedding or chunk_text.
    """
    from qdrant_client.models import PointStruct

    points = []

    for row in rows:
        if not row["embedding"] or not row["chunk_text"]:
            logger.warning(f"Skipping row with missing embedding or chunk_text: {row.get('chunk_id')}")
            continue

        point = PointStruct(
            id=abs(hash(row["chunk_id"])) % (2**63),
            vector=row["embedding"],
            payload={
                "chunk_id":          row["chunk_id"],
                "document_number":   row["document_number"],
                "title":             row["title"],
                "type":              row["type"],
                "action":            row["action"],
                "publication_date":  row["publication_date"],
                "effective_on":      row["effective_on"],
                "comments_close_on": row["comments_close_on"],
                "dates":             row["dates"],
                "page_length":       row["page_length"],
                "agency":            row["agency_name"],
                "parent_agency":     row["parent_agency_name"],
                "topics":            row["topics"],
                "chunk_text":        row["chunk_text"],
                "chunk_index":       row["chunk_index"],
                "html_url":          row["html_url"],
                "docket_id":         row["primary_docket_id"],
            },
        )
        points.append(point)

    return points


def load_qdrant_batch(batch_df, batch_id: int) -> None:
    """
    Spark foreachBatch sink — writes one micro-batch to Qdrant.
    Called automatically by Spark Structured Streaming every 30 seconds.
    Each row in the batch is one chunk with its embedding.
    Uses upsert — safe to run multiple times on same data, no duplicates.
    Imports inside function because foreachBatch runs on Spark workers
    which do not have access to driver-level imports.
    """
    from qdrant_client import QdrantClient

    rows = batch_df.collect()
    
    if not rows:
        logger.info(f"Batch {batch_id} — empty, skipping Qdrant write")
        return

    client = QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT)
    create_collection_if_not_exists(client)

    points = build_points(rows)

    if not points:
        logger.warning(f"Batch {batch_id} — no valid points to upsert")
        return

    client.upsert(collection_name=QDRANT_COLLECTION, points=points)
    logger.info(f"Batch {batch_id} — upserted {len(points)} points to Qdrant")