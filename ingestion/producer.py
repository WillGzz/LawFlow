import json
import logging
import time
import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError


from config.settings import (
    KAFKA_BROKER,
    KAFKA_TOPIC,
    FEDERAL_REGISTER_URL,
    FEDERAL_REGISTER_PARAMS,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def build_producer() -> KafkaProducer:
    """
    Create and return a Kafka producer.
    Retries connection up to 5 times with 5 second delay between attempts.
    Kafka may not be fully ready when this runs even with healthchecks.
    acks=all guarantees message is written before moving on — no silent data loss.
    """
    retries = 5
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8"),
                acks="all",
                retries=3,
                max_in_flight_requests_per_connection=1,
            )
            logger.info("Kafka producer connected successfully")
            return producer
        except KafkaError as e:
            logger.warning(f"Kafka connection attempt {attempt} - retries {retries} failed: {e}")
            if attempt < retries:
                time.sleep(5)
    raise RuntimeError("Failed to connect to Kafka after 5 attempts")


def fetch_documents() -> list[dict]:
    """
    Call 1 — Federal Register search endpoint.
    Fetches the 20 most recent Rules and Proposed Rules
    from EPA and Department of Energy.
    Returns raw API response results as-is — no transformation.
    """
    params = {
        **FEDERAL_REGISTER_PARAMS,
        "fields[]": [
            "document_number",
            "title",
            "type",
            "action",
            "abstract",
            "agencies",
            "publication_date",
            "effective_on",
            "comments_close_on",
            "dates",
            "page_length",
            "topics",
            "docket_ids",
            "html_url",
            "raw_text_url",
        ],
    }

    try:
        response = requests.get(
            FEDERAL_REGISTER_URL,
            params=params,
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        documents = data.get("results", [])
        logger.info(f"Fetched {len(documents)} documents from Federal Register API")
        return documents
    except requests.exceptions.Timeout:
        logger.error("Federal Register API request timed out")
        return []
    except requests.exceptions.HTTPError as e:
        logger.error(f"Federal Register API HTTP error: {e}")
        return []
    except requests.exceptions.RequestException as e:
        logger.error(f"Federal Register API request failed: {e}")
        return []


def fetch_raw_text(raw_text_url: str, document_number: str) -> str | None:
    """
    Call 2 — Fetch plain text content from raw_text_url.
    Returns the full document body as a plain string.
    Returns None if url is missing or fetch fails.
    Spark will fall back to abstract if full_text is None.
    """
    if not raw_text_url:
        logger.warning(f"No raw_text_url for {document_number}")
        return None

    try:
        response = requests.get(raw_text_url, timeout=60)
        response.raise_for_status()
        text = response.text.strip()
        logger.info(f"Fetched raw text for {document_number} — {len(text)} chars")
        return text
    except requests.exceptions.Timeout:
        logger.warning(f"Raw text fetch timed out for {document_number}")
        return None
    except requests.exceptions.HTTPError as e:
        logger.warning(f"Raw text fetch failed for {document_number}: {e}")
        return None
    except requests.exceptions.RequestException as e:
        logger.warning(f"Raw text fetch failed for {document_number}: {e}")
        return None


def produce_documents(producer: KafkaProducer, documents: list[dict]) -> None:
    """
    For each document:
    1. Fetch raw text from raw_text_url (Call 2)
    2. Attach full_text to the existing API response dict
    3. Produce to Kafka as JSON with document_number as key
    No transformation — raw API data + full_text forwarded as-is to Spark.
    """
    success_count = 0
    fail_count = 0

    for doc in documents:
        document_number = doc.get("document_number")

        if not document_number:
            logger.warning("Document missing document_number — skipping")
            fail_count += 1
            continue

        # call 2 — attach raw text to document
        raw_text = fetch_raw_text(doc.get("raw_text_url"), document_number)
        doc["full_text"] = raw_text

        # produce to kafka — document_number as partition key
        try:
            future = producer.send(
                KAFKA_TOPIC,
                key=document_number,
                value=doc,
            )
            future.get(timeout=10)
            logger.info(f"Produced {document_number} to {KAFKA_TOPIC}")
            success_count += 1
        except KafkaError as e:
            logger.error(f"Failed to produce {document_number}: {e}")
            fail_count += 1

        # small delay to avoid hammering the Federal Register API
        time.sleep(0.5)

    logger.info(f"Done — success: {success_count}, failed: {fail_count}")


def run() -> None:
    """
    Main entry point.
    Called by Airflow DAG daily at 9am Mon-Fri.
    Can also be run manually:
        docker exec -it lawflow python ingestion/producer.py
    """
    logger.info("LawFlow producer starting")

    producer = build_producer()

    try:
        documents = fetch_documents()

        if not documents:
            logger.warning("No documents returned — exiting")
            return

        produce_documents(producer, documents)

    finally:
        producer.flush()
        producer.close()
        logger.info("Kafka producer closed")


if __name__ == "__main__":
    run()