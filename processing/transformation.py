
import logging

from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, from_json, when, explode, concat, lit, udf)
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, ArrayType, FloatType )

import pandas as pd


from config import settings as cfg

from storage.qdrant_loader import load_qdrant_batch
from storage.arcadeDB import load_arcadedb_batch
 
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

 
def build_spark() -> SparkSession:
    """
    Create SparkSession in local mode.
    local[*] uses all available CPU cores.
    shuffle.partitions=8 is appropriate for local mode with small data volume.
    Kafka package gives Spark native ability to read from Kafka topics.
    """
    return (
        SparkSession.builder
        .appName("LawFlow")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8") # 8 partitions for shuffling data in local mode (low data volume)
        .config(
            "spark.jars.packages", #download the Kafka connector library.
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0", #
        )
        .getOrCreate()
    )
 
# org.apache.spark — Apache Spark organization
# spark-sql-kafka-0-10 — the Kafka connector package
# 2.12 — Scala version Spark was compiled with
# 3.5.0 — PySpark version 

 
# =============================================================================
# KAFKA MESSAGE SCHEMA
# =============================================================================
 
def get_kafka_schema() -> StructType:
    """
    Define the expected structure of messages arriving from Kafka.
    Mirrors exactly what producer.py sends.
    Spark uses this to parse raw JSON bytes into a typed DataFrame.
    nullable=True for fields that may not be present on every document.
    """
    agency_schema = StructType([
        StructField("id",        IntegerType(), True),
        StructField("raw_name",  StringType(),  True),
        StructField("url",       StringType(),  True),
        StructField("parent_id", IntegerType(), True),
    ])
 
    return StructType([
        StructField("document_number",  StringType(),         False),
        StructField("title",            StringType(),         False),
        StructField("type",             StringType(),         True),
        StructField("action",           StringType(),         True),
        StructField("abstract",         StringType(),         True),
        StructField("full_text",        StringType(),         True),
        StructField("publication_date", StringType(),         True),
        StructField("effective_on",     StringType(),         True),
        StructField("comments_close_on",StringType(),         True),
        StructField("dates",            StringType(),         True),
        StructField("page_length",      IntegerType(),        True),
        StructField("topics",           ArrayType(StringType()), True),
        StructField("docket_ids",       ArrayType(StringType()), True),
        StructField("html_url",         StringType(),         True),
        StructField("raw_text_url",     StringType(),         True),
        StructField("agencies",         ArrayType(agency_schema), True),
    ])
 
 
# =============================================================================
# USER DEFINED FUNCTIONS
# =============================================================================
 
def _extract_primary_agency(agencies: list) -> dict | None:
    """
    Return the sub-agency (the one with a parent_id).
    If no sub-agency exists return the first agency.
    Example: for [DOE, FERC] returns FERC because FERC has parent_id=136.
    """
    if not agencies:
        return None
    sub = [a for a in agencies if a.get("parent_id") is not None]
    return sub[0] if sub else agencies[0]
 
 
def _extract_parent_agency(agencies: list) -> dict | None:
    """
    Return the top-level parent agency (the one with parent_id=null).
    Returns None if only one agency or no parent found.
    Example: for [DOE, FERC] returns DOE because DOE has parent_id=null.
    """
    if not agencies or len(agencies) == 1:
        return None
    parents = [a for a in agencies if a.get("parent_id") is None]
    return parents[0] if parents else None
 
 
def _extract_primary_docket(docket_ids: list) -> str | None:
    """
    Return the primary docket ID filtering out FRL numbers.
    FRL numbers are internal Federal Register tracking IDs unique
    per document — they cannot connect related documents.
    Agency-prefixed IDs (e.g. EPA-R02-OAR-2025-1047) are shared
    across related documents and used for graph relationships.
    Strips "Docket No. " prefix if present.
    """
    if not docket_ids:
        return None
    non_frl = [d for d in docket_ids if not d.upper().startswith("FRL")]
    primary = non_frl[0] if non_frl else docket_ids[0]
    return primary.replace("Docket No. ", "").strip()
 
 
def _chunk_text(text: str, chunk_size: int, overlap: int) -> list[str]:
    """
    Split text into overlapping word-based chunks.
    chunk_size controls how many words per chunk (~512 words = ~3-4 paragraphs).
    overlap preserves context at chunk boundaries — last N words of one chunk
    are repeated at the start of the next.
    Strips form feed characters (common in government documents)
    and normalizes whitespace before chunking.
    """
    import re
    if not text:
        return []
    text = text.replace("\f", " ")
    text = re.sub(r"\s+", " ", text).strip()
    words = text.split()
    if not words:
        return []
    chunks = []
    start = 0
    while start < len(words):
        end = start + chunk_size
        chunks.append(" ".join(words[start:end]))
        start += chunk_size - overlap
    return chunks
 
 
# register UDFs with Spark
agency_name_udf = udf(
    lambda agencies: (
        _extract_primary_agency(agencies or {}).get("raw_name", "").title()
        if _extract_primary_agency(agencies or {}) else None
    ),
    StringType(),
)
 
agency_id_udf = udf(
    lambda agencies: (
        _extract_primary_agency(agencies or {}).get("id")
        if _extract_primary_agency(agencies or {}) else None
    ),
    IntegerType(),
)
 
agency_url_udf = udf(
    lambda agencies: (
        _extract_primary_agency(agencies or {}).get("url", "")
        if _extract_primary_agency(agencies or {}) else None
    ),
    StringType(),
)
 
parent_agency_name_udf = udf(
    lambda agencies: (
        _extract_parent_agency(agencies or {}).get("raw_name", "").title()
        if _extract_parent_agency(agencies or {}) else None
    ),
    StringType(),
)
 
parent_agency_id_udf = udf(
    lambda agencies: (
        _extract_parent_agency(agencies or {}).get("id")
        if _extract_parent_agency(agencies or {}) else None
    ),
    IntegerType(),
)
 
primary_docket_udf = udf(_extract_primary_docket, StringType())
 
chunk_text_udf = udf(
    lambda text: _chunk_text(text, CHUNK_SIZE, CHUNK_OVERLAP),
    ArrayType(StringType()),
)
 
 
# =============================================================================
# PANDAS UDF — EMBEDDING GENERATION
# =============================================================================
 
@pandas_udf(ArrayType(FloatType()))
def generate_embeddings(texts: pd.Series) -> pd.Series:
    """
    Generate sentence embeddings using all-MiniLM-L6-v2.
    Pandas UDF processes rows in batches not one at a time —
    significantly faster for CPU intensive embedding generation.
    Model loads once per batch, not once per row.
    384-dimensional vectors stored in Qdrant for semantic search.
    Model downloads automatically from HuggingFace on first run (~80MB).
    """
    from sentence_transformers import SentenceTransformer
    model = SentenceTransformer(EMBEDDING_MODEL)
    embeddings = model.encode(texts.tolist(), show_progress_bar=False)
    return pd.Series(embeddings.tolist())
 
 

 
def read_from_kafka(spark: SparkSession, schema: StructType):
    """
    Connect to Kafka and read the regulations topic as a stream.
    startingOffsets=latest means only process new messages —
    not reprocess everything from the beginning on restart.
    Spark uses checkpoints to track exact offset position.
    """
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BROKER)
        .option("subscribe", KAFKA_TOPIC)
        .option("kafka.group.id", KAFKA_CONSUMER_GROUP)
        .option("startingOffsets", "latest")
        .load()
        .select(from_json(col("value").cast("string"), schema).alias("doc"))
        .select("doc.*")
    )
 
 
def validate(df):
    """
    Drop rows missing required fields.
    document_number and title are the minimum required fields.
    text_to_process must not be null — use full_text if available,
    fall back to abstract. If both are null drop the row entirely.
    """
    return (
        df
        .filter(col("document_number").isNotNull())
        .filter(col("title").isNotNull())
        .withColumn(
            "text_to_process",
            when(col("full_text").isNotNull(), col("full_text"))
            .otherwise(col("abstract"))
        )
        .filter(col("text_to_process").isNotNull())
    )
 
 
def transform(df):
    """
    Apply all transformations to the validated DataFrame.
    Extracts agency fields, docket ID, chunks text, generates embeddings.
    Returns a DataFrame with one row per chunk ready for storage.
    """
    # extract agency and docket fields via UDFs
    enriched = (
        df
        .withColumn("agency_name",        agency_name_udf(col("agencies")))
        .withColumn("agency_id",          agency_id_udf(col("agencies")))
        .withColumn("agency_url",         agency_url_udf(col("agencies")))
        .withColumn("parent_agency_name", parent_agency_name_udf(col("agencies")))
        .withColumn("parent_agency_id",   parent_agency_id_udf(col("agencies")))
        .withColumn("primary_docket_id",  primary_docket_udf(col("docket_ids")))
    )
 
    # chunk text into overlapping paragraphs
    chunked = (
        enriched
        .withColumn("chunks", chunk_text_udf(col("text_to_process")))
        .drop("text_to_process", "full_text", "raw_text_url")
    )
 
    # explode chunks — one row per chunk
    exploded = (
        chunked
        .select("*", explode(col("chunks")).alias("chunk_text"))
        .drop("chunks")
    )
 
    # add chunk_index and chunk_id
    window = Window.partitionBy("document_number").orderBy(lit(1))
    with_index = (
        exploded
        .withColumn("chunk_index", (row_number().over(window) - 1))
        .withColumn(
            "chunk_id",
            concat(
                col("document_number"),
                lit("_"),
                col("chunk_index").cast(StringType()),
            )
        )
    )
 
    # generate embeddings — one 384-dim vector per chunk
    with_embeddings = with_index.withColumn(
        "embedding",
        generate_embeddings(col("chunk_text"))
    )
 
    return with_embeddings
 
 
def run() -> None:
    """
    Main entry point for the transformation pipeline.
    Starts two parallel streaming queries:
    1. Writes chunks + embeddings to Qdrant
    2. Writes document-level data to ArcadeDB
    Both run every 30 seconds processing whatever arrived in Kafka.
    Checkpoints track progress so restarts resume from last position.
    """
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
 
    logger.info("LawFlow transformation pipeline starting")
 
    schema = get_kafka_schema()
    raw = read_from_kafka(spark, schema)
    validated = validate(raw)
    transformed = transform(validated)
 
    # stream 1 — write chunks + embeddings to qdrant
    qdrant_query = (
        transformed
        .writeStream
        .foreachBatch(load_qdrant_batch)
        .option("checkpointLocation", "/tmp/checkpoints/qdrant")
        .trigger(processingTime="30 seconds")
        .start()
    )
 
    # stream 2 — deduplicate to document level then write to arcadedb
    doc_level = transformed.dropDuplicates(["document_number"])
 
    arcadedb_query = (
        doc_level
        .writeStream
        .foreachBatch(load_arcadedb_batch)
        .option("checkpointLocation", "/tmp/checkpoints/arcadedb")
        .trigger(processingTime="30 seconds")
        .start()
    )
 
    logger.info("Streaming queries running — waiting for data from Kafka")
    spark.streams.awaitAnyTermination()
 
 
if __name__ == "__main__":
    run()
 