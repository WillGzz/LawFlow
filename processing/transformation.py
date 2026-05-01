
import logging
import re
from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, from_json, when, explode, concat, lit, udf, pandas_udf, size)
from pyspark.sql.window import Window
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
    return (
        SparkSession.builder
        .appName("LawFlow")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")   # 8 partitions for shuffling data in local mode (low data volume)
        .config("spark.jars.packages",    #download the Kafka connector library.
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
        .getOrCreate()
    )
 
# org.apache.spark — Apache Spark organization
# spark-sql-kafka-0-10 — the Kafka connector package
# 2.12 — Scala version Spark was compiled with
# 3.5.0 — PySpark version 

 
def get_kafka_schema() -> StructType:
    
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
 

def _extract_primary_agency(agencies: list) -> dict | None:
    """
    Return the sub-agency — the one with a parent_id.
    If no sub-agency exists return the first agency.
    Example: for [DOE, FERC] returns FERC because FERC has parent_id=136.
    """
    if not agencies:
        return None
    sub = [a for a in agencies if a["parent_id"] is not None]
    return sub[0] if sub else agencies[0]
 
 
def _extract_parent_agency(agencies: list) -> dict | None:
    """
    Return the top-level parent agency — the one with parent_id=null.
    Returns None if only one agency or no parent found.
    Example: for [DOE, FERC] returns DOE because DOE has parent_id=null.
    """
    if not agencies or len(agencies) == 1:
        return None
    parents = [a for a in agencies if a["parent_id"] is None]
    return parents[0] if parents else None
 
 
def _extract_primary_docket(docket_ids: list) -> str | None:
    """
    Return the primary docket ID filtering out FRL numbers.
    FRL numbers are internal Federal Register tracking IDs unique
    per document — they cannot connect related documents.
    Agency-prefixed IDs (e.g. EPA-R02-OAR-2025-1047) are shared
    across related documents and used for graph relationships.
    Strips Docket No. prefix if present.
    """
    if not docket_ids:
        return None
    non_frl = [d for d in docket_ids if not d.upper().startswith("FRL")]
    primary = non_frl[0] if non_frl else docket_ids[0]
    return primary.replace("Docket No. ", "").strip()
 
 
def _clean_text(text: str) -> str:
    """
  
 
    - HTML tags including script blocks from Cloudflare email protection
    - Page markers [[Page 14306]] — print pagination artifacts
    - Footnote references \\1\\ \\2\\ — legal citation markers
    - Footnote content blocks that follow separator lines
    - Long separator lines of dashes used as section dividers
    - Form feed characters \\f used as page breaks in print format
    - HTML entities &amp; &#160; etc
    - Excessive whitespace and line breaks
    """
    if not text:
        return text
 
    # remove script tags and their full content
    text = re.sub(r'<script[^>]*>.*?</script>', '', text, flags=re.DOTALL)
 
    # remove all remaining html tags
    text = re.sub(r'<[^>]+>', '', text)
 
    # remove page markers [[Page 14306]]
    text = re.sub(r'\[\[Page \d+\]\]', '', text)
 
    # remove footnote references \1\ \2\ \3\ etc
    text = re.sub(r'\\\d+\\', '', text)
 
    # remove long separator lines of dashes (3 or more dashes)
    text = re.sub(r'-{3,}', '', text)
 
    # remove form feed characters used as page breaks
    text = text.replace('\f', ' ')
 
    # decode common html entities
    text = text.replace('&#160;', ' ')
    text = text.replace('&amp;', '&')
    text = text.replace('&lt;', '<')
    text = text.replace('&gt;', '>')
    text = text.replace('&nbsp;', ' ')
 
    # normalize all whitespace — tabs, newlines, multiple spaces
    text = re.sub(r'\s+', ' ', text).strip()
 
    return text
 
 
 
def _chunk_text(text: str, chunk_size: int, overlap: int) -> list[str]:
    """
    
    Tries to split on paragraph breaks first (\n\n), then sentences (\n),
    then periods, then spaces — preserving natural document structure.
 
    chunk_size in characters  — 2000 chars ~ 300-400 words ~ 2-3 paragraphs.
    overlap preserves context at chunk boundaries — last N characters of one
    chunk repeat at the start of the next so meaning is not lost at splits.
    """
    if not text:
        return []
    from langchain_text_splitters import RecursiveCharacterTextSplitter
    splitter = RecursiveCharacterTextSplitter(
        chunk_size=chunk_size,
        chunk_overlap=overlap,
        separators=["\n\n", "\n", ".", " "],
    )
    chunks = splitter.split_text(text)
    return [c.strip() for c in chunks if c.strip()]
 

# register UDFs with Spark
agency_name_udf = udf(
    lambda agencies: (
        _extract_primary_agency(agencies)["raw_name"]  #get raw name from primary agency if we have one 
        if _extract_primary_agency(agencies) else None
    ),
    StringType(),
)
 
agency_id_udf = udf(
    lambda agencies: (
        _extract_primary_agency(agencies)["id"]
        if _extract_primary_agency(agencies) else None
    ),
    IntegerType(),
)
 
agency_url_udf = udf(
    lambda agencies: (
        _extract_primary_agency(agencies)["url"]
        if _extract_primary_agency(agencies) else None
    ),
    StringType(),
)
 
parent_agency_name_udf = udf(
    lambda agencies: (
        _extract_parent_agency(agencies)["raw_name"].title()
        if _extract_parent_agency(agencies) else None
    ),
    StringType(),
)
 
parent_agency_id_udf = udf(
    lambda agencies: (
        _extract_parent_agency(agencies)["id"]
        if _extract_parent_agency(agencies) else None
    ),
    IntegerType(),
)
 
primary_docket_udf = udf(_extract_primary_docket, StringType())
 
clean_text_udf = udf(_clean_text, StringType())
 
chunk_text_udf = udf(
    lambda text: [
        (i, chunk)
        for i, chunk in enumerate(_chunk_text(text, cfg.CHUNK_SIZE, cfg.CHUNK_OVERLAP))
    ],
    ArrayType(StructType([
        StructField("chunk_index", IntegerType(), False),
        StructField("chunk_text", StringType(), False),
    ]))
)
 
_model = None

@udf(ArrayType(FloatType()))
def generate_embeddings(text: str) -> list:
    if not text:
        return []
    global _model
    if _model is None:
        from sentence_transformers import SentenceTransformer
        _model = SentenceTransformer(cfg.EMBEDDING_MODEL)
    embedding = _model.encode([text], show_progress_bar=False)
    return [float(x) for x in embedding[0]]
 
def read_from_kafka(spark: SparkSession, schema: StructType):
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", cfg.KAFKA_BROKER)
        .option("subscribe", cfg.KAFKA_TOPIC)
        .option("kafka.group.id", cfg.KAFKA_CONSUMER_GROUP)
        .option("startingOffsets", "earliest")
        .load()
        .select(from_json(col("value").cast("string"), schema).alias("doc"))
        .select("doc.*")
    )
 
 
def validate(df):
    """
    Drop rows missing required fields.
    document_number, title, and agencies are required.
    agencies must be non-empty — every Federal Register document
    has an agency by definition.

    Resolves text — use full_text if available, fall back to abstract.
    Drops rows where both are null — no text means no embeddings.
    """
    return (
        df
        .filter(col("document_number").isNotNull())
        .filter(col("title").isNotNull())
        .filter(col("agencies").isNotNull())
        .filter(size(col("agencies")) > 0)
        .withColumn(
            "processed_text",
            when(col("full_text").isNotNull(), col("full_text"))
            .otherwise(col("abstract"))
        )
        .filter(col("processed_text").isNotNull())
    )
 
 
def transform(df):
    """
    
    Steps
    1. Extract agency fields via UDFs — name, id, url, parent
    2. Extract primary docket ID filtering out FRL numbers
    3. Clean processed_text — remove HTML, page markers, footnotes, separators
    4. Chunk text — split using RecursiveCharacterTextSplitter
    5. Drop raw text columns no longer needed
    6. Explode chunks — one row per chunk
    7. Add chunk_index and chunk_id
    8. Generate embeddings — 1024-dim vector per chunk
    9. Deduplicate on chunk_id — prevent duplicate chunks
    """
 
    
    updated_df = (
        df
        .withColumn("agency_name",        agency_name_udf(col("agencies")))
        .withColumn("agency_id",          agency_id_udf(col("agencies")))
        .withColumn("agency_url",         agency_url_udf(col("agencies")))
        .withColumn("parent_agency_name", parent_agency_name_udf(col("agencies")))
        .withColumn("parent_agency_id",   parent_agency_id_udf(col("agencies")))
    )
 
    with_docket = updated_df.withColumn(
        "primary_docket_id",
        primary_docket_udf(col("docket_ids"))
    )
 

    with_clean = with_docket.withColumn(
        "processed_text",
        clean_text_udf(col("processed_text"))
    )
 
    
    chunked = (
        with_clean
        .withColumn("chunks", chunk_text_udf(col("processed_text")))
        .drop("processed_text", "full_text", "raw_text_url", "agencies")
    )
 
    exploded = (
    chunked
        .select("*", explode(col("chunks")).alias("chunk"))
        .drop("chunks")
        .withColumn("chunk_index", col("chunk.chunk_index"))
        .withColumn("chunk_text", col("chunk.chunk_text"))
        .drop("chunk")
    )

    with_index = exploded.withColumn(
    "chunk_id",
        concat(
            col("document_number"),
            lit("_"),
            col("chunk_index").cast(StringType()),
        )
    )
 
    
    with_embeddings = with_index.withColumn(
        "embedding",
        generate_embeddings(col("chunk_text"))
    )
 

    deduped_df = with_embeddings.dropDuplicates(["chunk_id"])
 
    return deduped_df
 

def run() -> None:
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
        .option("checkpointLocation", "/app/checkpoints/qdrant")
        .trigger(once=True)  
        .start()
    )
 
    # stream 2 — deduplicate to document level then write to arcadedb
    doc_level = transformed.dropDuplicates(["document_number"])
 
    arcadedb_query = (
        doc_level
        .writeStream
        .foreachBatch(load_arcadedb_batch)
        .option("checkpointLocation", "/app/checkpoints/arcadedb")
        .trigger(once=True)  
        .start()
    )
 
    logger.info("Streaming queries running — waiting for data from Kafka")
    # spark.streams.awaitAnyTermination()

    qdrant_query.awaitTermination()
    arcadedb_query.awaitTermination()
 
 
if __name__ == "__main__":
    run()