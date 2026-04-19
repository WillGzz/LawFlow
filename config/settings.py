import os
from dotenv import load_dotenv

load_dotenv()

# Kafka
KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "regulations"
KAFKA_CONSUMER_GROUP = "spark-processor"
KAFKA_PARTITIONS = 3
KAFKA_REPLICATION_FACTOR = 1

# Federal Register API
FEDERAL_REGISTER_URL = "https://www.federalregister.gov/api/v1/documents.json"
FEDERAL_REGISTER_PARAMS = {
    "per_page": 20,
    "order": "newest",
    "conditions[agencies][]": [
        "energy-department",
        "environmental-protection-agency",
    ],
    "conditions[type][]": ["RULE", "PRORULE"],
}

# Qdrant
QDRANT_HOST = "qdrant"
QDRANT_PORT = 6333
QDRANT_COLLECTION = "regulations"

# ArcadeDB
ARCADEDB_HOST = "arcadedb"
ARCADEDB_PORT = 2480
ARCADEDB_USER = "root"
ARCADEDB_PASSWORD = os.getenv("ARCADEDB_PASSWORD")
ARCADEDB_DATABASE = "lawflow"

# Anthropic
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY")
ANTHROPIC_MODEL = "claude-sonnet-4-6"

# Groq
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
LLM_PROVIDER="groq"
GROQ_MODEL="groq/compound"


# Processing
CHUNK_SIZE = 1500    # ~375 tokens, within 512 limit, ~1-2 regulatory paragraphs
CHUNK_OVERLAP = 150  # 10% overlap, preserves sentence context at boundaries
EMBEDDING_MODEL = "intfloat/multilingual-e5-large"
