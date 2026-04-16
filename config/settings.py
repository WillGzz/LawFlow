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


# Processing
CHUNK_SIZE = 512
CHUNK_OVERLAP = 50
EMBEDDING_MODEL = "all-MiniLM-L6-v2"