Currently working on LawFlow, a real-time legal intelligence Data pipeline that ingests regulatory updates from the Federal Register, transforms and chunks the documents with Spark, stores embeddings in Qdrant for semantic search and entity relationships in ArcadeDB, and surfaces insights through an AI agent."

Airflow — DAG runs daily, hits Federal Register API, produces documents to Kafka
Kafka ingests Federal Register updates
Spark cleans + chunks the documents
Embeddings generated + stored in Qdrant
Entities (agencies, topics, dates) stored in ArcadeDB
Simple agent that answers "what regulations changed recently around X topic?"