# LawFlow

An intelligent RAG data pipeline that ingests US federal regulations from the Environmental Protection Agency and Department of Energy and gives compliance teams and legal professionals insights on current and upcoming regulatory changes.

---

## What Problem Does It Solve?

Keeping up with federal regulations is time consuming. New rules and proposed rules publish every business day and missing a change can mean a compliance violation. LawFlow automates the monitoring process — ingesting documents daily, making them searchable, and letting you ask plain English questions to get cited answers instantly.

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    ETL PIPELINE                          │
│                                                          │
│  Airflow (9am Mon-Fri)                                   │
│      │                                                   │
│      ▼                                                   │
│  producer.py ──► Federal Register API                    │
│      │           (Rules + Proposed Rules)                │
│      │                                                   │
│      ▼                                                   │
│  Kafka (KRaft, topic: regulations)                       │
│      │                                                   │
│      ▼                                                   │
│  PySpark Structured Streaming                            │
│      │   - Schema validation                             │
│      │   - Text chunking (1500 chars / 150 overlap)      │
│      │   - Embedding (multilingual-e5-large, 1024-dim)   │
│      │   - Agency + docket extraction                    │
│      │                                                   │
│      ├──► Qdrant (vector store — chunk embeddings)       │
│      └──► ArcadeDB (graph — Agency/Document/Docket)      │
└─────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────┐
│                 RETRIEVAL PIPELINE                       │
│                                                          │
│  Chat UI (HTML/CSS/JS)                                   │
│      │                                                   │
│      ▼                                                   │
│  FastAPI /query endpoint                                 │
│      │                                                   │
│      ▼                                                   │
│  LangChain ReAct Agent                                   │
│      │   Tool 1: search_regulations → Qdrant             │
│      │   Tool 2: get_regulatory_context → ArcadeDB       │
│      │   Tool 3: search_by_agency → ArcadeDB             │
│      │                                                   │
│      ▼                                                   │
│  LLM (Llama 3.3 70B via Groq)                           │
│      │                                                   │
│      ▼                                                   │
│  Grounded answer with citations                          │
└─────────────────────────────────────────────────────────┘
```

---

## Tech Stack

| Component | Technology |
|---|---|
| Orchestration | Apache Airflow 2.8 |
| Message broker | Apache Kafka (KRaft) |
| Stream processing | PySpark 3.5 Structured Streaming |
| Embeddings | `intfloat/multilingual-e5-large` (1024-dim, 100+ languages) |
| Vector database | Qdrant |
| Graph database | ArcadeDB |
| Agent framework | LangChain ReAct |
| LLM | Llama 3.3 70B via Groq |
| API server | FastAPI + uvicorn |
| Frontend | Vanilla HTML/CSS/JS |
| Containerization | Docker Compose |

---

## Graph Model

ArcadeDB models the regulatory lifecycle as a property graph:

**Vertices:** `Agency`, `Document`, `Docket`

**Edges:**
- `PUBLISHED` — Agency → Document
- `PARENT_OF` — Agency → Sub-Agency
- `PART_OF` — Document → Docket
- `SUPERSEDES` — Final Rule → Proposed Rule

This enables queries like "has this proposed rule been finalized?" and "what other documents are in the same regulatory proceeding?" that a vector database alone cannot answer.

## Vector Database

Qdrant stores document chunk embeddings for semantic search.

- Each document is split into 1500 character chunks with 150 character overlap
- Chunks are embedded into 1024-dimensional vectors using `intfloat/multilingual-e5-large`
- Vectors are stored alongside full document metadata — title, agency, dates, source URL
- At query time the user's question is embedded with the same model and Qdrant retrieves the most similar chunks via cosine similarity

This enables retrieval by meaning rather than exact keyword match — searching "emissions limits" surfaces documents about "pollution standards" even if the exact words differ.

## Prerequisites

- Docker Desktop (16GB RAM recommended)
- Groq API key — [console.groq.com](https://console.groq.com)

---

## Setup

**1. Clone the repository**
```bash
git clone https://github.com/yourusername/lawflow.git
cd lawflow
```

**2. Create `.env` file**
```
GROQ_API_KEY=...
GROQ_MODEL=llama-3.3-70b-versatile
LLM_PROVIDER=groq

ARCADEDB_PASSWORD=your_password
ARCADEDB_USER=root
ARCADEDB_HOST=arcadedb
ARCADEDB_PORT=2480
ARCADEDB_DATABASE=lawflow

QDRANT_HOST=qdrant
QDRANT_PORT=6333
QDRANT_COLLECTION=regulations

KAFKA_BROKER=kafka:9092
KAFKA_TOPIC=regulations
KAFKA_CONSUMER_GROUP=lawflow-spark

EMBEDDING_MODEL=intfloat/multilingual-e5-large
```

**3. Build and start containers**
```bash
docker compose up --build
```

**4. Run the pipeline manually (first run)**
```bash
docker exec lawflow python ingestion/producer.py
docker exec lawflow python processing/transformation.py
```

**5. Open the chat UI**
```
http://localhost:8000
```

**6. Access Airflow**
```
http://localhost:8081
```
The `lawflow_pipeline` DAG runs automatically at 9am Monday through Friday.

---

## Example Queries

- *"What EPA rules changed recently about air quality?"*
- *"Show me all DOE proposed rules open for public comment"*
- *"What regulations has FERC issued this month?"*
- *"Has any proposed EPA air quality rule been finalized recently?"*
- *"What are the latest energy conservation standards from the Department of Energy?"*

---

*Built for ESG compliance teams and legal professionals navigating US federal regulatory changes.*