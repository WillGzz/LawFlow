import logging
import os
from typing import Any

from langchain.agents import AgentExecutor, create_react_agent
from langchain.tools import tool
from langchain.prompts import PromptTemplate
from qdrant_client import QdrantClient
from qdrant_client.models import Filter, FieldCondition, MatchValue
from sentence_transformers import SentenceTransformer
import requests

from config.settings import (
    ANTHROPIC_API_KEY,
    ANTHROPIC_MODEL,
    GROQ_API_KEY,
    GROQ_MODEL,
    LLM_PROVIDER,
    QDRANT_HOST,
    QDRANT_PORT,
    QDRANT_COLLECTION,
    ARCADEDB_HOST,
    ARCADEDB_PORT,
    ARCADEDB_USER,
    ARCADEDB_PASSWORD,
    ARCADEDB_DATABASE,
    EMBEDDING_MODEL,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


qdrant_client = QdrantClient(host=QDRANT_HOST, port=QDRANT_PORT)
embedding_model = SentenceTransformer(EMBEDDING_MODEL)



# Tools are functions the agent can call to retrieve information.
# The docstring is critical — the LLM reads it to decide WHEN to use each tool.
# Clear descriptions = better tool selection = better answers.

@tool
def search_regulations(query: str) -> str:
    """
    Search for regulatory documents semantically similar to the query.
    Use this tool when the user asks about:
    - Specific regulations, rules, or policy updates
    - What regulations changed recently on a topic
    - What an agency published about a subject
    - Details about a specific regulatory requirement
    - Proposed rules open for public comment
    Returns the most relevant regulatory text chunks with metadata.
    """
    try:
        # convert query to embedding using same model as ingestion
        query_vector = embedding_model.encode(query).tolist()

        # search qdrant for most similar chunks
        results = qdrant_client.search(
            collection_name=QDRANT_COLLECTION,
            query_vector=query_vector,
            limit=3,
            with_payload=True,
        )

        if not results:
            return "No relevant regulatory documents found for this query."

        # format results for the LLM
        formatted = []
        for i, result in enumerate(results, 1):
            payload = result.payload
            formatted.append(
                f"Result {i}:\n"
                f"Title: {payload.get('title', 'Unknown')}\n"
                f"Agency: {payload.get('agency', 'Unknown')}\n"
                f"Type: {payload.get('type', 'Unknown')}\n"
                f"Published: {payload.get('publication_date', 'Unknown')}\n"
                f"Effective: {payload.get('effective_on', 'N/A')}\n"
                f"Comment Deadline: {payload.get('comments_close_on', 'N/A')}\n"
                f"Docket: {payload.get('docket_id', 'N/A')}\n"
                f"Source: {payload.get('html_url', 'N/A')}\n"
                f"Content:\n{payload.get('chunk_text', '')}\n"
            )

        return "\n---\n".join(formatted)

    except Exception as e:
        logger.error(f"Qdrant search failed: {e}")
        return f"Search failed: {str(e)}"


@tool
def get_regulatory_context(document_number: str) -> str:
    """
    Get graph context for a regulatory document from ArcadeDB.
    Use this tool when you need to understand:
    - Which agency published a specific document
    - The full regulatory history of a rule (proposed + final)
    - Whether a proposed rule has been finalized
    - What other documents are in the same regulatory proceeding
    - The parent agency hierarchy for a sub-agency
    Requires a document_number (e.g. 2026-05709).
    """
    try:
        base_url = f"http://{ARCADEDB_HOST}:{ARCADEDB_PORT}/api/v1"
        auth = (ARCADEDB_USER, ARCADEDB_PASSWORD)
        headers = {"Content-Type": "application/json"}

        def query(sql: str) -> list:
            response = requests.post(
                f"{base_url}/command/{ARCADEDB_DATABASE}",
                json={"language": "sql", "command": sql},
                auth=auth,
                headers=headers,
                timeout=15,
            )
            response.raise_for_status()
            return response.json().get("result", [])

        # get the document
        docs = query(
            f"SELECT * FROM Document WHERE document_number = '{document_number}'"
        )
        if not docs:
            return f"No document found with number {document_number}"

        doc = docs[0]
        result_parts = []

        # document details
        result_parts.append(
            f"Document: {doc.get('document_number')}\n"
            f"Title: {doc.get('title')}\n"
            f"Type: {doc.get('type')}\n"
            f"Action: {doc.get('action')}\n"
            f"Published: {doc.get('publication_date')}\n"
            f"Effective: {doc.get('effective_on', 'N/A')}\n"
            f"Comment Deadline: {doc.get('comments_close_on', 'N/A')}\n"
            f"Dates: {doc.get('dates', 'N/A')}\n"
            f"Topics: {doc.get('topics', [])}\n"
            f"Source: {doc.get('html_url')}"
        )

        # get publishing agency
        agencies = query(
            f"SELECT expand(in('PUBLISHED')) FROM Document "
            f"WHERE document_number = '{document_number}'"
        )
        if agencies:
            agency = agencies[0]
            result_parts.append(
                f"\nPublished by: {agency.get('name')}\n"
                f"Agency URL: {agency.get('url')}"
            )

            # get parent agency
            parents = query(
                f"SELECT expand(in('PARENT_OF')) FROM Agency "
                f"WHERE agency_id = {agency.get('agency_id')}"
            )
            if parents:
                result_parts.append(f"Parent Agency: {parents[0].get('name')}")

        # get related documents in same docket
        docket_id = doc.get("primary_docket_id")
        if docket_id:
            related = query(
                f"SELECT document_number, title, type, publication_date "
                f"FROM Document WHERE primary_docket_id = '{docket_id}' "
                f"AND document_number != '{document_number}'"
            )
            if related:
                result_parts.append(f"\nRelated documents in docket {docket_id}:")
                for r in related:
                    result_parts.append(
                        f"  - {r.get('document_number')} | "
                        f"{r.get('type')} | "
                        f"{r.get('publication_date')} | "
                        f"{r.get('title', '')[:80]}"
                    )

        # check if this document supersedes another
        superseded = query(
            f"SELECT expand(out('SUPERSEDES')) FROM Document "
            f"WHERE document_number = '{document_number}'"
        )
        if superseded:
            result_parts.append(
                f"\nThis Final Rule supersedes: "
                f"{superseded[0].get('document_number')} "
                f"({superseded[0].get('title', '')[:80]})"
            )

        return "\n".join(result_parts)

    except Exception as e:
        logger.error(f"ArcadeDB query failed: {e}")
        return f"Graph query failed: {str(e)}"


@tool
def search_by_agency(agency_name: str) -> str:
    """
    Find all recent documents published by a specific agency.
    Use this tool when the user asks:
    - What has the EPA published recently
    - Show me all DOE regulations
    - What rules has FERC issued
    Returns a list of recent documents from that agency.
    """
    try:
        base_url = f"http://{ARCADEDB_HOST}:{ARCADEDB_PORT}/api/v1"
        auth = (ARCADEDB_USER, ARCADEDB_PASSWORD)

        response = requests.post(
            f"{base_url}/command/{ARCADEDB_DATABASE}",
            json={
                "language": "sql",
                "command": (
                    f"SELECT document_number, title, type, publication_date, "
                    f"effective_on, topics, html_url "
                    f"FROM Document WHERE agency_name LIKE '%{agency_name}%' "
                    f"ORDER BY publication_date DESC LIMIT 10"
                ),
            },
            auth=auth,
            headers={"Content-Type": "application/json"},
            timeout=15,
        )
        response.raise_for_status()
        results = response.json().get("result", [])

        if not results:
            return f"No documents found for agency: {agency_name}"

        formatted = [f"Recent documents from {agency_name}:\n"]
        for doc in results:
            formatted.append(
                f"- {doc.get('document_number')} | "
                f"{doc.get('type')} | "
                f"{doc.get('publication_date')} | "
                f"{doc.get('title', '')[:80]}\n"
                f"  Effective: {doc.get('effective_on', 'N/A')} | "
                f"Source: {doc.get('html_url', 'N/A')}"
            )

        return "\n".join(formatted)

    except Exception as e:
        logger.error(f"Agency search failed: {e}")
        return f"Agency search failed: {str(e)}"



# AGENT PROMPT
# The system prompt tells the LLM its role, what tools it has, and how to format responses.
# The ReAct format requires specific placeholders:
# {tools} — list of available tools
# {tool_names} — tool names only
# {agent_scratchpad} — where the agent writes its reasoning
# {input} — the user question

AGENT_PROMPT = PromptTemplate.from_template("""
You are LawFlow, an AI regulatory intelligence agent. You help legal teams,
compliance officers, and business professionals understand and navigate
US federal regulatory changes from the EPA and Department of Energy.

You have access to a real-time database of regulatory documents from the
Federal Register. Always ground your answers in the retrieved documents.
When citing a document always include the source URL.

Available tools:
{tools}

Use the following format:
Question: the input question you must answer
Thought: think about what tool to use and why
Action: the action to take, should be one of [{tool_names}]
Action Input: the input to the action
Observation: the result of the action
... (this Thought/Action/Action Input/Observation can repeat N times)
Thought: I now know the final answer
Final Answer: the final answer to the original input question

Always:
- Cite document numbers and source URLs in your answer
- Mention effective dates and comment deadlines when relevant
- Indicate whether a rule is Final or Proposed
- If a proposed rule has been superseded by a final rule mention it

Question: {input}
{agent_scratchpad}
""")


def build_agent() -> AgentExecutor:
    """
  
    ReAct = Reason + Act — the agent reasons about which tool to use,
    calls it, observes the result, reasons again, repeats until
    it has enough information to give a final answer.

    verbose=True logs each reasoning step — useful for debugging.
    max_iterations=3 prevents infinite loops if the agent gets stuck.
    """
    if LLM_PROVIDER == "groq":
        from langchain_groq import ChatGroq
        llm = ChatGroq(
            model=GROQ_MODEL,
            api_key=GROQ_API_KEY,
        )
        logger.info(f"Using Groq LLM: {GROQ_MODEL}")
    else:
        from langchain_anthropic import ChatAnthropic
        llm = ChatAnthropic(
            model=ANTHROPIC_MODEL,
            api_key=ANTHROPIC_API_KEY,
            max_tokens=2048,
        )
        logger.info(f"Using Anthropic LLM: {ANTHROPIC_MODEL}")

    tools = [search_regulations, get_regulatory_context, search_by_agency]

    agent = create_react_agent(
        llm=llm,
        tools=tools,
        prompt=AGENT_PROMPT,
    )

    return AgentExecutor(
        agent=agent,
        tools=tools,
        verbose=True,
        max_iterations=3,
        handle_parsing_errors=True,
    )


# Build the agent once at module level — avoids re-initialization overhead on every question.
agent_executor = build_agent()


def run(question: str) -> str:
    """
    Run the agent with a user question and return the answer.
    Called by FastAPI when a user submits a question through the frontend.
    """
    logger.info(f"Agent received question: {question}")
    try:
        result = agent_executor.invoke({"input": question})
        answer = result.get("output", "I was unable to find an answer.")
        logger.info("Agent completed successfully")
        return answer
    except Exception as e:
        logger.error(f"Agent failed: {e}")
        return f"I encountered an error processing your question: {str(e)}"

