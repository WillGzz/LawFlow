import json
import logging

from config.settings import (
    ARCADEDB_HOST,
    ARCADEDB_PORT,
    ARCADEDB_USER,
    ARCADEDB_PASSWORD,
    ARCADEDB_DATABASE,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)


def execute(sql: str, auth: tuple, base_url: str) -> dict:
  
    import requests

    payload = {"language": "sql", "command": sql}
    response = requests.post(
        f"{base_url}/command/{ARCADEDB_DATABASE}",
        json=payload,
        auth=auth,
        headers={"Content-Type": "application/json"},
        timeout=30,
    )
    response.raise_for_status()
    return response.json()


def setup_schema(auth: tuple, base_url: str) -> None:
    """
    Create vertex and edge types on first run.
    ArcadeDB requires types to be defined before inserting records.
    Safe to run on every batch — IF NOT EXISTS prevents errors on re-runs.
    Must be called before any upsert or edge creation operations.
    """
    import requests

    # create database if it doesn't exist
    # database creation uses a different endpoint
    try:
        response = requests.post(
            f"{base_url.replace('/api/v1', '')}/api/v1/create/{ARCADEDB_DATABASE}",
            auth=auth,
            headers={"Content-Type": "application/json"},
            timeout=30,
        )
        # 200 = created, 500 with "already exists" = fine, anything else = problem
        if response.status_code not in (200, 500):
            response.raise_for_status()
    except Exception as e:
        # database may already exist — continue
        logger.debug(f"Database creation: {e}")

    vertex_types = ["Agency", "Document", "Docket"]
    for vtype in vertex_types:
        try:
            execute(f"CREATE VERTEX TYPE {vtype} IF NOT EXISTS", auth, base_url)
        except Exception as e:
            logger.debug(f"Vertex type {vtype}: {e}")

    # create edge types
    edge_types = ["PUBLISHED", "PARENT_OF", "PART_OF", "SUPERSEDES"]
    for etype in edge_types:
        try:
            execute(f"CREATE EDGE TYPE {etype} IF NOT EXISTS", auth, base_url)
        except Exception as e:
            logger.debug(f"Edge type {etype}: {e}")

    logger.info("ArcadeDB schema setup complete")


def upsert_vertex(vertex_type: str, key_field: str, key_value: str,
                  fields: dict, auth: tuple, base_url: str) -> None:
    """
    Upsert a vertex — update if exists, insert if not.
    Prevents duplicate vertices when the same document
    is ingested more than once.
    Skips None values — only sets fields that have data.
    Escapes single quotes in string values to prevent SQL injection.
    """
    field_assignments = []
    for k, v in fields.items():
        if v is None:
            continue
        if isinstance(v, str):
            v_escaped = v.replace("'", "\\'")
            field_assignments.append(f"{k} = '{v_escaped}'")
        elif isinstance(v, list):
            field_assignments.append(f"{k} = {json.dumps(v)}")
        else:
            field_assignments.append(f"{k} = {v}")

    if not field_assignments:
        return

    field_str = ", ".join(field_assignments)
    sql = f"UPDATE {vertex_type} SET {field_str} UPSERT WHERE {key_field} = '{key_value}'"
    execute(sql, auth, base_url)


def edge_exists(edge_type: str, from_type: str, from_key: str, from_val: str,
                to_type: str, to_key: str, to_val: str,
                auth: tuple, base_url: str) -> bool:
    """
    Check if an edge already exists between two vertices.
    ArcadeDB does not support IF NOT EXISTS on CREATE EDGE.
    We check manually before creating to prevent duplicates on re-runs.
    """
    sql = (
        f"SELECT FROM {edge_type} "
        f"WHERE out.{from_key} = '{from_val}' "
        f"AND in.{to_key} = '{to_val}'"
    )
    result = execute(sql, auth, base_url)
    return len(result.get("result", [])) > 0


def upsert_edge(edge_type: str, from_type: str, from_key: str, from_val: str,
                to_type: str, to_key: str, to_val: str,
                auth: tuple, base_url: str) -> None:
    """
    Create an edge between two vertices if it does not already exist.
    Checks for existence first then creates — ArcadeDB does not support
    IF NOT EXISTS on CREATE EDGE unlike vertex types.
    """
    if edge_exists(edge_type, from_type, from_key, from_val,
                   to_type, to_key, to_val, auth, base_url):
        return

    sql = f"""
        CREATE EDGE {edge_type}
        FROM (SELECT FROM {from_type} WHERE {from_key} = '{from_val}')
        TO   (SELECT FROM {to_type}   WHERE {to_key}   = '{to_val}')
    """.strip()
    execute(sql, auth, base_url)


def load_document(row, auth: tuple, base_url: str) -> None:
    """
    Load one document and its relationships into ArcadeDB.

    Graph operations in order:
    1. Upsert primary Agency vertex
    2. Upsert parent Agency vertex if exists
    3. Create PARENT_OF edge from parent to sub-agency
    4. Upsert Document vertex
    5. Create PUBLISHED edge from Agency to Document
    6. Upsert Docket vertex if primary docket id exists
    7. Create PART_OF edge from Document to Docket
    8. Create SUPERSEDES edge if Final Rule and Proposed Rule
       exists in the same docket
    """
    doc_num = row["document_number"]

    # 1. upsert primary agency vertex
    upsert_vertex(
        "Agency", "agency_id", str(row["agency_id"]),
        {
            "agency_id": row["agency_id"],
            "name":      row["agency_name"],
            "url":       row["agency_url"],
        },
        auth, base_url,
    )

    # 2. upsert parent agency vertex if exists
    if row["parent_agency_id"]:
        upsert_vertex(
            "Agency", "agency_id", str(row["parent_agency_id"]),
            {
                "agency_id": row["parent_agency_id"],
                "name":      row["parent_agency_name"],
            },
            auth, base_url,
        )

        # 3. create PARENT_OF edge from parent to sub-agency
        upsert_edge(
            "PARENT_OF",
            "Agency", "agency_id", str(row["parent_agency_id"]),
            "Agency", "agency_id", str(row["agency_id"]),
            auth, base_url,
        )

    # 4. upsert document vertex
    upsert_vertex(
        "Document", "document_number", doc_num,
        {
            "document_number":   doc_num,
            "title":             row["title"],
            "type":              row["type"],
            "action":            row["action"],
            "publication_date":  row["publication_date"],
            "effective_on":      row["effective_on"],
            "comments_close_on": row["comments_close_on"],
            "dates":             row["dates"],
            "page_length":       row["page_length"],
            "abstract":          row["abstract"][:500] if row["abstract"] else None,
            "html_url":          row["html_url"],
            "primary_docket_id": row["primary_docket_id"],
            "topics":            row["topics"],
            "docket_ids":        row["docket_ids"],
        },
        auth, base_url,
    )

    # 5. create PUBLISHED edge from agency to document
    upsert_edge(
        "PUBLISHED",
        "Agency",   "agency_id",       str(row["agency_id"]),
        "Document", "document_number", doc_num,
        auth, base_url,
    )

    # 6. upsert docket vertex if primary docket id exists
    if row["primary_docket_id"]:
        upsert_vertex(
            "Docket", "docket_id", row["primary_docket_id"],
            {"docket_id": row["primary_docket_id"]},
            auth, base_url,
        )

        # 7. create PART_OF edge from document to docket
        upsert_edge(
            "PART_OF",
            "Document", "document_number", doc_num,
            "Docket",   "docket_id",       row["primary_docket_id"],
            auth, base_url,
        )

        # 8. create SUPERSEDES edge if this is a final rule
        if row["type"] in ("Rule", "RULE"):
            result = execute(
                f"SELECT document_number FROM Document "
                f"WHERE primary_docket_id = '{row['primary_docket_id']}' "
                f"AND type IN ['Proposed Rule', 'PRORULE']",
                auth, base_url,
            )
            proposed_docs = result.get("result", [])
            for proposed in proposed_docs:
                proposed_num = proposed.get("document_number")
                if proposed_num:
                    upsert_edge(
                        "SUPERSEDES",
                        "Document", "document_number", doc_num,
                        "Document", "document_number", proposed_num,
                        auth, base_url,
                    )
                    logger.info(f"Created SUPERSEDES edge: {doc_num} -> {proposed_num}")


def load_arcadedb_batch(batch_df, batch_id: int) -> None:
    """

    Imports inside function because foreachBatch runs on Spark workers
    which do not have access to driver-level imports.
    Continues on individual document failures — one bad document
    does not stop the entire batch.
    """
    if batch_df.rdd.isEmpty():
        logger.info(f"Batch {batch_id} — empty, skipping ArcadeDB write")
        return

    base_url = f"http://{ARCADEDB_HOST}:{ARCADEDB_PORT}/api/v1"
    auth = (ARCADEDB_USER, ARCADEDB_PASSWORD)

    # ensure schema exists before writing
    # safe to call on every batch — IF NOT EXISTS on all operations
    setup_schema(auth, base_url)

    rows = batch_df.collect()
    success_count = 0
    fail_count = 0

    for row in rows:
        try:
            load_document(row, auth, base_url)
            success_count += 1
            logger.info(f"Batch {batch_id} — loaded {row['document_number']} to ArcadeDB")
        except Exception as e:
            fail_count += 1
            logger.error(f"Batch {batch_id} — failed to load {row.get('document_number', 'unknown')}: {e}")
            continue

    logger.info(f"Batch {batch_id} — ArcadeDB complete: success={success_count}, failed={fail_count}")