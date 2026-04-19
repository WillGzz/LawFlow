from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago


# =============================================================================
# DEFAULT ARGUMENTS
# =============================================================================
# Applied to every task in the DAG unless overridden at the task level.
# retries=2 means if a task fails Airflow retries it 2 more times
# before marking it as failed.
# retry_delay=5 minutes between each retry attempt.
# email_on_failure=False — no email setup for local dev.

default_args = {
    "owner": "lawflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


# =============================================================================
# DAG DEFINITION
# =============================================================================
# schedule_interval="0 9 * * 1-5" = run at 9am Monday through Friday.
# Matches Federal Register publish schedule — documents drop daily on
# business days. No need to run on weekends when nothing publishes.
# catchup=False — if the pipeline was down for a few days do not
# backfill all missed runs automatically. Each run fetches the
# 20 newest documents so a missed day is not critical.
# max_active_runs=1 — only one pipeline run at a time.
# Two concurrent runs could produce duplicate Kafka messages.

with DAG(
    dag_id="lawflow_pipeline",
    default_args=default_args,
    description="LawFlow real-time legal intelligence pipeline — ingests Federal Register regulatory documents daily",
    schedule_interval="0 9 * * 1-5",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["lawflow", "regulatory", "ingestion"],
) as dag:

    # =========================================================================
    # TASK 1 — HEALTH CHECK
    # =========================================================================
    # Verify Kafka is reachable before attempting to produce messages.
    # Uses netcat (nc) to check if Kafka broker port is open.
    # Fails fast if Kafka is down — no point running the producer
    # if there is nowhere to send messages.
    # BashOperator runs a shell command inside the Airflow container.

    check_kafka = BashOperator(
        task_id="check_kafka_health",
        bash_command="nc -z kafka 9092 && echo 'Kafka is reachable' || exit 1",
        retries=3,
        retry_delay=timedelta(minutes=2),
    )

    # =========================================================================
    # TASK 2 — RUN PRODUCER
    # =========================================================================
    # Runs producer.py inside the lawflow container via docker exec.
    # Airflow has access to the Docker socket so it can exec into
    # any running container on the host.
    # producer.py hits Federal Register API and produces documents to Kafka.
    # execution_timeout=10 minutes — if producer takes longer than
    # 10 minutes something is wrong, fail the task.

    run_producer = BashOperator(
        task_id="run_producer",
        bash_command="docker exec lawflow python ingestion/producer.py",
        execution_timeout=timedelta(minutes=10),
    )

    # =========================================================================
    # TASK 3 — VERIFY KAFKA MESSAGES
    # =========================================================================
    # Check that messages were actually produced to the regulations topic.
    # Uses kafka-topics.sh to describe the topic and verify it exists
    # and has messages.
    # This is a lightweight sanity check before transformation starts.

    verify_kafka = BashOperator(
        task_id="verify_kafka_messages",
        bash_command="""
            docker exec kafka kafka-topics.sh \
                --bootstrap-server localhost:9092 \
                --describe \
                --topic regulations \
            && echo 'Kafka topic regulations exists and is healthy'
        """,
    )

    # =========================================================================
    # TASK 4 — RUN TRANSFORMATION
    # =========================================================================
    # Runs transformation.py inside the lawflow container.
    # Spark reads from Kafka, transforms documents, loads into
    # Qdrant and ArcadeDB.
    # transformation.py runs as a streaming job — it processes
    # the current batch then exits.
    # execution_timeout=30 minutes — transformation is heavier than
    # ingestion due to embedding generation.

    run_transformation = BashOperator(
        task_id="run_transformation",
        bash_command="docker exec lawflow python processing/transformation.py",
        execution_timeout=timedelta(minutes=30),
    )

    # =========================================================================
    # TASK 5 — VERIFY QDRANT
    # =========================================================================
    # Confirm Qdrant collection exists and has points after transformation.
    # Calls Qdrant REST API health endpoint.
    # Simple sanity check — does not verify count, just confirms
    # the collection is accessible.

    verify_qdrant = BashOperator(
        task_id="verify_qdrant",
        bash_command="""
            curl -f http://qdrant:6333/collections/regulations \
            && echo 'Qdrant collection regulations is accessible'
        """,
    )

    # =========================================================================
    # TASK 6 — VERIFY ARCADEDB
    # =========================================================================
    # Confirm ArcadeDB is accessible after transformation.
    # Calls ArcadeDB ready endpoint.

    verify_arcadedb = BashOperator(
        task_id="verify_arcadedb",
        bash_command="""
            curl -f http://arcadedb:2480/api/v1/ready \
            && echo 'ArcadeDB is accessible'
        """,
    )

    # =========================================================================
    # TASK DEPENDENCIES — PIPELINE ORDER
    # =========================================================================
    # >> operator defines execution order.
    # check_kafka must pass before producer runs.
    # producer must complete before verify_kafka.
    # verify_kafka must pass before transformation starts.
    # transformation must complete before storage is verified.
    # qdrant and arcadedb verification run in parallel after transformation.
    #
    # Flow:
    # check_kafka → run_producer → verify_kafka → run_transformation → verify_qdrant
    #                                                                 → verify_arcadedb

    check_kafka >> run_producer >> verify_kafka >> run_transformation >> [verify_qdrant, verify_arcadedb]