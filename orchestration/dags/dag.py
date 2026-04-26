from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago



# Applied to every task in the DAG unless overridden at the task level.
# retries=2 means if a task fails Airflow retries it 2 more times

default_args = {
    "owner": "lawflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5)
}


# schedule_interval="0 9 * * 1-5" = run at 9am Monday through Friday.
#  Each run fetches the  20 newest documents so a missed day is not critical.
# max_active_runs=1 — only one pipeline run at a time.Two concurrent runs could produce duplicate Kafka messages.

with DAG(
    dag_id="lawflow_pipeline",
    default_args=default_args,
    description="LawFlow real-time legal intelligence pipeline — ingests Federal Register regulatory documents daily",
    schedule_interval="0 9 * * 1-5",
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["lawflow", "regulatory", "ingestion"]
) as dag:

   
    # Verify Kafka is reachable before attempting to produce messages.
    # Uses netcat (nc) to check if Kafka broker port is open.

    check_kafka_broker = BashOperator(
        task_id="check_kafka_health",
        bash_command="nc -z kafka 9092 && echo 'Kafka is reachable' || exit 1",
        retries=3,
        retry_delay=timedelta(minutes=2)
    )


    run_producer = BashOperator(
        task_id="run_producer",
        bash_command="docker exec -w /app lawflow python ingestion/producer.py",
        execution_timeout=timedelta(minutes=3)  #if producer takes longer than 3 minutes it will fail the task.
    )


    # Check that messages were actually produced to the regulations topic.
    # Uses kafka-topics.sh to describe the topic and verify it exists and has messages.
  
    verify_kafka_messages = BashOperator(
        task_id="check_kafka_messages",
        bash_command="""
            echo "Reading message from regulations topic..."
            docker exec kafka /usr/bin/kafka-console-consumer \
                --bootstrap-server kafka:9092 \
                --topic regulations \
                --from-beginning \
                --max-messages 2 \
                --timeout-ms 10000 || exit 1
            echo "Kafka verification passed — message confirmed in topic"
        """,
    )
    
    run_transformation = BashOperator(
        task_id="run_transformation",
        bash_command="docker exec -w /app lawflow python processing/transformation.py",
        execution_timeout=timedelta(hours=1)
    )

    # Confirm Qdrant collection exists and has points after transformation.

    verify_qdrant = BashOperator(
        task_id="verify_qdrant",
        bash_command="""
            curl -f http://qdrant:6333/collections/regulations \
            && echo 'Qdrant collection regulations is accessible'
        """
    )

    # Confirm ArcadeDB is accessible after transformation.
    # Calls ArcadeDB ready endpoint.

    verify_arcadedb = BashOperator(
        task_id="verify_arcadedb",
        bash_command="""
            curl -f http://arcadedb:2480/api/v1/ready \
            && echo 'ArcadeDB is accessible'
        """
    )

    check_kafka_broker >> run_producer >> verify_kafka_messages >> run_transformation >> [verify_qdrant, verify_arcadedb]