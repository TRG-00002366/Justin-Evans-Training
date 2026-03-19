from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.hooks.base import BaseHook

from kafka import KafkaConsumer
from datetime import datetime
import json
import os


def consume_events():
    # To consume we need to create a kafka consumer just like we did before
    conn = BaseHook.get_connection("kafka_game_events")

    extras = conn.extra_dejson

    consumer = KafkaConsumer(
        "game_events", #topic
        bootstrap_servers= extras.get("bootstrap.servers"),
        auto_offset_reset = extras.get("auto.offset.reset"),
        group_id = extras.get("group.id"),
        enable_auto_commit = True,
        value_deserializer = lambda v : json.loads(v.decode("utf-8"))
    )

    # Now that we have defined our consumer, we now need to consume the generated events in the queue
    msgs = consumer.poll()
    events = []
    for tp, messages in msgs.items():
        # OUR goal is to add all the pieces of data to a json file to be read by our ETL job
        for message in messages:
            events.append(message.value)

    #We need to write this to a json file to be read by our etl job
    os.makedirs("/opt/spark-data/landing")

    filename = "/opt/spark-data/landing/sample.json"

    with open(filename, "a") as f:
        json.dump(events, f)
    print(f"consumed {len(events)} events")

def validate_outputs():
    print("Imagine validate functionality here")

with DAG(
    dag_id="kafka_consumer_and_etl",
    start_date = datetime(2026,3,13),
    schedule = "*/5 * * * *", #Runs every 5 minutes
    tags = ["demo", "development", "kafka", "consumer"]
) as dag:
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    consume = PythonOperator(
        task_id="consume_events",
        python_callable=consume_events

    )

    run_spark = BashOperator(
        task_id="run_spark_job",
        bash_command = '''
            spark-submit \
                --master spark://spark-master:7077 \
                --deploy-mode client \
                --name "KafkaSparkETL" \
                /opt/spark-jobs/sample_etl_job.py \
                /opt/spark-data/landing \
                /opt/spark-data/gold \
                {{ ds }}
            '''
    )

    # {{ ds }} is the way to use the DADS execution datestring

    validate = PythonOperator(
        task_id = "validate_outputs",
        python_callable=validate_outputs
    )


    start >> consume >> run_spark >> validate >> end