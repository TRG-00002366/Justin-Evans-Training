from airflow import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator
import time
from airflow.exceptions import AirflowException

def always_passes():
    print("This task should always pass, but wi will make it wait for a few seconds")
    time.sleep(15)

def sometimes_fails():
    import random
    print("This task has a 30 percent chance to fail")

    if random.random() < .3:
        raise AirflowException


with DAG(
    dag_id = "sample_dag",
    description = "A sample dag for us to test various operators and see how things work",
    start_date = datetime(2026,3,1),
    schedule = '@daily', #"* * * * *",  This should run automatically every minute, but we can use presets like @daily for
    catchup = True,
    tags = ["sample", "development"]
) as dag:

    # Empty operator is one used mainly for organization and it's going to do no operator
    start = EmptyOperator(task_id = "start")

    pass_task = PythonOperator(
        task_id = "always_passes",
        python_callable = always_passes

    )

    fail_task = PythonOperator(
        task_id = "sometimes_fails",
        python_callable = sometimes_fails
    )

    end = BashOperator(
        task_id = "end",
        bash_command = "echo 'Pipeline has been completed'"
    )


    #provide the dependencies order
    start >> pass_task >> fail_task >> end