'''
This is our first DAG, we'll leverage this to create a simple pipeline using some of the built in operators Airflow
We'll showcase defining a DAG, a BashOperator, PythonOperator and task dependencies
'''

# Let's start by defining a few functions that we want to run during our pipeline
from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def greet():
    print("Hello from Airflow!")
    print("We should see this message appear in our task logs")


    return "greeting_complete"

def process_data():
    # This function will simulate the processing of data

    import time
    print("Processing Data....")

    # Simulated work

    for i in range(3):
        print(f"Processing record {i + 1} of 3")
        time.sleep(5)


    print("processing completed")




# Defining a basic starts with DAG
with DAG(
    # Lets provide some properties
    dag_id = "hello_world_demo",
    description = "A simple hello world DAG demo",
    start_date = datetime(26,1,1), # Start date for the pipeline
    schedule = None, # This is used to define how often the pipeline runs, cron notation or Airflow specific inserts like @daily, manual cu
    catchup = False, # Catchup is used to determin if the pipeline should be run for each scheduled event from the start to now
    tags = ["demo", "development"], #This is used on the UI side for grouping related pipelines (prod vs dev)
    default_args = {
        "owner":"revature_dev_team",
        "retries": 1
    }
) as dag:
    #To build out our DAG we need some operators

    # Recall Operators are like classes and tasks are instances of those classes
    # BashOperator
    # PythonOperator

    # Recall that an operator basically tells airflow how to interact with the specific task


    # Task 1 is starting the pipeline
    start = BashOperator(
        task_id = "start", # Unique Identifier for the task itself
        bash_command = "echo 'Pipeline starting at $(date)'"
    )

    # Task 2 leverage the greet function to showcase a greeting
    greet_task = PythonOperator(
        task_id = "greet",
        python_callable = greet # The callable is the function I plan to use with this task
    )

    # Task 3 Leverage the process function to do some processing
    process_task = PythonOperator(
        task_id = "process",
        python_callable = process_data
    )


    # Task 4 is ending the pipeline
    end = BashOperator(
        task_id = "end",
        bash_command = "echo 'Pipeline completed at $(date)'"
    )


    # Define dependencies (what task is dependent on what task)
    # start -> greet -> process -> end
    # We use the bitwise operators to define this piece
    start >> greet_task >> process_task >> end
