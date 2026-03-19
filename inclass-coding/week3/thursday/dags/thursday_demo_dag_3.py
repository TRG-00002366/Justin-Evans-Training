from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
import random
import time

COUNTRIES = ["US", "MX", "CA"]

def sample_task():
    print("Sample task")


def extract_country(country: str, **context):
    print(f"Extracting data from {country}")

    # Lets add some variance to how long this will take
    time.sleep(random.uniform(1,5))

    # Simulate getting some records
    record_count = random.randint(1000,5000)
    print(f"    Extracted {record_count} records from the {country}")
    #Pushing data to xcom
    return {"country": country, "records": record_count}


def validate_all_extracts(**context):
    #Grab the data that was returned from the tasks from xcom
    ti = context['ti']
    # What is ti? ti stands for TaskInstance and allows us to pull data from specific tasks
    # dag > DAG object
    # task  -> task definition
    # ds -> execution date string
    # run_id -> DAG run identifier


    #Lets get all of the data from the individual takes from above
    # us_results = ti.xcom_pull(task_ids="extract_us")
    # mx_results = ti.xcom_pull(task_ids="extract_mx")
    # ca_results = ti.xcom_pull(task_ids="extract_ca")


    print("Validating all records")
    total_records = 0
    for country in COUNTRIES:
        #Fetch the data returned from the task
        country_records = ti.xcom_pull(task_ids=f"extract_{country.lower()}")
        print(f"    {country}: {country_records['records']} records")
        total_records += country_records['records']

    print("Total record count: ", total_records)



    # print(f"    US: {us_results['records']}")
    # print(f"    MX: {mx_results['records']}")
    # print(f"    CA: {ca_results['records']}")

    # total_records = us_results['records'] + ca_results['records'] + mx_results['records']

    print(f"Total Records: {total_records}")
    return {"total_records": total_records, "status": "validated"}


def choose_load_strategy(**context):
    ti = context["ti"]
    total_records = ti.xcom_pull(task_ids="validate")["total_records"]

    #Now that we have our total records we need to determine which task to run

    if total_records > 10000:
        return "load_partitioned"
    else:
        return "load_simple"


def load_simple():
    print("We opted to do a simple load due to the low amount of dtaa")

def load_partitioned():
    print("We decided to do a partitioned load beacae of the large amount of data")

with DAG(
    dag_id="thursday_demo_3",
    description="Testing dependency based DAGS as well as triggers and branching",
    start_date = datetime(2026,3,12),
    schedule=None, #Manual trigger only
    catchup=False,
    tags=['development', 'demo', 'branching', 'dependencies'],
    default_args = {
        "owner": "airflow-demo",
        "retries": 1
    }
) as dag:
    

    # Recall that we can use our operators to build out our tasks

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end", trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)



    extract_tasks = []


    for country in COUNTRIES:
        extract_task = PythonOperator(
            task_id = f"extract_{country.lower()}",
            python_callable = extract_country,
            op_kwargs = {"country": country}
        )
        extract_tasks.append(extract_task)

    validate_all_extracts_task = PythonOperator(
        task_id = "validate",
        python_callable=validate_all_extracts

    )

    # We have now extracted the data, lets go about loading it.
    #We will use a branching strat to determine which loading technique to use
    # Load simple -> Less than 10k records
    #Load partitioned -> More than 10k records

    # We need a new operator, the branchpythonoperator
    # This operator is for a python function that returns a task_id and based off that we will need to run a branched task

    choose_load_strategy_task = BranchPythonOperator(
        task_id = "choose_load_strategy",
        python_callable= choose_load_strategy
    )

    load_simple_task = PythonOperator(
        task_id="load_simple",
        python_callable=load_simple
    )

    load_partitioned_task = PythonOperator(
        task_id="load_partitioned",
        python_callable=load_simple
    )
    
    #Rejoin the branches
    # This step is necessary since we are branching and one of the paths will always get skipped
    # By default Airflow will only run a task if all upstream tasks are sucessful
    # We will add a trigger rule to garuntee that atleast one success has happened and no failures have occured (skips are ok)

    join = EmptyOperator(
        task_id="join",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    # We can add a dependency flows on multiple lines for ease of view
    start >> extract_tasks >> validate_all_extracts_task


    validate_all_extracts_task >> choose_load_strategy_task >> [load_partitioned_task, load_simple_task] >> join >> end

dag.doc_md = '''
## Dependency Demo DAG

This demo is used to show off the basics of dag
'''