from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
import random
import time

COUNTRIES = ["US", "MX", "CA", "UK", "FR", "NZ", "AU"]

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



# def extract_us():
#     print("Extracting data from US")

#     # Lets add some variance to how long this will take
#     time.sleep(random.uniform(1,5))

#     # Simulate getting some records
#     record_count = random.randint(1000,5000)
#     print(f"    Extracted {record_count} records from the US")
#     #Pushing data to xcom
#     return {"country": "US", "records": record_count}

# def extract_ca():
#     print("Extracting data from US")

#     # Lets add some variance to how long this will take
#     time.sleep(random.uniform(1,5))

#     # Simulate getting some records
#     record_count = random.randint(1000,5000)
#     print(f"    Extracted {record_count} records from the Canada")
#     return {"country": "CA", "records": record_count}

# def extract_mx():
#     print("Extracting data from US")

#     # Lets add some variance to how long this will take
#     time.sleep(random.uniform(1,5))

#     # Simulate getting some records
#     record_count = random.randint(1000,5000)
#     print(f"    Extracted {record_count} records from the Mexico")
#     return {"country": "MX", "records": record_count}

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


with DAG(
    dag_id="thursday_demo_2",
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
    end = EmptyOperator(task_id="end")


    # Lets shorten these down and make this a little more programattic
    # extract_us_task = PythonOperator(
    #     task_id="extract_us",
    #     python_callable=extract_country,
    #     op_kwargs={"country": "US"}
    # )

    # extract_ca_task = PythonOperator(
    #     task_id="extract_ca",
    #     python_callable=extract_country,
    #     op_kwargs={"country": "CA"}
    # )

    # extract_mx_task = PythonOperator(
    #     task_id="extract_mx",
    #     python_callable=extract_country,
    #     op_kwargs={"country": "MX"}
    # )

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

    #Define the flow of our dag here
    # start >> extract_us_task >> extract_mx_task >> extract_ca_task >> validate_all_extracts_task >> end
    
    #Lets update the flow of the daag tp run the extraction tasks concurrently
    #Leverage parallelism!
    #In this case all extract tasks run concurrently and must all finish and pass before the bvalidate step happens
    # start >> [extract_us_task, extract_mx_task, extract_ca_task] >> validate_all_extracts_task >> end
    start >> extract_tasks >> validate_all_extracts_task >> end

dag.doc_md = '''
## Dependency Demo DAG

This demo is used to show off the basics of dag
'''