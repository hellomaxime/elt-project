from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'owner' : 'maxime',
    'retries' : '5',
    'retries_delay' : timedelta(minutes=2)
}

with DAG(
    dag_id = 'elt_pipeline',
    default_args = default_args,
    description = 'extract and load data to a datawarehouse for analytics',
    start_date=datetime(2022, 12, 3, 8),
    schedule_interval='@daily'
) as dag:

    extract_task = BashOperator(
        task_id = 'extract',
        bash_command = 'python3 /home/maxime/elt_project/extract.py'
    )

    load_task = BashOperator(
        task_id = 'load',
        bash_command = 'python3 /home/maxime/elt_project/load.py'
    )

extract_task >> load_task

