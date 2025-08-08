import os

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash import BashOperator


from datetime import datetime, timedelta


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'depends_on_past': False,
    'retries': None,
}

with DAG(
    'daily_run',
    default_args=default_args,
    schedule=timedelta(days=1),
    catchup=False,
    description='Sample daily DAG',
) as dag:

    # tasks
    start = DummyOperator(task_id="start", dag=dag)

    print_output = BashOperator(
        task_id="print_data",
        depends_on_past=False,
        bash_command='echo "{{ ts }}"',
    )

    end = DummyOperator(task_id="end", dag=dag)

    start >> print_output >> end