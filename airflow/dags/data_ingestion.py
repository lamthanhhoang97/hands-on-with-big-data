import os

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
from airflow.datasets import Dataset


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 26),
    'depends_on_past': False,
    'retries': None
}

with DAG(
    'data_ingestion',
    default_args=default_args,
    schedule_interval=None,  # One-time run
    catchup=False,
    description='Ingest data from landing zone to raw layer',
) as dag:

    # tasks
    start = DummyOperator(task_id="start", dag=dag)

    jar_packages = [
        'io.delta:delta-spark_2.12:3.3.0',
        'com.amazon.deequ:deequ:2.0.9-spark-3.5',
        "org.apache.hadoop:hadoop-aws:3.3.4"
    ]

    jobs = []
    for table_name in [
        'orders',
        'nation',
        'region',
        'customer',
        'lineitem',
        'part',
        'supplier',
        'partsupp'        
    ]:
        jobs.append(SparkSubmitOperator(
            task_id=f'extract_and_load_{table_name}',
            application='/opt/airflow/apps/extract/extract_and_load_data_to_raw_layer.py',
            packages=",".join(jar_packages),
            name=f'Load data from landing zone to raw layer: {table_name}',
            conn_id='spark_conn',
            deploy_mode='client',
            conf={
                'spark.executor.cores': 4,
                'spark.executor.memory': '6g',
                # 'spark.driver.cores': 4,
                'spark.driver.memory': '4g',
                # spark web ui                
                'spark.ui.enabled': 'true',
                'spark.ui.port': '4040',
                # spark event log
                'spark.eventLog.enabled': 'true',
                'spark.eventLog.dir': 'hdfs://hadoop:9000/user/hadoopuser/spark-history',
                # spark driver log
                'spark.driver.log.persistToDfs.enabled': 'true',
                'spark.driver.log.dfsDir': 'hdfs://hadoop:9000/user/hadoopuser/spark-driver',
                'spark.history.fs.driverlog.cleaner.enabled': 'true',
                # argument for app
                'spark.app.table_name': table_name,
                'spark.app.dag_id': '{{dag_run.dag_id}}',
                'spark.app.run_id': '{{run_id}}',
                # In standalone cluster mode, client waits to exit until the application completes
                'spark.standalone.submit.waitAppCompletion': 'true'
            },
            num_executors=4,
            verbose=True,
            dag=dag,
            application_args=[table_name],
            execution_timeout=timedelta(minutes=60),
            outlets=[
                Dataset(f"hdfs://hadoop:9000/user/hadoopuser/raw/raw_{table_name}")
            ]
        ))

    end = DummyOperator(task_id="end", dag=dag)

    # workflows
    start >> jobs >> end
