import os

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from datetime import datetime


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 26),
    'depends_on_past': False,
    'retries': None
}

with DAG(
    'incremental_dimenisonal_modelling',
    default_args=default_args,
    schedule_interval=None, # one-time trigger
    catchup=False,
    description='Build incremental models',
) as dag:

    # tasks
    start = DummyOperator(task_id="start", dag=dag)

    jar_packages = [
        "io.delta:delta-spark_2.12:3.3.0",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5"
    ]

    transform_raw_supplier_cdc_op = SparkSubmitOperator(
        task_id='transform_raw_supplier_cdc',
        application='/opt/airflow/apps/transform/transform_cdc_raw_supplier.py',
        packages=",".join(jar_packages),
        name='Transform raw supplier cdc',
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
            # In standalone cluster mode, client waits to exit until the application completes
            'spark.standalone.submit.waitAppCompletion': 'true',
            # Application arguments
            # 'spark.app.from_timestamp': None,
            'spark.app.to_timestamp': '{{ ts }}',
        },
        num_executors=4,
        verbose=True,
        dag=dag
    )

    transform_incremental_dim_supplier_op = SparkSubmitOperator(
        task_id='transform_incremental_dim_supplier',
        application='/opt/airflow/apps/transform/transform_incremental_dim_supplier.py',
        packages=",".join(jar_packages),
        name='Transform incremental dim supplier',
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
            # In standalone cluster mode, client waits to exit until the application completes
            'spark.standalone.submit.waitAppCompletion': 'true',
            # Application arguments
            # 'spark.app.from_timestamp': None,
            'spark.app.to_timestamp': '{{ ts }}',
            'spark.app.logical_date': '{{ ts }}'
        },
        num_executors=4,
        verbose=True,
        dag=dag
    )

    end = DummyOperator(task_id="end", dag=dag)

    start >> transform_raw_supplier_cdc_op >> transform_incremental_dim_supplier_op >> end