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
    'change_data_capture',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description='Debizum CDC and Kafka',
) as dag:

    # tasks
    start = DummyOperator(task_id="start", dag=dag)

    jar_packages = [
        "io.delta:delta-spark_2.12:3.3.0",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5"
    ]

    extract_cdc_raw_supplier_op = SparkSubmitOperator(
        task_id='extract_cdc_raw_supplier',
        application='/opt/airflow/apps/extract/extract_cdc_raw_supplier.py',
        packages=",".join(jar_packages),
        name='Extract raw_supplier cdc',
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
            # Streaming configs
            'spark.streaming.stopGracefullyOnShutdown': 'true',
            # Application arguments
            'spark.app.kafka_topic': 'tpch.public.supplier',
        },
        num_executors=4,
        verbose=True,
        dag=dag
    )

    end = DummyOperator(task_id="end", dag=dag)

    start >> [
        extract_cdc_raw_supplier_op
    ] >> end