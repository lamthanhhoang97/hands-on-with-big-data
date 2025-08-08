import os

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from datetime import datetime


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 26),
    'depends_on_past': False,
    'retries': None
}

with DAG(
    'odoo_cdc',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    description='Odoo Change Data Capture',
) as dag:

    # tasks
    start = DummyOperator(task_id="start", dag=dag)

    jar_packages = [
        "io.delta:delta-spark_2.12:3.3.0",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5"
    ]

    extract_raw_sale_order_cdc_op = SparkSubmitOperator(
        task_id='extract_raw_sale_order_cdc',
        application='/opt/airflow/apps/odoo/ingestion/extract_raw_sale_order_cdc.py',
        packages=",".join(jar_packages),
        name='Extract sale_order cdc',
        conn_id='spark_conn',
        deploy_mode='client',
        conf={
            'spark.executor.cores': 4,
            'spark.executor.memory': '6g',
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
            'spark.dynamicAllocation.enabled': 'false', # force num_executors
            # Streaming configs
            'spark.streaming.stopGracefullyOnShutdown': 'true',
            # Application arguments
            'spark.app.kafka_topic': 'odoo.public.sale_order',
        },
        num_executors=1,
        verbose=True,
        dag=dag
    )

    extract_raw_sale_order_line_cdc_op = SparkSubmitOperator(
        task_id='extract_raw_sale_order_line_cdc',
        application='/opt/airflow/apps/odoo/ingestion/extract_raw_sale_order_line_cdc.py',
        packages=",".join(jar_packages),
        name='Extract sale_order_line cdc',
        conn_id='spark_conn',
        deploy_mode='client',
        conf={
            'spark.executor.cores': 4,
            'spark.executor.memory': '6g',
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
            'spark.dynamicAllocation.enabled': 'false',
            # Streaming configs
            'spark.streaming.stopGracefullyOnShutdown': 'true',
            # Application arguments
            'spark.app.kafka_topic': 'odoo.public.sale_order_line',
        },
        num_executors=1,
        verbose=True,
        dag=dag
    )

    extract_raw_res_partner_cdc_op = SparkSubmitOperator(
        task_id='extract_raw_res_partner_cdc',
        application='/opt/airflow/apps/odoo/ingestion/extract_raw_res_partner_cdc.py',
        packages=",".join(jar_packages),
        name='Extract res_partner cdc',
        conn_id='spark_conn',
        deploy_mode='client',
        conf={
            'spark.executor.cores': 4,
            'spark.executor.memory': '6g',
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
            'spark.dynamicAllocation.enabled': 'false',
            # Streaming configs
            'spark.streaming.stopGracefullyOnShutdown': 'true',
            # Application arguments
            'spark.app.kafka_topic': 'odoo.public.res_partner',
        },
        num_executors=1,
        verbose=True,
        dag=dag
    )

    end = DummyOperator(task_id="end", dag=dag)

    start >> [
        extract_raw_sale_order_cdc_op,
        extract_raw_sale_order_line_cdc_op,
        extract_raw_res_partner_cdc_op
    ] >> end