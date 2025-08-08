import os

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.datasets import Dataset

from datetime import datetime


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 26),
    'depends_on_past': False,
    'retries': None
}

with DAG(
    'odoo_data_modelling',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
    description='Incremental Odoo Data Modelling',
) as dag:

    # tasks
    start = DummyOperator(task_id="start", dag=dag)

    jar_packages = [
        "io.delta:delta-spark_2.12:3.3.0",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5",
        "com.amazon.deequ:deequ:2.0.9-spark-3.5"
    ]

    transform_raw_sale_order_op = SparkSubmitOperator(
        task_id='transform_raw_sale_order',
        application='/opt/airflow/apps/odoo/modelling/transform_raw_sale_order.py',
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
            # Application arguments
            'spark.app.from_timestamp': '{{data_interval_start}}',
            'spark.app.to_timestamp': '{{data_interval_end}}',
            'spark.app.logical_date': '{{ts}}',
        },
        num_executors=1,
        verbose=True,
        dag=dag
    )

    transform_dim_order_op = SparkSubmitOperator(
        task_id='transform_dim_order',
        application='/opt/airflow/apps/odoo/modelling/transform_dim_order.py',
        packages=",".join(jar_packages),
        name='Create dim_order',
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
            # Application arguments
            'spark.app.from_timestamp': '{{data_interval_start}}',
            'spark.app.to_timestamp': '{{data_interval_end}}',
            'spark.app.logical_date': '{{ts}}',
        },
        num_executors=1,
        verbose=True,
        dag=dag,
        outlets=[
            Dataset("s3a://incremental-etl/dim_order")
        ]
    )

    transform_raw_res_partner_op = SparkSubmitOperator(
        task_id='transform_raw_res_partner',
        application='/opt/airflow/apps/odoo/modelling/transform_raw_res_partner.py',
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
            # Application arguments
            'spark.app.from_timestamp': '{{data_interval_start}}',
            'spark.app.to_timestamp': '{{data_interval_end}}',
            'spark.app.logical_date': '{{ts}}',
        },
        num_executors=1,
        verbose=True,
        dag=dag
    )


    transform_dim_partner_op = SparkSubmitOperator(
        task_id='transform_dim_partner',
        application='/opt/airflow/apps/odoo/modelling/transform_dim_partner.py',
        packages=",".join(jar_packages),
        name='Create dim_partner',
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
            # Application arguments
            'spark.app.from_timestamp': '{{data_interval_start}}',
            'spark.app.to_timestamp': '{{data_interval_end}}',
            'spark.app.logical_date': '{{ts}}',
        },
        num_executors=1,
        verbose=True,
        dag=dag
    )

    transform_raw_sale_order_line_op = SparkSubmitOperator(
        task_id='transform_raw_sale_order_line',
        application='/opt/airflow/apps/odoo/modelling/transform_raw_sale_order_line.py',
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
            # Application arguments
            'spark.app.from_timestamp': '{{data_interval_start}}',
            'spark.app.to_timestamp': '{{data_interval_end}}',
            'spark.app.logical_date': '{{ts}}',
        },
        num_executors=1,
        verbose=True,
        dag=dag
    )

    transform_fact_order_line_op = SparkSubmitOperator(
        task_id='transform_fact_order_line',
        application='/opt/airflow/apps/odoo/modelling/transform_fact_order_line.py',
        packages=",".join(jar_packages),
        name='Create fact_order_line',
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
            # Application arguments
            'spark.app.from_timestamp': '{{data_interval_start}}',
            'spark.app.to_timestamp': '{{data_interval_end}}',
            'spark.app.logical_date': '{{ts}}',
        },
        num_executors=1,
        verbose=True,
        dag=dag,
        outlets=[
            Dataset("s3a://incremental-etl/fact_order_line")
        ]
    )

    end = DummyOperator(task_id="end", dag=dag)

    start >> transform_raw_sale_order_op >> transform_dim_order_op
    start >> transform_raw_res_partner_op >> transform_dim_partner_op
    start >> transform_raw_sale_order_line_op >> transform_fact_order_line_op >> end
    [
        transform_dim_order_op,
        transform_dim_partner_op
    ] >> transform_fact_order_line_op
    