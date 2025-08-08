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
    'odoo_incremental_agg',
    default_args=default_args,
    catchup=False,
    description='Odoo Incremental Aggregation',
    schedule=(        
        Dataset("s3a://incremental-etl/fact_order_line") | Dataset("s3a://incremental-etl/dim_order")
    )
) as dag:

    # tasks
    start = DummyOperator(task_id="start", dag=dag)

    jar_packages = [
        "io.delta:delta-spark_2.12:3.3.0",
        "org.apache.hadoop:hadoop-aws:3.3.4",
        "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5",
        "com.amazon.deequ:deequ:2.0.9-spark-3.5"
    ]

    daily_product_sales_agg_op = SparkSubmitOperator(
        task_id='daily_product_sales_agg',
        application='/opt/airflow/apps/odoo/analytics/aggregate_daily_product_sales.py',
        packages=",".join(jar_packages),
        name='Aggregate daily product sales',
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
            # 'spark.app.to_timestamp': '{{data_interval_end}}',
        },
        num_executors=1,
        verbose=True,
        dag=dag
    )

    end = DummyOperator(task_id="end", dag=dag)
    
    start >> daily_product_sales_agg_op >> end