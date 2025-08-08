import os

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime
from airflow.datasets import Dataset

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 26),
    'depends_on_past': False,
    'retries': None
}

with DAG(
    'data_serving',
    default_args=default_args,
    catchup=False,
    description='Load analytics layers to MinIO',
    schedule=[        
        Dataset("hdfs://hadoop:9000/user/hadoopuser/analytics/dim_order"),
        Dataset("hdfs://hadoop:9000/user/hadoopuser/analytics/dim_customer"),
        Dataset("hdfs://hadoop:9000/user/hadoopuser/analytics/dim_part"),
        Dataset("hdfs://hadoop:9000/user/hadoopuser/analytics/dim_supplier"),
        Dataset("hdfs://hadoop:9000/user/hadoopuser/analytics/fact_line_item"),
    ]
) as dag:

    # tasks
    start = DummyOperator(task_id="start", dag=dag)


    jars_packages = [        
        # spark and hadoop dependencies: https://mvnrepository.com/artifact/org.apache.spark/spark-core_2.12/3.5.5
        "io.delta:delta-spark_2.12:3.3.0",
        "org.apache.hadoop:hadoop-aws:3.3.4"
    ]

    default_spark_config = {
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
    }

    load_fact_item_line_op = SparkSubmitOperator(
        task_id='load_fact_item_line',
        application='/opt/airflow/apps/load/load_data_to_warehouse.py',
        packages=",".join(jars_packages),
        name='Load fact line item to MinIO',
        conn_id='spark_conn',
        deploy_mode='client',
        conf={
            **default_spark_config,
            **{
                # App params
                'spark.app.input_path': 'hdfs://hadoop:9000/user/hadoopuser/analytics/fact_line_item',
                'spark.app.output_path': 's3a://data-warehouse/fact_line_item',
            }
        },
        num_executors=4,
        verbose=True,
        dag=dag,
        outlets=[
            Dataset("s3a://data-warehouse/fact_line_item")
        ]
    )

    load_dim_customer_op = SparkSubmitOperator(
        task_id='load_dim_customer',
        application='/opt/airflow/apps/load/load_data_to_warehouse.py',
        packages=",".join(jars_packages),
        name='Load dim customer to MinIO',
        conn_id='spark_conn',
        deploy_mode='client',
        conf={
            **default_spark_config,
            **{
                # App params
                'spark.app.input_path': 'hdfs://hadoop:9000/user/hadoopuser/analytics/dim_customer',
                'spark.app.output_path': 's3a://data-warehouse/dim_customer'
            }
        },
        num_executors=4,
        verbose=True,
        dag=dag,
        outlets=[
            Dataset("s3a://data-warehouse/dim_customer")
        ]
    )

    load_dim_order_op = SparkSubmitOperator(
        task_id='load_dim_order',
        application='/opt/airflow/apps/load/load_data_to_warehouse.py',
        packages=",".join(jars_packages),
        name='Load dim order to MinIO',
        conn_id='spark_conn',
        deploy_mode='client',
        conf={
            **default_spark_config,
            **{
                # App params
                'spark.app.input_path': 'hdfs://hadoop:9000/user/hadoopuser/analytics/dim_order',
                'spark.app.output_path': 's3a://data-warehouse/dim_order'
            }
        },
        num_executors=4,
        verbose=True,
        dag=dag,
        outlets=[
            Dataset("s3a://data-warehouse/dim_order")
        ]
    )

    load_dim_part_op = SparkSubmitOperator(
        task_id='load_dim_part',
        application='/opt/airflow/apps/load/load_data_to_warehouse.py',
        packages=",".join(jars_packages),
        name='Load dim part to MinIO',
        conn_id='spark_conn',
        deploy_mode='client',
        conf={
            **default_spark_config,
            **{
                # App params
                'spark.app.input_path': 'hdfs://hadoop:9000/user/hadoopuser/analytics/dim_part',
                'spark.app.output_path': 's3a://data-warehouse/dim_part'
            }
        },
        num_executors=4,
        verbose=True,
        dag=dag,
        outlets=[
            Dataset("s3a://data-warehouse/dim_part")
        ]
    )

    load_dim_supplier_op = SparkSubmitOperator(
        task_id='load_dim_supplier',
        application='/opt/airflow/apps/load/load_data_to_warehouse.py',
        packages=",".join(jars_packages),
        name='Load dim supplier to MinIO',
        conn_id='spark_conn',
        deploy_mode='client',
        conf={
            **default_spark_config,
            **{
                # App params
                'spark.app.input_path': 'hdfs://hadoop:9000/user/hadoopuser/analytics/dim_supplier',
                'spark.app.output_path': 's3a://data-warehouse/dim_supplier'
            }
        },
        num_executors=4,
        verbose=True,
        dag=dag,
        outlets=[
            Dataset("s3a://data-warehouse/dim_supplier")
        ]
    )

    end = DummyOperator(task_id="end", dag=dag)

    # workflows
    start >> [
        load_fact_item_line_op,
        load_dim_customer_op,
        load_dim_order_op,
        load_dim_part_op,
        load_dim_supplier_op
    ] >> end