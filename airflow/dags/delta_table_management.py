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
    'delta_table_management',
    default_args=default_args,
    schedule_interval=None,  # One-time run
    catchup=False,
    description='Optimize Delta tables',
) as dag:

    # tasks
    start = DummyOperator(task_id="start", dag=dag)

    minio_delta_jar_packages = [
        "io.delta:delta-spark_2.12:3.3.0",
        "org.apache.hadoop:hadoop-aws:3.3.4"
    ]

    # jobs = []
    # for delta_table_path in [
    #     'hdfs://hadoop:9000/user/hadoopuser/raw/raw_orders',
    #     'hdfs://hadoop:9000/user/hadoopuser/raw/raw_lineitem',
    # ]:
    #     table_name = delta_table_path.split('/')[-1]
    #     jobs.append(SparkSubmitOperator(
    #         task_id=f'manage_delta_table_{table_name}',
    #         application='/opt/airflow/apps/common/manage_delta_table.py',
    #         packages="io.delta:delta-spark_2.12:3.3.0",
    #         name=f'Manage files in Delta table: {table_name}',
    #         conn_id='spark_conn',
    #         conf={
    #             'spark.executor.cores': 4,
    #             'spark.executor.memory': '6g',
    #             'spark.driver.cores': 2,
    #             'spark.driver.memory': '12g',
    #             # spark web ui
    #             'spark.eventLog.enabled': 'true',
    #             'spark.eventLog.dir': 'hdfs://hadoop:9000/user/hadoopuser/spark-history',
    #             'spark.ui.enabled': 'true',
    #             'spark.ui.port': '4040',
    #             # argument for app
    #             'spark.app.delta_table_path': delta_table_path,
    #             'spark.app.dag_id': '{{dag_run.dag_id}}',
    #             'spark.app.run_id': '{{run_id}}'
    #         },
    #         verbose=True,
    #         dag=dag,
    #     ))

    optimize_table_with_zorder_op = SparkSubmitOperator(
        task_id='optimize_table_with_zorder',
        application='/opt/airflow/apps/common/optimize_detla_table.py',
        packages=",".join(minio_delta_jar_packages),
        name=f'Delta table optimizations',
        conn_id='spark_conn',
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
        },
        verbose=True,
        dag=dag,
    )

    optimize_raw_supplier_cdc = SparkSubmitOperator(
            task_id=f'compact_raw_supplier_cdc',
            application='/opt/airflow/apps/common/manage_delta_table.py',
            packages=",".join(minio_delta_jar_packages),
            name=f'Manage files in Delta table: raw_supplier_cdc',
            conn_id='spark_conn',
            conf={
                'spark.executor.cores': 4,
                'spark.executor.memory': '6g',
                # 'spark.driver.cores': 2,
                'spark.driver.memory': '6g',
                # spark web ui
                'spark.eventLog.enabled': 'true',
                'spark.eventLog.dir': 'hdfs://hadoop:9000/user/hadoopuser/spark-history',
                'spark.ui.enabled': 'true',
                'spark.ui.port': '4040',
                # argument for app
                'spark.app.delta_table_path': 's3a://cdc-events/raw-supplier-cdc/',
                'spark.app.dag_id': '{{dag_run.dag_id}}',
                'spark.app.run_id': '{{run_id}}'
            },
            verbose=True,
            dag=dag,
        )

    end = DummyOperator(task_id="end", dag=dag)

    # workflows
    start >> [
        optimize_table_with_zorder_op, 
        optimize_raw_supplier_cdc
    ] >> end
