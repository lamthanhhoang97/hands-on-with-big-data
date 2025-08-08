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
    'tpch_query_validation',
    default_args=default_args,
    schedule_interval=None,  # One-time run
    catchup=False,
    description='TPC-H query validation',
) as dag:

    # tasks
    start = DummyOperator(task_id="start", dag=dag)

    jar_packages = [
        'io.delta:delta-spark_2.12:3.3.0',
        "org.apache.hadoop:hadoop-aws:3.3.4"
    ]

    jobs = []
    for query_number in [1, 3, 4, 6, 12]:
        jobs.append(SparkSubmitOperator(
            task_id=f'query_{query_number}',
            application=f'/opt/airflow/apps/analytics/q{query_number}.py',
            packages=",".join(jar_packages),
            name=f'Run TPC-H query {query_number}',
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
                # 'spark.app.table_name': table_name,
                # In standalone cluster mode, client waits to exit until the application completes
                'spark.standalone.submit.waitAppCompletion': 'true'
            },
            num_executors=4,
            verbose=True,
            dag=dag,
            # execution_timeout=timedelta(minutes=60),
        ))

    end = DummyOperator(task_id="end", dag=dag)

    # workflows
    start >> jobs >> end
