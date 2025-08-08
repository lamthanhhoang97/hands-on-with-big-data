import os

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.datasets import Dataset

from datetime import datetime, timedelta


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 10, 26),
    'depends_on_past': False,
    'retries': None
}

with DAG(
    'data_modelling',
    default_args=default_args,
    catchup=False,
    description='Data Modelling for TPC-H dataset',
    schedule=[
        Dataset("hdfs://hadoop:9000/user/hadoopuser/raw/raw_orders"),
        Dataset("hdfs://hadoop:9000/user/hadoopuser/raw/raw_customer"),        
        Dataset("hdfs://hadoop:9000/user/hadoopuser/raw/raw_lineitem"),
        Dataset("hdfs://hadoop:9000/user/hadoopuser/raw/raw_supplier"),        
        Dataset("hdfs://hadoop:9000/user/hadoopuser/raw/raw_part"),
    ]
) as dag:

    # tasks
    start = DummyOperator(task_id="start", dag=dag)

    minio_delta_jar_packages = [
        "io.delta:delta-spark_2.12:3.3.0",
        "org.apache.hadoop:hadoop-aws:3.3.4"
    ]

    audit_raw_nation_op = SparkSubmitOperator(
        task_id='audit_raw_nation',
        application='/opt/airflow/apps/audit/audit_raw_nation.py',
        packages="io.delta:delta-spark_2.12:3.3.0,com.amazon.deequ:deequ:2.0.9-spark-3.5",
        name='audit_raw_nation',
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
            # argument for app
            'spark.app.dag_id': '{{dag_run.dag_id}}',
            'spark.app.run_id': '{{run_id}}',
        },
        num_executors=4,
        verbose=True,
        dag=dag,
        execution_timeout=timedelta(minutes=15),
    )

    audit_raw_region_op = SparkSubmitOperator(
        task_id='audit_raw_region',
        application='/opt/airflow/apps/audit/audit_raw_region.py',
        packages="io.delta:delta-spark_2.12:3.3.0,com.amazon.deequ:deequ:2.0.9-spark-3.5",
        name='audit_raw_region',
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
            # argument for app
            'spark.app.dag_id': '{{dag_run.dag_id}}',
            'spark.app.run_id': '{{run_id}}',
        },
        num_executors=4,
        verbose=True,
        dag=dag,
        execution_timeout=timedelta(minutes=15),
    )

    transform_order_dim_op = SparkSubmitOperator(
        task_id='transform_order_dim',
        application='/opt/airflow/apps/transform/transform_order_dim.py',
        packages="io.delta:delta-spark_2.12:3.3.0",
        name='Create order dim',
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
            'spark.standalone.submit.waitAppCompletion': 'true'
        },
        num_executors=4,
        verbose=True,
        dag=dag,
        execution_timeout=timedelta(minutes=15),
    )

    audit_order_dim_op = SparkSubmitOperator(
        task_id='audit_order_dim',
        application='/opt/airflow/apps/audit/audit_order_dim.py',
        packages="io.delta:delta-spark_2.12:3.3.0,com.amazon.deequ:deequ:2.0.9-spark-3.5",
        name='Audit order dim',
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
            # argument for app
            'spark.app.dag_id': '{{dag_run.dag_id}}',
            'spark.app.run_id': '{{run_id}}',
        },
        num_executors=4,
        verbose=True,
        dag=dag,
        execution_timeout=timedelta(minutes=15),
    )


    publish_order_dim_op = SparkSubmitOperator(
        task_id='publish_order_dim',
        application='/opt/airflow/apps/publish/publish_order_dim.py',
        packages=",".join(minio_delta_jar_packages),
        name='Publish order dim',
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
        },
        num_executors=4,
        verbose=True,
        dag=dag,
        execution_timeout=timedelta(minutes=15),
        outlets=[
            Dataset("s3a://data-warehouse/dim_order")
        ]
    )

    
    transform_customer_dim_op = SparkSubmitOperator(
        task_id='transform_customer_dim',
        application='/opt/airflow/apps/transform/transform_customer_dim.py',
        packages="io.delta:delta-spark_2.12:3.3.0",
        name='Create customer dim',
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
            'spark.standalone.submit.waitAppCompletion': 'true'
        },
        num_executors=4,
        verbose=True,
        dag=dag,
        execution_timeout=timedelta(minutes=15)
    )

    audit_customer_dim_op = SparkSubmitOperator(
        task_id='audit_customer_dim',
        application='/opt/airflow/apps/audit/audit_customer_dim.py',
        packages="io.delta:delta-spark_2.12:3.3.0,com.amazon.deequ:deequ:2.0.9-spark-3.5",
        name='Audit customer dim',
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
            # argument for app
            'spark.app.dag_id': '{{dag_run.dag_id}}',
            'spark.app.run_id': '{{run_id}}',
        },
        num_executors=4,
        verbose=True,
        dag=dag,
        execution_timeout=timedelta(minutes=15),
    )

    publish_customer_dim_op = SparkSubmitOperator(
        task_id='publish_customer_dim',
        application='/opt/airflow/apps/publish/publish_customer_dim.py',
        packages=",".join(minio_delta_jar_packages),
        name='Publish customer dim',
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
        },
        num_executors=4,
        verbose=True,
        dag=dag,
        execution_timeout=timedelta(minutes=15),
        outlets=[
            Dataset("s3a://data-warehouse/dim_customer")
        ]
    )


    transform_part_dim_op = SparkSubmitOperator(
        task_id='transform_part_dim',
        application='/opt/airflow/apps/transform/transform_part_dim.py',
        packages="io.delta:delta-spark_2.12:3.3.0",
        name='Create part dim',
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
            'spark.standalone.submit.waitAppCompletion': 'true'
        },
        num_executors=4,
        verbose=True,
        dag=dag,
        execution_timeout=timedelta(minutes=15)
    )

    publish_part_dim_op = SparkSubmitOperator(
        task_id='publish_part_dim',
        application='/opt/airflow/apps/publish/publish_part_dim.py',
        packages=",".join(minio_delta_jar_packages),
        name='Publish part dim',
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
            'spark.standalone.submit.waitAppCompletion': 'true'
        },
        num_executors=4,
        verbose=True,
        dag=dag,
        execution_timeout=timedelta(minutes=15),
        outlets=[
            Dataset("s3a://data-warehouse/dim_part")
        ]
    )

    
    transform_supplier_dim_op = SparkSubmitOperator(
        task_id='transform_supplier_dim',
        application='/opt/airflow/apps/transform/transform_supplier_dim.py',
        packages="io.delta:delta-spark_2.12:3.3.0",
        name='Create supplier dim',
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
            'spark.standalone.submit.waitAppCompletion': 'true'
        },
        num_executors=4,
        verbose=True,
        dag=dag,
        execution_timeout=timedelta(minutes=15)
    )


    publish_supplier_dim_op = SparkSubmitOperator(
        task_id='publish_supplier_dim',
        application='/opt/airflow/apps/publish/publish_supplier_dim.py',
        packages=",".join(minio_delta_jar_packages),
        name='Publish supplier dim',
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
            'spark.standalone.submit.waitAppCompletion': 'true'
        },
        num_executors=4,
        verbose=True,
        dag=dag,
        execution_timeout=timedelta(minutes=15),
        outlets=[
            Dataset("s3a://data-warehouse/dim_supplier")
        ]
    )

    transform_date_dim_op = SparkSubmitOperator(
        task_id='transform_date_dim',
        application='/opt/airflow/apps/transform/transform_date_dim.py',
        packages="io.delta:delta-spark_2.12:3.3.0",
        name='Create date dim',
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
            'spark.standalone.submit.waitAppCompletion': 'true'
        },
        num_executors=4,
        verbose=True,
        dag=dag,
        execution_timeout=timedelta(minutes=15)
    )



    publish_date_dim_op = SparkSubmitOperator(
        task_id='publish_date_dim',
        application='/opt/airflow/apps/publish/publish_date_dim.py',
        packages=",".join(minio_delta_jar_packages),
        name='Publish date dim',
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
            'spark.standalone.submit.waitAppCompletion': 'true'
        },
        num_executors=4,
        verbose=True,
        dag=dag,
        execution_timeout=timedelta(minutes=15),
        outlets=[
            Dataset("s3a://data-warehouse/dim_date")
        ]
    )

    transform_line_item_fact_op = SparkSubmitOperator(
        task_id='transform_line_item_fact',
        application='/opt/airflow/apps/transform/transform_line_item_fact.py',
        packages=",".join(minio_delta_jar_packages),
        name='Create line item fact',
        conn_id='spark_conn',
        deploy_mode='client',
        conf={
            # Tuning Resource Allocation
            'spark.executor.cores': 4,
            'spark.executor.memory': '6g',
            # 'spark.driver.cores': 4,
            'spark.driver.memory': '6g',
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
            'spark.standalone.submit.waitAppCompletion': 'true'
        },
        num_executors=4,
        verbose=True,
        dag=dag
    )

    audit_line_item_fact_op = SparkSubmitOperator(
        task_id='audit_line_item_fact',
        application='/opt/airflow/apps/audit/audit_line_item_fact.py',
        packages="io.delta:delta-spark_2.12:3.3.0,com.amazon.deequ:deequ:2.0.9-spark-3.5",
        name='Audit line item fact',
        conn_id='spark_conn',
        deploy_mode='client',
        conf={
            'spark.executor.cores': 4,
            'spark.executor.memory': '6g',
            # 'spark.driver.cores': 4,
            'spark.driver.memory': '6g',
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
            # argument for app
            'spark.app.dag_id': '{{dag_run.dag_id}}',
            'spark.app.run_id': '{{run_id}}',
        },
        num_executors=4,
        verbose=True,
        dag=dag,
    )

    publish_line_item_fact_op = SparkSubmitOperator(
        task_id='publish_line_item_fact',
        application='/opt/airflow/apps/publish/publish_line_item_fact.py',
        packages=",".join(minio_delta_jar_packages),
        name='Publish line item fact',
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
            'spark.standalone.submit.waitAppCompletion': 'true'
        },
        num_executors=4,
        verbose=True,
        dag=dag,
        outlets=[
            Dataset("s3a://data-warehouse/fact_line_item")
        ]
    )

    transform_order_customer_bridge_op = SparkSubmitOperator(
        task_id='transform_order_customer_bridge',
        application='/opt/airflow/apps/transform/transform_order_customer_bridge.py',
        packages=",".join(minio_delta_jar_packages),
        name='Create order customer bridge',
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
            'spark.standalone.submit.waitAppCompletion': 'true'
        },
        num_executors=4,
        verbose=True,
        dag=dag
    )

    publish_order_customer_bridge_op = SparkSubmitOperator(
        task_id='publish_order_customer_bridge',
        application='/opt/airflow/apps/publish/publish_order_customer_bridge.py',
        packages=",".join(minio_delta_jar_packages),
        name='Publish order customer bridge',
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
            'spark.standalone.submit.waitAppCompletion': 'true'
        },
        num_executors=4,
        verbose=True,
        dag=dag,
        outlets=[
            Dataset("s3a://data-warehouse/bridge_order_customer")
        ]
    )

    transform_part_supp_bridge_op = SparkSubmitOperator(
        task_id='transform_part_supp_bridge',
        application='/opt/airflow/apps/transform/transform_part_supp_bridge.py',
        packages=",".join(minio_delta_jar_packages),
        name='Create part supplier bridge',
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
            'spark.standalone.submit.waitAppCompletion': 'true'
        },
        num_executors=4,
        verbose=True,
        dag=dag
    )

    publish_part_supp_bridge_op = SparkSubmitOperator(
        task_id='publish_part_supp_bridge',
        application='/opt/airflow/apps/publish/publish_part_supp_bridge.py',
        packages=",".join(minio_delta_jar_packages),
        name='Publish part supp bridge',
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
            'spark.standalone.submit.waitAppCompletion': 'true'
        },
        num_executors=4,
        verbose=True,
        dag=dag,
        outlets=[
            Dataset("s3a://data-warehouse/bridge_part_supp")
        ]
    )

    

    end = DummyOperator(task_id="end", dag=dag)

    start >> [
        audit_raw_nation_op,
        audit_raw_region_op,
        transform_order_dim_op,
        transform_date_dim_op,
        transform_part_dim_op
    ]

    [
        audit_raw_nation_op,
        audit_raw_region_op
    ] >> transform_customer_dim_op >> audit_customer_dim_op >> publish_customer_dim_op >> transform_order_customer_bridge_op
    transform_order_dim_op >> audit_order_dim_op >> publish_order_dim_op >> transform_order_customer_bridge_op

    transform_date_dim_op >> publish_date_dim_op

    transform_part_dim_op >> publish_part_dim_op
    [
        audit_raw_nation_op,
        audit_raw_region_op
    ] >> transform_supplier_dim_op >> publish_supplier_dim_op

    transform_order_customer_bridge_op >> publish_order_customer_bridge_op
    [
        publish_part_dim_op,
        publish_supplier_dim_op
    ] >> transform_part_supp_bridge_op >> publish_part_supp_bridge_op
    
    [
        publish_order_customer_bridge_op,
        publish_order_dim_op,
        publish_date_dim_op,
        publish_part_supp_bridge_op,
        publish_part_dim_op,
        publish_supplier_dim_op,
        publish_customer_dim_op
    ] >> transform_line_item_fact_op >> audit_line_item_fact_op >> publish_line_item_fact_op >> end