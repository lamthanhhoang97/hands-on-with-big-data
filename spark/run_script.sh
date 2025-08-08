docker exec spark-master bash -c "/opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --deploy-mode client \
    --conf spark.executor.cores=4 \
    --conf spark.executor.memory=6g \
    --conf spark.driver.memory=6g \
    --conf spark.ui.enabled=true \
    --conf spark.ui.port=4040 \
    --conf spark.eventLog.enabled=true \
    --conf spark.eventLog.dir=hdfs://hadoop:9000/user/hadoopuser/spark-history \
    --conf spark.driver.log.persistToDfs.enabled=true \
    --conf spark.driver.log.dfsDir=hdfs://hadoop:9000/user/hadoopuser/spark-driver \
    --conf spark.history.fs.driverlog.cleaner.enabled=true \
    --conf spark.app.table_name=lineitem \
    --conf spark.app.dag_id=data_ingestion \
    --conf spark.app.run_id=manual__2025-04-07T09:34:17.913858+00:00 \
    --conf spark.jars.ivy=/opt/spark/.ivy2/cache \
    --packages io.delta:delta-spark_2.12:3.3.0,com.amazon.deequ:deequ:2.0.9-spark-3.5 \
    --num-executors 4 \
    --name Test \
    --verbose /app/extract_and_load_data_to_raw_layer.py"



spark-submit --master spark://spark-master:7077 \
    --conf spark.executor.cores=4 \
    --conf spark.executor.memory=6g \
    --conf spark.driver.memory=4g \
    --conf spark.ui.enabled=true \
    --conf spark.ui.port=4040 \
    --conf spark.eventLog.enabled=true \
    --conf spark.eventLog.dir=hdfs://hadoop:9000/user/hadoopuser/spark-history \
    --conf spark.driver.log.persistToDfs.enabled=true \
    --conf spark.driver.log.dfsDir=hdfs://hadoop:9000/user/hadoopuser/spark-driver \
    --conf spark.history.fs.driverlog.cleaner.enabled=true \
    --conf spark.standalone.submit.waitAppCompletion=true \
    --conf spark.jars.ivy=/home/airflow/spark/.ivy2/cache \
    --conf spark.app.table_name=lineitem \
    --packages io.delta:delta-spark_2.12:3.3.0,org.apache.hadoop:hadoop-aws:3.3.4 \
    --num-executors 4 \
    --name Test local file \
    --deploy-mode client --verbose /opt/airflow/apps/extract/extract_raw_data.py


spark-submit --class org.apache.spark.deploy.PythonRunner \
    --master spark://spark-master:7077 \
    --conf spark.executor.cores=4 \
    --conf spark.executor.memory=6g \
    --conf spark.driver.memory=4g \
    --conf spark.ui.enabled=true \
    --conf spark.ui.port=4040 \
    --conf spark.eventLog.enabled=true \
    --conf spark.eventLog.dir=hdfs://hadoop:9000/user/hadoopuser/spark-history \
    --conf spark.driver.log.persistToDfs.enabled=true \
    --conf spark.driver.log.dfsDir=hdfs://hadoop:9000/user/hadoopuser/spark-driver \
    --conf spark.history.fs.driverlog.cleaner.enabled=true \
    --conf spark.app.table_name=orders \
    --conf spark.standalone.submit.waitAppCompletion=true \
    --conf spark.jars.ivy=/home/airflow/.ivy2 \
    --packages io.delta:delta-spark_2.12:3.3.0,com.amazon.deequ:deequ:2.0.9-spark-3.5,org.apache.hadoop:hadoop-aws:3.3.4 \
    --num-executors 4 \
    --name Test --verbose --deploy-mode client /opt/airflow/apps/extract/extract_raw_data.py

