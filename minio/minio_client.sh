/opt/spark/bin/spark-shell \
    --packages io.delta:delta-spark_2.12:3.3.0,org.apache.hadoop:hadoop-aws:3.4.1,org.apache.hadoop:hadoop-common:3.4.1 \
    --conf spark.hadoop.fs.s3a.access.key=xDtG8La8lBfc3fMZarcC \
    --conf spark.hadoop.fs.s3a.secret.key=g878uFBUnERPIHq4RNyup75jGmO2nzhyQ3ETlVZk
    --conf "spark.hadoop.fs.s3a.endpoint=http://minio:29000 \
    --conf "spark.databricks.delta.retentionDurationCheck.enabled=false" \
    --conf "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension" \
    --conf spark.jars.ivy=/opt/spark/.ivy2/cache \
    --conf "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"