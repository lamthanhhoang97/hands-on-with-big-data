import pyspark
import os
import pydeequ

from delta import *
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pydeequ.profiles import *
from pydeequ.checks import *
from pydeequ.verification import *

packages = [
    "io.delta:delta-spark_2.12:3.3.0",
    "org.apache.hadoop:hadoop-aws:3.4.1",
    "org.apache.hadoop:hadoop-common:3.4.1",
    "com.amazonaws:aws-java-sdk:1.12.262"
]

builder = (
    pyspark
    .sql.SparkSession.builder
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    # .config("spark.jars.packages", ",".join(packages))
    # MinIO
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", os.environ['MINIO_ACCESS_KEY']) \
    .config("spark.hadoop.fs.s3a.secret.key", os.environ['MINIO_SECRET_KEY']) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    # Tuning performance
    .config("spark.sql.join.preferSortMergeJoin", "false")
    .config("spark.sql.cbo.enabled", "true")
    .config("spark.sql.cbo.joinReorder.enabled", "true")
    .config("spark.sql.statistics.histogram.enabled", "true")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    # IO Compression
    .config("spark.io.compression.codec", "snappy")
    # Deequ
    .config("spark.jars.packages", pydeequ.deequ_maven_coord)
    .config("spark.jars.excludes", pydeequ.f2j_maven_coord)
    # Logging
    # .config("spark.log.level", "DEBUG")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
sc = spark.sparkContext

dag_id = spark.conf.get("spark.app.dag_id")
run_id = spark.conf.get("spark.app.run_id")

# define table
table_path = "hdfs://hadoop:9000/user/hadoopuser/stage/stg_line_item_fact"
transform_df = (
    spark.read
    .format("delta")
    .load(table_path)
)

# data quality checks
session_id = 'f9c71ff9-9c31-4bfa-9c27-a2e2cc8a4b12'
transform_df = (
    transform_df
    .where(f"__session_id = '{session_id}'")
)

table_name = "stg_line_item_fact"
check = Check(spark, CheckLevel.Error, f"Quality Check for table: {table_name}")
checkResult = (
    VerificationSuite(spark)
    .onData(transform_df)
    .addCheck(
        check
        .isComplete("sk_orderkey")            
        .isComplete("sk_partkey")
        .isComplete("sk_suppkey")
        .isComplete("sk_custkey")
        .hasUniqueness(["l_orderkey", "l_linenumber"], lambda x: x >= 0.8)
    )
    .run()
)

checkResult_df = (
    VerificationResult.checkResultsAsDataFrame(spark, checkResult)
    .withColumn("dag_id", lit(dag_id))
    .withColumn("run_id", lit(run_id))
    .withColumn("check_date", current_timestamp())
)
checkResult_df.show(truncate=False)

(
    checkResult_df
    .write
    .format("delta")
    .mode("append")
    .option("mergeSchema", "true")
    .save(f"hdfs://hadoop:9000/user/hadoopuser/audit/{table_name}")
)

failed_checks_df = (
    checkResult_df
    .filter(col("constraint_status") == "Failure")
)

if failed_checks_df.count() > 0:
    raise Exception("Data quality checks failed!") 
else:
    print("Data quality checks passed!")

spark.stop()
sc._gateway.jvm.System.exit(0)