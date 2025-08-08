import pyspark
import os

from delta import *

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
    # Logging
    # .config("spark.log.level", "DEBUG")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
sc = spark.sparkContext

# get arguments
input_path = spark.conf.get("spark.app.input_path")
output_path = spark.conf.get("spark.app.output_path")
partition_cols = spark.conf.get("spark.app.partition_cols")

# read data
df = (
    spark
    .read
    .format("delta")
    .load(input_path)
)

# write data to minio
if partition_cols:
    (
        df
        .write
        .format("delta")
        .mode("overwrite")
        .option("optimizeWrite", "true")
        .option("partitionOverwriteMode", "dynamic")
        .partitionBy(partition_cols)
        .save(output_path)
    )
else:
    (
        df
        .write
        .format("delta")
        .mode("overwrite")
        .option("optimizeWrite", "true")
        .save(output_path)
    )

spark.stop()
sc._gateway.jvm.System.exit(0)