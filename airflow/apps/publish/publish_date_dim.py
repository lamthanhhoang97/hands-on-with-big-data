import pyspark
import os

from delta import *

from pyspark.sql.types import *

builder = (
    pyspark
    .sql.SparkSession.builder
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
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
sc = spark.sparkContext # spark context

# read data
df = (
    spark
    .read.format("delta")
    .load("hdfs://hadoop:9000/user/hadoopuser/stage/stg_date_dim")
)

# output_path = "hdfs://hadoop:9000/user/hadoopuser/analytics/dim_date"
output_path = "s3a://data-warehouse/dim_date"
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
