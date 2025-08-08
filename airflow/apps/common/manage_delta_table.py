import os
import pyspark

from delta import *
from delta.tables import DeltaTable

builder = (
    pyspark
    .sql.SparkSession.builder
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    # MinIO
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", os.environ['MINIO_ACCESS_KEY'])
    .config("spark.hadoop.fs.s3a.secret.key", os.environ['MINIO_SECRET_KEY'])
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    # Logging
    # .config("spark.log.level", "DEBUG")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# get arguments
delta_table_path = spark.conf.get("spark.app.delta_table_path")

deltaTable = DeltaTable.forPath(spark, delta_table_path)

# optimize table
deltaTable.optimize().executeCompaction()

# remove old files
# deltaTable.vacuum(7)

spark.stop()
