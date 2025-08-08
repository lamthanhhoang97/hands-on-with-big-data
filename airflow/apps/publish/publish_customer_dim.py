import pyspark
import os

from delta import *

from delta.tables import DeltaTable, IdentityGenerator
from pyspark.sql.types import *
from pyspark.sql.functions import *


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

# define table
# table_path = "hdfs://hadoop:9000/user/hadoopuser/analytics/dim_customer"
table_path = "s3a://data-warehouse/dim_customer"
dt = (
    DeltaTable
    .createIfNotExists(spark)
    .addColumn("sk_custkey", dataType=LongType())
    .addColumn("c_custkey", dataType=IntegerType())
    .addColumn("c_name", dataType=VarcharType(25))
    .addColumn("c_address", dataType=VarcharType(40))
    .addColumn("c_phone", dataType=CharType(15))
    .addColumn("c_acctbal", dataType=DoubleType())
    .addColumn("c_mktsegment", dataType=CharType(10))
    .addColumn("c_comment", dataType=VarcharType(117))
    .addColumn("n_nation", dataType=CharType(25))
    .addColumn("r_region", dataType=CharType(25))
    .addColumn("record_active_flag", dataType=BooleanType())
    .addColumn("record_start_date", dataType=DateType())
    .addColumn("record_end_date", dataType=DateType())
    .addColumn("record_track_hash", dataType=IntegerType())
    .location(table_path)
    .execute()
)

# read data
df = (
    spark.read
    .format("delta")
    .load("hdfs://hadoop:9000/user/hadoopuser/stage/stg_customer_dim")
)

df.show()

# merge data to dedup the data by unique key
(
    dt
    .alias("target")
    .merge(
        df.alias("source"), 
        "source.sk_custkey = target.sk_custkey")
    .whenMatchedUpdate(
        condition="target.record_active_flag = true AND coalesce(target.record_track_hash, 0) != source.record_track_hash",
        set={
            "target.record_active_flag": lit(False),
            "target.record_end_date": current_date()
        }
    )
    .whenNotMatchedInsertAll()
    .execute()
)

spark.stop()
sc._gateway.jvm.System.exit(0)