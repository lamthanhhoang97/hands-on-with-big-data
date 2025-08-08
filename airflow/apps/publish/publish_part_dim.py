import pyspark
import os

from delta import *

from delta.tables import DeltaTable
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
# table_path = "hdfs://hadoop:9000/user/hadoopuser/analytics/dim_part"
table_path = 's3a://data-warehouse/dim_part'
dt = (
    DeltaTable
    .createIfNotExists(spark)
    .addColumn("sk_partkey", dataType=LongType())
    .addColumn("p_partkey", dataType=IntegerType())
    .addColumn("p_name", dataType=VarcharType(55))    
    .addColumn("p_mfgr", dataType=CharType(25))    
    .addColumn("p_brand", dataType=CharType(10))    
    .addColumn("p_type", dataType=VarcharType(25))    
    .addColumn("p_size", dataType=IntegerType())    
    .addColumn("p_container", dataType=CharType(10))    
    .addColumn("p_retailprice", dataType=DoubleType())
    .addColumn("record_active_flag", dataType=BooleanType())
    .addColumn("record_start_date", dataType=DateType())
    .addColumn("record_end_date", dataType=DateType())
    .addColumn("record_track_hash", dataType=IntegerType())
    .clusterBy("sk_partkey", "p_partkey")
    .location(table_path)
    .execute()
)

# read data
df = (
    spark.read
    .format("delta")
    .load("hdfs://hadoop:9000/user/hadoopuser/stage/stg_part_dim")
)

# merge into existing
(
    dt
    .alias("target")
    .merge(
        df.alias("source"), 
        "source.sk_partkey = target.sk_partkey")
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