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
# table_path = "hdfs://hadoop:9000/user/hadoopuser/analytics/dim_order"
table_path = 's3a://data-warehouse/dim_order'
dt = (
    DeltaTable
    .createIfNotExists(spark)
    .addColumn("sk_orderkey", dataType=LongType())
    .addColumn("o_orderkey", dataType=IntegerType())
    .addColumn("o_orderstatus", dataType=CharType(1))
    .addColumn("o_totalprice", dataType=DoubleType())
    .addColumn("o_orderdate", dataType=DateType())
    .addColumn("o_orderpriority", dataType=CharType(15))
    .addColumn("o_clerk", dataType=CharType(15))
    .addColumn("o_shippriority", dataType=IntegerType())
    .addColumn("record_active_flag", dataType=BooleanType())
    .addColumn("record_start_date", dataType=DateType())
    .addColumn("record_end_date", dataType=DateType())
    .addColumn("record_track_hash", dataType=IntegerType())
    .clusterBy("sk_orderkey", "o_orderkey")
    .location(table_path)
    .execute()
)

#NOTE: trigger only once
# spark.sql(f"""
#     ALTER TABLE delta.`{table_path}` 
#     SET TBLPROPERTIES (
#         'delta.minReaderVersion' = '2',
#         'delta.minWriterVersion' = '5',
#         'delta.columnMapping.mode' = 'name'
#     )
# """)

# spark.sql(f"""
#     ALTER TABLE delta.`{table_path}`
#     DROP COLUMNS (o_comment)
# """)

# spark.sql(f"""
#     ALTER TABLE delta.`{table_path}`
#     ADD COLUMNS (record_track_hash INTEGER)
# """)


# read data
df = (
    spark.read
    .format("delta")
    .load("hdfs://hadoop:9000/user/hadoopuser/stage/stg_order_dim")
)

# merge into existing
(
    dt
    .alias("target")
    .merge(
        df.alias("source"), 
        "target.sk_orderkey = source.sk_orderkey"
    )
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