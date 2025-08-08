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
sc = spark.sparkContext

# define table
table_path = "s3a://data-warehouse/fact_line_item"
dt = (
    DeltaTable
    .createIfNotExists(spark)
    .addColumn("sk_lineitemkey", dataType=LongType())

    .addColumn("sk_orderkey", dataType=IntegerType())
    .addColumn("sk_partkey", dataType=IntegerType())
    .addColumn("sk_suppkey", dataType=IntegerType())
    .addColumn("sk_custkey", dataType=IntegerType())

    .addColumn("l_orderkey", dataType=IntegerType())
    .addColumn("l_linenumber", dataType=IntegerType())

    .addColumn("o_orderdate", dataType=DateType())
    .addColumn("o_orderpriority", dataType=CharType(15))
    .addColumn("o_shippriority", dataType=IntegerType())
    .addColumn("o_orderstatus", dataType=CharType(1))
    .addColumn("o_totalprice", dataType=DoubleType())
    .addColumn("o_clerk", dataType=CharType(15))

    .addColumn("ps_supplycost", dataType=DoubleType())
    .addColumn("ps_availqty", dataType=IntegerType())

    .addColumn("l_quantity", dataType=DoubleType())
    .addColumn("l_extendedprice", dataType=DoubleType())
    .addColumn("l_discount", dataType=DoubleType())
    .addColumn("l_tax", dataType=DoubleType())
    .addColumn("l_returnflag", dataType=CharType(1))
    .addColumn("l_linestatus", dataType=CharType(1))
    .addColumn("l_shipdate", dataType=DateType())
    .addColumn("l_commitdate", dataType=DateType())
    .addColumn("l_receiptdate", dataType=DateType())
    .addColumn("l_shipinstruct", dataType=CharType(25))
    .addColumn("l_shipmode", dataType=CharType(10))
    
    .addColumn("record_active_flag", dataType=BooleanType())
    .addColumn("record_start_date", dataType=DateType())
    .addColumn("record_end_date", dataType=DateType())
    .addColumn("record_track_hash", dataType=IntegerType())
    # record_is_deleted
    .location(table_path)
    .execute()
)

# read data
transform_df = (
    spark.read
    .format("delta")
    .load("hdfs://hadoop:9000/user/hadoopuser/stage/stg_line_item_fact")
)

# # existing data
existing_df = dt.toDF()

# rows to insert
data_to_insert_df = (
    existing_df
    #NOTE: handle NULL values
    # .withColumn("record_track_hash_notnull", coalesce(col("record_track_hash"), lit(0)))
    # .drop("record_track_hash")
    .alias("existings")
    .filter(
        "existings.record_active_flag = true"
    )
    .join(
        transform_df.alias("updates").select("l_orderkey", 'l_linenumber', "record_track_hash"),
        ['l_orderkey', 'l_linenumber'] # natural keys
    )
    .filter(
        "existings.record_track_hash != updates.record_track_hash"
    )
    .select("existings.*")
)

# data_to_insert_df.show()
# print("Data to insert")

data_to_update_df = (
    transform_df
    .withColumn("merge_orderkey", transform_df.l_orderkey)
    .withColumn("merge_linenumber", transform_df.l_linenumber)
    .select(
        "merge_orderkey",
        "merge_linenumber",
        'sk_orderkey',
        'sk_partkey',
        'sk_suppkey',
        'sk_custkey',
        'l_orderkey',
        'l_linenumber',
        'o_orderdate',
        'o_orderpriority',
        'o_shippriority',
        'o_orderstatus',
        'o_totalprice',
        'o_clerk',
        'ps_supplycost',
        'ps_availqty',
        'l_quantity',
        'l_extendedprice',
        'l_discount',
        'l_tax',
        'l_returnflag',
        'l_linestatus',
        'l_shipdate',
        'l_commitdate',
        'l_receiptdate',
        'l_shipinstruct',
        'l_shipmode',
        'record_track_hash'
    )
    .union(
        data_to_insert_df
        .withColumn("merge_orderkey", lit(None))
        .withColumn("merge_linenumber", lit(None))
        .select(
            "merge_orderkey",
            "merge_linenumber",
            'sk_orderkey',
            'sk_partkey',
            'sk_suppkey',
            'sk_custkey',
            'l_orderkey',
            'l_linenumber',
            'o_orderdate',
            'o_orderpriority',
            'o_shippriority',
            'o_orderstatus',
            'o_totalprice',
            'o_clerk',
            'ps_supplycost',
            'ps_availqty',
            'l_quantity',
            'l_extendedprice',
            'l_discount',
            'l_tax',
            'l_returnflag',
            'l_linestatus',
            'l_shipdate',
            'l_commitdate',
            'l_receiptdate',
            'l_shipinstruct',
            'l_shipmode',
            'record_track_hash'
        )
    )
)

# data_to_update_df.show()
# print("Data to update")

# merge into existing
insert_col_stmt = {
    "target.record_active_flag": lit(True),
    "target.record_start_date": current_date(),
}

for column in [
    'sk_orderkey',
    'sk_partkey',
    'sk_suppkey',
    'sk_custkey',
    'l_orderkey',
    'l_linenumber',
    'o_orderdate',
    'o_orderpriority',
    'o_shippriority',
    'o_orderstatus',
    'o_totalprice',
    'o_clerk',
    'ps_supplycost',
    'ps_availqty',
    'l_quantity',
    'l_extendedprice',
    'l_discount',
    'l_tax',
    'l_returnflag',
    'l_linestatus',
    'l_shipdate',
    'l_commitdate',
    'l_receiptdate',
    'l_shipinstruct',
    'l_shipmode',
    'record_track_hash'
]:
    insert_col_stmt[f"target.{column}"] = f"source.{column}"

(
    dt
    .alias("target")
    .merge(
        data_to_update_df.alias("source"), 
        "target.l_orderkey = source.merge_orderkey AND target.l_linenumber = source.merge_linenumber")
    .whenMatchedUpdate(
        condition="target.record_active_flag = true AND target.record_track_hash != source.record_track_hash",
        set={            
            "target.record_active_flag": lit(False),
            "target.record_end_date": current_date()
        }
    )
    .whenNotMatchedInsert(values=insert_col_stmt)
    .execute()
)

spark.stop()
sc._gateway.jvm.System.exit(0)