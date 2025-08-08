import pyspark
import os
import uuid

from delta import *
from delta.tables import DeltaTable, IdentityGenerator
from pyspark.sql.types import *
from pyspark.sql.functions import *


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
    # Logging
    # .config("spark.log.level", "DEBUG")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
sc = spark.sparkContext

# start session
session_id = str(uuid.uuid4())
session_id = 'f9c71ff9-9c31-4bfa-9c27-a2e2cc8a4b12'

# create a checkpoint
# spark.sparkContext.setCheckpointDir("hdfs://hadoop:9000/user/hadoopuser/spark_checkpoints")

# define table
table_path = "hdfs://hadoop:9000/user/hadoopuser/stage/stg_line_item_fact"
dt = (
    DeltaTable
    .createIfNotExists(spark)
    #NOTE: identity colum disables concurrent transactions on the target table
    .addColumn("sk_lineitemkey", dataType=LongType(), generatedAlwaysAs=IdentityGenerator())

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
    .addColumn("__session_id", dataType=CharType(36)) # uuid.uuid4()
    .location(table_path)
    .execute()
)

# read data
line_item_df = (
    spark.read
    .format("delta")
    .load("hdfs://hadoop:9000/user/hadoopuser/raw/raw_lineitem")
)

dim_part_df = (
    spark.read
    .format("delta")
    .load("s3a://data-warehouse/dim_part")
)

dim_supplier_df = (
    spark.read
    .format("delta")
    .load("s3a://data-warehouse/dim_supplier")
)

dim_order_df = (
    spark.read
    .format("delta")
    .load("s3a://data-warehouse/dim_order")
)

dim_customer_df = (
    spark.read
    .format("delta")
    .load("s3a://data-warehouse/dim_customer")
)

bridge_order_customer_df = (
    spark.read
    .format("delta")
    .load("s3a://data-warehouse/bridge_order_customer")
)

bridge_part_supp_df = (
    spark.read
    .format("delta")
    .load("s3a://data-warehouse/bridge_part_supp")
)

# merge data
transform_df = (
    line_item_df
    .join(
        dim_order_df.where("record_active_flag = true"), 
        line_item_df.l_orderkey == dim_order_df.o_orderkey
    )
    .join(
        broadcast(dim_part_df).where("record_active_flag = true").select("p_partkey", "sk_partkey"), 
        line_item_df.l_partkey == dim_part_df.p_partkey
    )
    .join(
        broadcast(dim_supplier_df).where("record_active_flag = true").select("s_suppkey", "sk_suppkey"),
        line_item_df.l_suppkey == dim_supplier_df.s_suppkey
    )
    .join(
        bridge_order_customer_df.where("record_active_flag = true"),
        line_item_df.l_orderkey == bridge_order_customer_df.o_orderkey
    )
    .join(
        broadcast(dim_customer_df).where("record_active_flag = true").select("sk_custkey"),
        bridge_order_customer_df.sk_custkey == dim_customer_df.sk_custkey
    )
    .join(
        bridge_part_supp_df.where("record_active_flag = true"),
        [
            line_item_df.l_partkey == bridge_part_supp_df.p_partkey,
            line_item_df.l_suppkey == bridge_part_supp_df.s_suppkey
        ]
    )
    .select(
        # surrogate keys
        dim_order_df.sk_orderkey,
        dim_part_df.sk_partkey,
        dim_supplier_df.sk_suppkey,
        bridge_order_customer_df.sk_custkey,

        # natural keys
        line_item_df.l_orderkey,
        line_item_df.l_linenumber,
        
        # order
        dim_order_df.o_orderdate,
        dim_order_df.o_orderpriority,
        dim_order_df.o_shippriority,
        dim_order_df.o_orderstatus,
        dim_order_df.o_totalprice,
        dim_order_df.o_clerk,

        # partsupp
        bridge_part_supp_df.ps_supplycost,
        bridge_part_supp_df.ps_availqty,

        # line item
        line_item_df.l_quantity,
        line_item_df.l_extendedprice,
        line_item_df.l_discount,
        line_item_df.l_tax,
        line_item_df.l_returnflag,
        line_item_df.l_linestatus,
        line_item_df.l_shipdate,
        line_item_df.l_commitdate,
        line_item_df.l_receiptdate,
        line_item_df.l_shipinstruct,
        line_item_df.l_shipmode
    )
    .withColumn('record_track_hash', hash(
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
    ))
    .withColumn("__session_id", lit(session_id))
)

insert_col_stmt = {}

for column in list(set(transform_df.columns) - set([
    'sk_lineitemkey'
])):
    insert_col_stmt[f"target.{column}"] = f"source.{column}"

(
    dt
    .alias("target")
    .merge(
        transform_df.alias("source"), 
        "target.l_orderkey = source.l_orderkey AND target.l_linenumber = source.l_linenumber AND target.__session_id = source.__session_id")
    .whenMatchedUpdate(set=insert_col_stmt)
    .whenNotMatchedInsert(values=insert_col_stmt)
    .execute()
)

spark.stop()
sc._gateway.jvm.System.exit(0)