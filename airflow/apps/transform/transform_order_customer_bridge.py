import pyspark
import os

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
    # Logging
    # .config("spark.log.level", "DEBUG")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
sc = spark.sparkContext # spark context

# define table
table_path = "hdfs://hadoop:9000/user/hadoopuser/stage/stg_order_customer_bridge"
dt = (
    DeltaTable
    .createIfNotExists(spark)
    .addColumn("sk_bridge_key", dataType=LongType(), generatedAlwaysAs=IdentityGenerator())

    .addColumn("sk_orderkey", dataType=IntegerType())
    .addColumn("sk_custkey", dataType=IntegerType())

    .addColumn("o_orderkey", dataType=IntegerType())
    .addColumn("c_custkey", dataType=IntegerType())

    .addColumn("record_active_flag", dataType=BooleanType())
    .addColumn("record_start_date", dataType=DateType())
    .addColumn("record_end_date", dataType=DateType())
    .addColumn("record_track_hash", dataType=IntegerType(), nullable=False)
    .location(table_path)
    .execute()
)

# order data
order_df = (
    spark.read
    .format("delta")
    .load("hdfs://hadoop:9000/user/hadoopuser/raw/raw_orders")
)
order_df.show()
order_df.printSchema()

# customer data
customer_df = (
    spark.read
    .format("delta")
    .load("hdfs://hadoop:9000/user/hadoopuser/raw/raw_customer")
)
customer_df.show()
customer_df.printSchema()

# order dim
dim_order_df = (
    spark.read
    .format("delta")
    .load("s3a://data-warehouse/dim_order")
)

# customer dim
dim_customer_df = (
    spark.read
    .format("delta")
    .load("s3a://data-warehouse/dim_customer")
)

transform_df = (
    order_df
    .join(
        customer_df, order_df.o_custkey == customer_df.c_custkey
    )
    .select(
        order_df.o_orderkey,
        customer_df.c_custkey
    )
    .join(
        dim_customer_df.where(dim_customer_df.record_active_flag == True).select("c_custkey", "sk_custkey"),
        "c_custkey"
    )
    .join(
        dim_order_df.where(dim_order_df.record_active_flag == True).select("o_orderkey", "sk_orderkey"),
        "o_orderkey"
    )
    .select(
        "sk_orderkey",
        "sk_custkey",
        "o_orderkey",
        "c_custkey"
    )
    .withColumn('record_track_hash', hash(
        'sk_orderkey',
        'sk_custkey',
        'o_orderkey',
        'c_custkey'
    ))
)
transform_df.show()

# rows to insert
existing_df = dt.toDF()

existing_df.show()
print("Existing data")

rows_to_insert_df = (
    existing_df
    .alias("existings")
    .filter("existings.record_active_flag == true")
    .join(
        transform_df.alias("updates"),
        ['o_orderkey', 'c_custkey']
    )
    .filter("existings.record_track_hash != updates.record_track_hash")
    .select("existings.*")
)

rows_to_insert_df.show()
print("Data to insert")

# rows to update
update_df = (
    transform_df
    .withColumn("merge_order_key", transform_df.o_orderkey)
    .withColumn("merge_customer_key", transform_df.c_custkey)
    .select(
        "merge_order_key",
        "merge_customer_key",
        "sk_orderkey",
        "sk_custkey",
        "o_orderkey",
        "c_custkey",
        "record_track_hash"
    )
    .union(
        rows_to_insert_df
        .withColumn("merge_order_key", lit(None))
        .withColumn("merge_customer_key", lit(None))
        .select(
            "merge_order_key",
            "merge_customer_key",
            "sk_orderkey",
            "sk_custkey",
            "o_orderkey",
            "c_custkey",
            "record_track_hash"
        )
    )
)

update_df.show()
print("Data to update")

dt.history()
print("Table history")

# upsert the table
(
    dt
    .alias("target")
    .merge(
        update_df.alias("source"),
        # handle NULL values when the table is empty
        "coalesce(target.o_orderkey, 0) = source.merge_order_key AND coalesce(target.c_custkey, 0) = source.merge_customer_key"
    )
    .whenMatchedUpdate(
        condition="target.record_active_flag = true AND target.record_track_hash != source.record_track_hash",
        set={
            "target.record_active_flag": lit(False),
            "target.record_end_date": current_date()
        }
    )
    .whenNotMatchedInsert(values={
        "target.sk_orderkey": "source.sk_orderkey",
        "target.sk_custkey": "source.sk_custkey",
        "target.o_orderkey": "source.o_orderkey",
        "target.c_custkey": "source.c_custkey",
        "target.record_active_flag": lit(True),
        "target.record_start_date": current_date(),
        "target.record_end_date": lit(None),
        "target.record_track_hash": "source.record_track_hash"
    })
    .execute()
)

spark.stop()
sc._gateway.jvm.System.exit(0)