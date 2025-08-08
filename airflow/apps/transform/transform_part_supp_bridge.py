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
table_path = "hdfs://hadoop:9000/user/hadoopuser/stage/stg_part_supp_bridge"
dt = (
    DeltaTable
    .createIfNotExists(spark)
    .addColumn("sk_partsupp_key", dataType=LongType(), generatedAlwaysAs=IdentityGenerator())

    .addColumn("sk_partkey", dataType=IntegerType())
    .addColumn("sk_suppkey", dataType=IntegerType())

    .addColumn("p_partkey", dataType=IntegerType())
    .addColumn("s_suppkey", dataType=IntegerType())
    .addColumn("ps_availqty", dataType=IntegerType())
    .addColumn("ps_supplycost", dataType=DoubleType())

    .addColumn("record_active_flag", dataType=BooleanType())
    .addColumn("record_start_date", dataType=DateType())
    .addColumn("record_end_date", dataType=DateType())
    .addColumn("record_track_hash", dataType=IntegerType(), nullable=False)
    .location(table_path)
    .execute()
)

# read data
raw_partsupp_df = (
    spark.read
    .format("delta")
    .load("hdfs://hadoop:9000/user/hadoopuser/raw/raw_partsupp")
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

transform_df = (
    raw_partsupp_df
    .join(
        dim_part_df.where("record_active_flag = true"),
        raw_partsupp_df.ps_partkey == dim_part_df.p_partkey
    )
    .join(
        dim_supplier_df.where("record_active_flag = true"),
        raw_partsupp_df.ps_suppkey == dim_supplier_df.s_suppkey
    )
    .select(
        dim_part_df.sk_partkey,
        dim_supplier_df.sk_suppkey,
        dim_part_df.p_partkey,
        dim_supplier_df.s_suppkey,
        raw_partsupp_df.ps_availqty,
        raw_partsupp_df.ps_supplycost,
    )
    .withColumn('record_track_hash', hash(
        'sk_partkey',
        'sk_suppkey',
        'p_partkey',
        's_suppkey',
        'ps_availqty',
        'ps_supplycost'
    ))
)
transform_df.show()

# rows to insert
existing_df = dt.toDF()

rows_to_insert_df = (
    existing_df
    .alias("existings")
    .filter("existings.record_active_flag == true")
    .join(
        transform_df.alias("updates"),
        ['p_partkey', 's_suppkey']
    )
    .filter("existings.record_track_hash != updates.record_track_hash")
    .select("existings.*")
)

# rows to update
update_df = (
    transform_df
    .withColumn("merge_part_key", transform_df.p_partkey)
    .withColumn("merge_supp_key", transform_df.s_suppkey)
    .select(
        "merge_part_key",
        "merge_supp_key",
        'sk_partkey',
        'sk_suppkey',
        'p_partkey',
        's_suppkey',
        'ps_availqty',
        'ps_supplycost',
        "record_track_hash"
    )
    .union(
        rows_to_insert_df
        .withColumn("merge_part_key", lit(None))
        .withColumn("merge_supp_key", lit(None))
        .select(
            "merge_part_key",
            "merge_supp_key",
            'sk_partkey',
            'sk_suppkey',
            'p_partkey',
            's_suppkey',
            'ps_availqty',
            'ps_supplycost',
            "record_track_hash"
        )
    )
)

# upsert the table
(
    dt
    .alias("target")
    .merge(
        update_df.alias("source"),
        # handle NULL values when the table is empty
        "coalesce(target.p_partkey, 0) = source.merge_part_key AND coalesce(target.s_suppkey, 0) = source.merge_supp_key"
    )
    .whenMatchedUpdate(
        condition="target.record_active_flag = true AND target.record_track_hash != source.record_track_hash",
        set={
            "target.record_active_flag": lit(False),
            "target.record_end_date": current_date()
        }
    )
    .whenNotMatchedInsert(values={
        "target.sk_partkey": "source.sk_partkey",
        "target.sk_suppkey": "source.sk_suppkey",
        "target.p_partkey": "source.p_partkey",
        "target.s_suppkey": "source.s_suppkey",
        "target.ps_availqty": "source.ps_availqty",
        "target.ps_supplycost": "source.ps_supplycost",
        "target.record_active_flag": lit(True),
        "target.record_start_date": current_date(),
        "target.record_end_date": lit(None),
        "target.record_track_hash": "source.record_track_hash"
    })
    .execute()
)

spark.stop()
sc._gateway.jvm.System.exit(0)