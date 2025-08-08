import pyspark

from delta import *

from delta.tables import DeltaTable, IdentityGenerator
from pyspark.sql.types import *
from pyspark.sql.functions import *

builder = (
    pyspark
    .sql.SparkSession.builder
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    # Logging
    # .config("spark.log.level", "DEBUG")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
sc = spark.sparkContext # spark context

# define table
table_path = "hdfs://hadoop:9000/user/hadoopuser/stage/stg_order_dim"
dt = (
    DeltaTable
    .createIfNotExists(spark)
    .addColumn("sk_orderkey", dataType=LongType(), generatedAlwaysAs=IdentityGenerator())
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
    .location(table_path)
    .execute()
)

# read data
df = (
    spark.read
    .format("delta")
    .load("hdfs://hadoop:9000/user/hadoopuser/raw/raw_orders")
)

# transform data
transform_df = (
    df
    .withColumn("record_track_hash", hash(
        'o_orderkey',
        'o_orderstatus',
        'o_totalprice',
        'o_orderdate',
        'o_orderpriority',
        'o_clerk',
        'o_shippriority'
    ))
)

transform_df.show()
print("Transform data")

# existing data
existing_df = dt.toDF()

# rows to insert with new values (scd type 2)
data_to_insert_df = (
    existing_df
    #NOTE: handle NULL values
    .withColumn("record_track_hash_notnull", coalesce(col("record_track_hash"), lit(0)))
    .drop("record_track_hash")
    .alias("existings")
    .filter(
        "existings.record_active_flag = true"
    )
    .join(
        transform_df.alias("updates").select("o_orderkey", "record_track_hash"),
        "o_orderkey"
    )
    # handle null values in existings
    .filter(
        "existings.record_track_hash_notnull != updates.record_track_hash"
    )
)

data_to_insert_df.show()
print("Data to insert")


data_to_update_df = (
    transform_df
    .withColumn("merge_key", transform_df.o_orderkey)
    .select(
        "merge_key",
        'o_orderkey',
        'o_orderstatus',
        'o_totalprice',
        'o_orderdate',
        'o_orderpriority',
        'o_clerk',
        'o_shippriority',
        'record_track_hash'
    )
    .union(
        data_to_insert_df
        .withColumn("merge_key", lit(None))
        .select(
            "merge_key",
            'o_orderkey',
            'o_orderstatus',
            'o_totalprice',
            'o_orderdate',
            'o_orderpriority',
            'o_clerk',
            'o_shippriority',
            'record_track_hash'
        )
    )
)
data_to_update_df.show()
print("Data to update")


# merge into existing
(
    dt
    .alias("target")
    .merge(
        data_to_update_df.alias("source"), 
        "target.o_orderkey = source.merge_key")
    .whenMatchedUpdate(
        condition="target.record_active_flag = true AND coalesce(target.record_track_hash, 0) != source.record_track_hash",
        set={            
            "target.record_active_flag": lit(False),
            "target.record_end_date": current_date()
        }
    )
    .whenNotMatchedInsert(values={
        "target.o_orderkey": "source.o_orderkey",
        "target.o_orderstatus": "source.o_orderstatus",
        "target.o_totalprice": "source.o_totalprice",
        "target.o_orderdate": "source.o_orderdate",
        "target.o_orderpriority": "source.o_orderpriority",
        "target.o_clerk": "source.o_clerk",
        "target.o_shippriority": "source.o_shippriority",
        "target.record_active_flag": lit(True),
        "target.record_start_date": current_date(),
        "target.record_track_hash": "source.record_track_hash",
    })
    .execute()
)

spark.stop()
sc._gateway.jvm.System.exit(0)