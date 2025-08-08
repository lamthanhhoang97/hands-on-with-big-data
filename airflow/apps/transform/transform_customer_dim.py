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
table_path = "hdfs://hadoop:9000/user/hadoopuser/stage/stg_customer_dim"
dt = (
    DeltaTable
    .createIfNotExists(spark)
    # .createOrReplace(spark)
    #NOTE: identity colum disables concurrent transactions on the target table
    .addColumn("sk_custkey", dataType=LongType(), generatedAlwaysAs=IdentityGenerator())
    .addColumn("c_custkey", dataType=IntegerType())
    .addColumn("c_name", dataType=VarcharType(25))
    .addColumn("c_address", dataType=VarcharType(40))
    .addColumn("c_phone", dataType=CharType(15))
    .addColumn("c_acctbal", dataType=DoubleType())
    .addColumn("c_mktsegment", dataType=CharType(10))
    .addColumn("c_comment", dataType=VarcharType(117))
    .addColumn("n_nation", dataType=CharType(25))
    .addColumn("r_region", dataType=CharType(25))
    .addColumn("record_active_flag", dataType=BooleanType(), nullable=False)
    .addColumn("record_start_date", dataType=DateType(), nullable=False)
    .addColumn("record_end_date", dataType=DateType())
    .addColumn("record_track_hash", dataType=IntegerType())
    .location(table_path)
    .execute()
)

# read data
nation_df = (
    spark.read
    .format("delta")
    .load("hdfs://hadoop:9000/user/hadoopuser/raw/raw_nation")
)
nation_df.show(10)

region_df = (
    spark.read
    .format("delta")
    .load("hdfs://hadoop:9000/user/hadoopuser/raw/raw_region")
)
region_df.show(10)

customer_df = (
    spark.read
    .format("delta")
    .load("hdfs://hadoop:9000/user/hadoopuser/raw/raw_customer")
)

# get incremental data from change data feed
# customer_df = (
#     spark.read.format("delta")
#     .option("readChangeFeed", "true")
#     .option("startingVersion", 0)
#     .option("endingVersion", 10)
#     .load("hdfs://hadoop:9000/user/hadoopuser/raw/raw_customer")
# )

# transform data
transform_df = (
    customer_df
    .join(nation_df, customer_df.c_nationkey == nation_df.n_nationkey, how='left')
    .join(region_df, region_df.r_regionkey == nation_df.n_regionkey, how='left')
    .select(
        customer_df.c_custkey,
        customer_df.c_name,
        customer_df.c_address,
        customer_df.c_phone,
        customer_df.c_acctbal,
        customer_df.c_mktsegment,
        customer_df.c_comment,
        nation_df.n_name,
        region_df.r_name
    )
    .withColumnsRenamed({
        'n_name': 'n_nation',
        'r_name': 'r_region'
    })
    .withColumn('record_track_hash', hash(
        'c_custkey',
        'c_name',
        'c_address',
        'c_phone',
        'c_acctbal',
        'c_mktsegment',
        'c_comment',
        'n_nation',
        'r_region'
    ))    
)
transform_df.show()

# load data from existing table
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
        transform_df.alias("updates").select("c_custkey", "record_track_hash"),
        "c_custkey"
    )
    # handle null values in existings
    .filter(
        "existings.record_track_hash_notnull != updates.record_track_hash"
    )
)

# rows to update
# 1. mark old rows with active = false and set end_date
# 2. new rows with new business key
data_to_update_df = (
    # data for whenMatchedUpdate and whenNotMatchedInsert
    transform_df
    .withColumn("merge_key", transform_df.c_custkey)
    .select(
        "merge_key",
        'c_custkey',
        'c_name',
        'c_address',
        'c_phone',
        'c_acctbal',
        'c_mktsegment',
        'c_comment',
        'n_nation',
        'r_region',
        'record_track_hash'
    )
    .union(
        # data for whenNotMatchedInsert
        data_to_insert_df
        .withColumn("merge_key", lit(None))
        .select(
            "merge_key",
            'c_custkey',
            'c_name',
            'c_address',
            'c_phone',
            'c_acctbal',
            'c_mktsegment',
            'c_comment',
            'n_nation',
            'r_region',
            'record_track_hash'
        )
    )
)

# merge data to table
(
    dt
    .alias("target")
    .merge(
        data_to_update_df.alias("source"), 
        "target.c_custkey = source.merge_key")
    .whenMatchedUpdate(
        condition="target.record_active_flag = true AND coalesce(target.record_track_hash, 0) != source.record_track_hash",
        set={
            "target.record_active_flag": lit(False),
            "target.record_end_date": current_date()
        }
    )
    .whenNotMatchedInsert(values={
        'target.c_custkey': 'source.c_custkey',
        'target.c_name': 'source.c_name',
        'target.c_address': 'source.c_address',
        'target.c_phone': 'source.c_phone',
        'target.c_acctbal': 'source.c_acctbal',
        'target.c_mktsegment': 'source.c_mktsegment',
        'target.c_comment': 'source.c_comment',
        'target.n_nation': 'source.n_nation',
        'target.r_region': 'source.r_region',
        "target.record_active_flag": lit(True),
        "target.record_start_date": current_date(),
        "target.record_track_hash": "source.record_track_hash",
    })
    .execute()
)

spark.stop()
sc._gateway.jvm.System.exit(0)