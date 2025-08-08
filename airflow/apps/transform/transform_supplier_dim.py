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
table_path = "hdfs://hadoop:9000/user/hadoopuser/stage/stg_supplier_dim"
dt = (
    DeltaTable
    .createIfNotExists(spark)
    .addColumn("sk_suppkey", dataType=LongType(), generatedAlwaysAs=IdentityGenerator())
    .addColumn("s_suppkey", dataType=IntegerType())
    .addColumn("s_name", dataType=CharType(25))    
    .addColumn("s_address", dataType=VarcharType(40))        
    .addColumn("s_phone", dataType=CharType(15))    
    .addColumn("s_acctbal", dataType=DoubleType())
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
    .load("hdfs://hadoop:9000/user/hadoopuser/raw/raw_supplier")
)

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

transform_df = (
    df
    .join(nation_df, df.s_nationkey == nation_df.n_nationkey, how='left')
    .join(region_df, region_df.r_regionkey == nation_df.n_regionkey, how='left')
    .select(
        df.s_suppkey,
        df.s_name,
        df.s_address,
        df.s_phone,
        df.s_acctbal,
        nation_df.n_name,
        region_df.r_name
    )
    .withColumnsRenamed({
        'n_name': 'n_nation',
        'r_name': 'r_region'
    })
    .withColumn('record_track_hash', hash(
        's_suppkey',
        's_name',
        's_address',
        's_phone',
        's_acctbal',
        'n_nation',
        'r_region'
    ))
)
transform_df.show(10)

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
        transform_df.alias("updates").select("s_suppkey", "record_track_hash"),
        "s_suppkey"
    )
    # handle null values in existings
    .filter(
        "existings.record_track_hash_notnull != updates.record_track_hash"
    )
)

data_to_update_df = (
    # data for whenMatchedUpdate and whenNotMatchedInsert
    transform_df
    .withColumn("merge_key", transform_df.s_suppkey)
    .select(
        "merge_key",
        's_suppkey',
        's_name',
        's_address',
        's_phone',
        's_acctbal',
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
            's_suppkey',
            's_name',
            's_address',
            's_phone',
            's_acctbal',
            'n_nation',
            'r_region',
            'record_track_hash'
        )
    )
)



# merge into existing
(
    dt
    .alias("target")
    .merge(
        data_to_update_df.alias("source"), 
        "target.s_suppkey = source.merge_key")
    .whenMatchedUpdate(
        condition="target.record_active_flag = true AND coalesce(target.record_track_hash, 0) != source.record_track_hash",
        set={
            "target.record_active_flag": lit(False),
            "target.record_end_date": current_date()
        }
    )
    .whenNotMatchedInsert(values={
        "target.s_suppkey": "source.s_suppkey",
        "target.s_name": "source.s_name",
        "target.s_address": "source.s_address",
        "target.s_phone": "source.s_phone",
        "target.s_acctbal": "source.s_acctbal",
        "target.n_nation": "source.n_nation",
        "target.r_region": "source.r_region",
        "target.record_active_flag": lit(True),
        "target.record_start_date": current_date(),
        "target.record_track_hash": "source.record_track_hash",
    })
    .execute()
)

spark.stop()
sc._gateway.jvm.System.exit(0)