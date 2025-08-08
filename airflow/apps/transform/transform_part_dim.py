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
table_path = "hdfs://hadoop:9000/user/hadoopuser/stage/stg_part_dim"
dt = (
    DeltaTable
    .createIfNotExists(spark)
    .addColumn("sk_partkey", dataType=LongType(), generatedAlwaysAs=IdentityGenerator())
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
    .location(table_path)
    .execute()
)

# read data
df = (
    spark.read
    .format("delta")
    .load("hdfs://hadoop:9000/user/hadoopuser/raw/raw_part")
)
df.show(10)

# transform data
transform_df = (
    df
    .withColumn('record_track_hash', hash(
        'p_partkey',
        'p_name',
        'p_mfgr',
        'p_brand',
        'p_type',
        'p_size',
        'p_container',
        'p_retailprice'
    ))    
)
transform_df.show()

existing_df = dt.toDF()

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
        transform_df.alias("updates").select("p_partkey", "record_track_hash"),
        "p_partkey"
    )
    # handle null values in existings
    .filter(
        "existings.record_track_hash_notnull != updates.record_track_hash"
    )
)

data_to_update_df = (
    # data for whenMatchedUpdate and whenNotMatchedInsert
    transform_df
    .withColumn("merge_key", transform_df.p_partkey)
    .select(
        "merge_key",
        'p_partkey',
        'p_name',
        'p_mfgr',
        'p_brand',
        'p_type',
        'p_size',
        'p_container',
        'p_retailprice',
        'record_track_hash'
    )
    .union(
        # data for whenNotMatchedInsert
        data_to_insert_df
        .withColumn("merge_key", lit(None))
        .select(
            "merge_key",
            'p_partkey',
            'p_name',
            'p_mfgr',
            'p_brand',
            'p_type',
            'p_size',
            'p_container',
            'p_retailprice',
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
        "target.p_partkey = source.merge_key")
    .whenMatchedUpdate(
        condition="target.record_active_flag = true AND coalesce(target.record_track_hash, 0) != source.record_track_hash",
        set={
            "target.record_active_flag": lit(False),
            "target.record_end_date": current_date()
        }
    )
    .whenNotMatchedInsert(values={
        'target.p_partkey': 'source.p_partkey',
        'target.p_name': 'source.p_name',
        'target.p_mfgr': 'source.p_mfgr',
        'target.p_brand': 'source.p_brand',
        'target.p_type': 'source.p_type',
        'target.p_size': 'source.p_size',
        'target.p_container': 'source.p_container',
        'target.p_retailprice': 'source.p_retailprice',
        "target.record_active_flag": lit(True),
        "target.record_start_date": current_date(),
        "target.record_track_hash": "source.record_track_hash",
    })
    .execute()
)

spark.stop()
sc._gateway.jvm.System.exit(0)