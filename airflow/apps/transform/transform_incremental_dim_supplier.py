import os
import pyspark
import pendulum

from delta import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import DeltaTable, IdentityGenerator
from datetime import datetime


DELTA_DATETIME_FOPMAT = "YYYY-MM-DD HH:mm:ss"
DEFAULT_DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"

def convert_datetime_to_unix_timestamp(datetime_str):
    """
    Example input: 2018-01-01T00:00:00+00:00
    """
    dt_obj = pendulum.parse(datetime_str)
    return int(dt_obj.timestamp() * 1000)

def build_incremental_parameters(
    spark,
    delta_input_path: str,
    delta_output_path: str,
    logical_date: datetime      
):
    """
    Change data feed feature in Delta Lake: <https://docs.delta.io/latest/delta-change-data-feed.html>
    If startingTimestamp == endingTimestamp, it returns empty result
    """
    result = {}

    # input table
    query_result = spark.sql(
        f"select min(_commit_timestamp), max(_commit_timestamp) from table_changes_by_path('{delta_input_path}', 0)"
    ).collect()
    print(f"Query result: {query_result}")

    first_commit_ts, last_commit_ts = query_result[0][0], query_result[0][1] # datetime.datetime

    # current table
    prev_commit_ts = spark.sql(
        f"select max(record_commit_timestamp) from delta.`{delta_output_path}`"
    ).collect()[0][0]

    starting_ts = first_commit_ts # default value
    if prev_commit_ts and prev_commit_ts > first_commit_ts:
        starting_ts = prev_commit_ts

    ending_ts = last_commit_ts
    if logical_date < last_commit_ts and logical_date > first_commit_ts:
        ending_ts = logical_date

    result.update({
        'startingTimestamp': datetime.strftime(starting_ts, DEFAULT_DATETIME_FORMAT),
        'endingTimestamp': datetime.strftime(ending_ts, DEFAULT_DATETIME_FORMAT)
    })
    return result


builder = (
    pyspark
    .sql.SparkSession.builder
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    # MinIO
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", os.environ['MINIO_ACCESS_KEY'])
    .config("spark.hadoop.fs.s3a.secret.key", os.environ['MINIO_SECRET_KEY'])
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    # Logging
    # .config("spark.log.level", "DEBUG")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
sc = spark.sparkContext

# define table
DELTA_TABLE_PATH = 's3a://cdc-events/dim-supplier-incremental'
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
    .addColumn("record_start_date", dataType=TimestampType())
    .addColumn("record_end_date", dataType=TimestampType())
    .addColumn("record_track_hash", dataType=IntegerType())
    # last commit timestamp from change data feed (CDF)
    .addColumn("record_commit_timestamp", dataType=TimestampType())
    .property('delta.enableChangeDataFeed', 'true')
    .location(DELTA_TABLE_PATH)
    .execute()
)

# read application arguments
from_ts = spark.conf.get("spark.app.from_timestamp", None)

# convert from pendulum.datetime to datetime.datetime
to_ts = datetime.strptime(
    pendulum.parse(spark.conf.get("spark.app.to_timestamp", None)).format(DELTA_DATETIME_FOPMAT),
    DEFAULT_DATETIME_FORMAT
)


nation_df = (
    spark.read
    .format("delta")
    .load("hdfs://hadoop:9000/user/hadoopuser/raw/raw_nation")
)

region_df = (
    spark.read
    .format("delta")
    .load("hdfs://hadoop:9000/user/hadoopuser/raw/raw_region")
)

# query change data
read_options = {
    'readChangeFeed': 'true',
}

incremental_params = build_incremental_parameters(
    spark,
    delta_input_path='s3a://cdc-events/raw-supplier',
    delta_output_path=DELTA_TABLE_PATH,
    logical_date=to_ts
)
read_options.update(incremental_params)

print(f"Incremental query parameters: {read_options}")   

change_df = (
    spark
    .read.format("delta")
    .options(**read_options)
    .load('s3a://cdc-events/raw-supplier')
)

# _change_type | _commit_version | _commit_timestamp
change_df = (
    change_df
    .filter(col("_change_type").isin(["insert", "update_postimage", "delete"]))
)

# handle duplicated records
change_df = (
    change_df
    .groupBy("s_suppkey")
    .agg(
        max_by(struct("*"), "_commit_timestamp").alias("row")
    )
    .select("row.*")
)

change_df.show()


transform_df = (
    change_df
    .join(nation_df, change_df.s_nationkey == nation_df.n_nationkey, how='left')
    .join(region_df, region_df.r_regionkey == nation_df.n_regionkey, how='left')
    .select(
        change_df.s_suppkey,
        change_df.s_name,
        change_df.s_address,
        change_df.s_phone,
        change_df.s_acctbal,
        nation_df.n_name,
        region_df.r_name,
        change_df._commit_timestamp,
        change_df.create_ts
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

transform_df.show()

# merge data to target table
existing_df = dt.toDF()

data_to_insert_df = (
    existing_df
    .filter(
        "record_active_flag = true"
    )
    .alias("existing")
    .join(
        transform_df.alias("update"),
        [
            existing_df.s_suppkey == transform_df.s_suppkey,
            existing_df.record_track_hash != transform_df.record_track_hash
        ],
        how='inner'
    )
    .selectExpr("existing.*", "update.create_ts", "update._commit_timestamp")
)

selected_cols = transform_df.columns
print(f"Selected columns: {selected_cols}")

data_to_update_df = (
    transform_df
    .withColumn("mergeKey", transform_df.s_suppkey)
    .select(["mergeKey"] + selected_cols)
    .union(
        data_to_insert_df
        .withColumn("mergeKey", lit(None))
        .select(["mergeKey"] + selected_cols)
    )
)

# default values
insert_values_stmt = {
    'target.record_active_flag': lit(True),
    'target.record_start_date': 'source.create_ts',
    'target.record_end_date': lit(None),
    "target.record_track_hash": "source.record_track_hash",
    "target.record_commit_timestamp": "source._commit_timestamp",
    'target.s_suppkey': 'source.s_suppkey',
    'target.s_name': 'source.s_name',
    'target.s_address': 'source.s_address',
    'target.s_phone': 'source.s_phone',
    'target.s_acctbal': 'source.s_acctbal',
    'target.n_nation': 'source.n_nation',
    'target.r_region': 'source.r_region'
}

(
    dt.alias("target")
    .merge(
        data_to_update_df.alias("source"),
        "target.s_suppkey = source.mergeKey"
    )
    .whenMatchedUpdate(
        condition="target.record_active_flag = true and target.record_track_hash != source.record_track_hash",
        set={
            "target.record_active_flag": lit(False),
            "target.record_end_date": "source.create_ts",
            "target.record_commit_timestamp": "source._commit_timestamp"
        }
    )
    .whenNotMatchedInsert(
        values=insert_values_stmt
    )
    .execute()
)


spark.stop()
sc._gateway.jvm.System.exit(0)