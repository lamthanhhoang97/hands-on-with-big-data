import os
import pyspark
import pendulum

from delta import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime

def convert_datetime_to_unix_timestamp(datetime_str):
    """
    Example input: 2018-01-01T00:00:00+00:00
    """
    dt_obj = pendulum.parse(datetime_str)
    return int(dt_obj.timestamp() * 1000)

# merge into target table
def apply_changes(
    dt,
    df,
    record_key,
    sequence_by,
    except_columns=[]
):
    """
    Delete events:
    {
        "before": {"id": 10001},
        "after": null
    }
    - If INSERT and DELETE in the same batch, we loss the value for other fields when we get the latest changes
    """
    # get latest changes
    latest_df = (
        df
        .groupBy(record_key)
        .agg(
            max_by(struct("*"), sequence_by).alias("row")
        )
        .select("row.*")
    )

    # create insert value statement
    source_cols = latest_df.columns
    print(f"Source columns: {source_cols}")
    insert_value_smt = {}
    for col in list(set(source_cols) - set(except_columns)):
        insert_value_smt[f'target.{col}'] = f'source.{col}'
    
    # merge changes
    (
        dt.alias("target")
        .merge(
            latest_df.alias("source"),
            f"source.{record_key} = target.{record_key}"
        )
        .whenMatchedUpdate(
            condition="source.op = 'd'",
            set={
                'target.delete_ts_ms': 'source.event_ts_ms',
                'target.delete_ts': 'source.event_ts'
            }
        )
        .whenMatchedUpdate(
            condition=f"source.{sequence_by} > target.{sequence_by}",
            set=insert_value_smt
        )
        .whenNotMatchedInsert(
            values=insert_value_smt
        )
        .execute()
    )


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

# read application arguments
# timestamp filter: [from_ts_ms, to_ts_ms)
to_ts = pendulum.parse(spark.conf.get("spark.app.to_timestamp", None))
to_ts_ms = int(to_ts.timestamp() * 1000)

LOOKBACK_WINDOW = 3 # handle late events data
from_ts = pendulum.parse(spark.conf.get("spark.app.from_timestamp", None))
from_ts = (from_ts - from_ts.subtract(days=LOOKBACK_WINDOW)).start
from_ts_ms = int(from_ts.timestamp() * 1000)

# define table
output_path = 's3a://incremental-etl/raw_res_partner'
dt = (
    DeltaTable
    .createIfNotExists(spark)
    .addColumn("id", dataType=IntegerType())
    .addColumn("name", dataType=CharType(50))
    .addColumn("email", dataType=CharType(50))
    .addColumn("phone", dataType=CharType(50))
    .addColumn("street", dataType=CharType(50))
    .addColumn("city", dataType=CharType(50))
    .addColumn("create_date", dataType=TimestampType())
    .addColumn("write_date", dataType=TimestampType())
    # tracking columns
    .addColumn("batch_id", dataType=LongType()) # batch
    .addColumn("lsn", dataType=LongType()) # sequence
    .addColumn("event_ts_ms", dataType=LongType()) # event time
    .addColumn("event_ts", dataType=TimestampType())
    .addColumn("delete_ts_ms", dataType=LongType()) # soft-delete
    .addColumn("delete_ts", dataType=TimestampType())
    # properties
    .property('delta.enableChangeDataFeed', 'true')
    .property('delta.autoOptimize.autoCompact', 'true')
    .location(output_path)
    .execute()
)


# read incremental data
input_path = 's3a://incremental-etl/raw-res-partner-cdc/'

change_df = (
    spark
    .read
    .format("delta")
    .load(input_path)
    .filter(col("op").isin(['c', 'r', 'u', 'd'])) # create, read, update, delete
    # filter by event time
    .filter(col("`source.ts_ms`") < lit(to_ts_ms)) # < 00:00 UTC
    .filter(col("`source.ts_ms`") >= lit(from_ts_ms))
)

change_df.show()
print(f"Incremental data from {from_ts_ms} to {to_ts_ms}")

# transform data
transform_df = (
    change_df
    .withColumn("id", coalesce(col("`after.id`"), col("`before.id`"))) 
    .withColumn("name", coalesce(col("`after.name`"), col("`before.name`"))) 
    .withColumn("email", coalesce(col("`after.email`"), col("`before.email`")))
    .withColumn("phone", coalesce(col("`after.phone`"), col("`before.phone`")))
    .withColumn("street", coalesce(col("`after.street`"), col("`before.street`")))
    .withColumn("city", coalesce(col("`after.city`"), col("`before.city`")))
    # micro timestamps
    .withColumn("create_date", from_unixtime(coalesce(col("`after.create_date`"), col("`before.create_date`")) / lit(1_000_000)).cast(TimestampType()))
    .withColumn("write_date", from_unixtime(coalesce(col("`after.write_date`"), col("`before.write_date`")) / lit(1_000_000)).cast(TimestampType()))
    # tracking columns
    .withColumn("event_ts_ms", col("`source.ts_ms`"))
    .withColumn("lsn", col("`source.lsn`"))
    .select(
        "id",
        "name",
        "email",
        "phone",
        "street",
        "city",
        "create_date",
        "write_date",
        "op",
        "batch_id",
        "lsn",
        "event_ts_ms",
    )
    .withColumn("event_ts", from_unixtime(col("event_ts_ms") / lit(1_000)).cast(TimestampType()))
)
transform_df.show(truncate=False)

# merge latest changes into table
apply_changes(
    dt, 
    transform_df, 
    record_key='id',
    sequence_by='lsn',
    except_columns=[
        "op"
    ]
)

spark.stop()
sc._gateway.jvm.System.exit(0)