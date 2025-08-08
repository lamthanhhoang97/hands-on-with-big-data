import os
import pyspark
import pendulum

from delta import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from decimal import Decimal
from datetime import datetime


def parse_decimal_bytes(decimal_bytes):    
    """
    Snippet Java code for conversion
    String encoded = "CO96";
    int scale = 2;
    final BigDecimal decoded = new BigDecimal(new BigInteger(Base64.getDecoder().decode(encoded)), scale);
    """
    if decimal_bytes is None:
        return None
    
    # This is a common way Connect serializes decimals:
    unscaled_value = int.from_bytes(decimal_bytes, byteorder='big', signed=True)
    scale = 2  # From your schema
    return Decimal(unscaled_value) / (10 ** scale)

def convert_datetime_to_unix_timestamp(datetime_str):
    """
    Example input: 2018-01-01T00:00:00+00:00
    """
    dt_obj = pendulum.parse(datetime_str)
    return int(dt_obj.timestamp() * 1000)


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
from_ts = spark.conf.get("spark.app.from_timestamp", None)
to_ts = spark.conf.get("spark.app.to_timestamp", None)

to_ts_ms = convert_datetime_to_unix_timestamp(to_ts)


# define table
output_path = 's3a://cdc-events/raw-supplier'
dt = (
    DeltaTable
    .createIfNotExists(spark)
    .addColumn("s_suppkey", dataType=IntegerType())
    .addColumn("s_name", dataType=CharType(25))    
    .addColumn("s_address", dataType=VarcharType(40))        
    .addColumn("s_phone", dataType=CharType(15))    
    .addColumn("s_acctbal", dataType=DoubleType())
    .addColumn("s_nationkey", dataType=CharType(25))
    # tracking columns
    .addColumn("ts_ms", dataType=LongType()) # event time
    .addColumn("create_ts", dataType=TimestampType())
    .addColumn("delete_ts_ms", dataType=LongType()) # soft-delete
    .addColumn("delete_ts", dataType=TimestampType())
    .addColumn("lsn", dataType=LongType()) # sequence
    .addColumn("batch_id", dataType=LongType()) # batch
    .property('delta.enableChangeDataFeed', 'true')
    .property('delta.autoOptimize.autoCompact', 'true')
    .location(output_path)
    .execute()
)


# read incremental data
input_path = 's3a://cdc-events/raw-supplier-cdc'

from_ts_ms = None
if not from_ts:
    last_run_df = spark.sql(f"select max(ts_ms) from delta.`{output_path}`")
    if not last_run_df.isEmpty():
        from_ts_ms = last_run_df.collect()[0][0]

change_df = (
    spark
    .read
    .format("delta")
    .load(input_path)
    .filter(col("op").isin(['c', 'r', 'u', 'd'])) # create, read, update, delete
)

if from_ts_ms:
    change_df = (
        change_df
        .filter(f"source__ts_ms >= {from_ts_ms}")
    )


change_df = (
    change_df
    .filter(f"source__ts_ms < {to_ts_ms}")

)

change_df.show()
print(f"======== Extract data from {from_ts_ms} to {to_ts_ms} ===============")


parse_decimal_bytes_udf = udf(
    parse_decimal_bytes,
    DecimalType(precision=15, scale=2)
)

# transform data
transform_df = (
    change_df
    .withColumn("s_suppkey", coalesce(col("after__s_suppkey"), col("before__s_suppkey")))
    .withColumn("s_name", coalesce(col("after__s_name"), col("before__s_name")))
    .withColumn("s_address", coalesce(col("after__s_address"), col("before__s_address")))
    .withColumn("s_nationkey", coalesce(col("after__s_nationkey"), col("before__s_nationkey")))
    .withColumn("s_phone", coalesce(col("after__s_phone"), col("before__s_phone")))
    .withColumn("s_acctbal_bytes", coalesce(col("after__s_acctbal"), col("before__s_acctbal"), ))
    .withColumn("create_ts", from_unixtime(col("source__ts_ms") / 1000).cast(TimestampType()))
    # rename
    .withColumnRenamed("source__lsn", "lsn")
    # parse Decimal field
    .withColumn("s_acctbal", parse_decimal_bytes_udf(unbase64(col("s_acctbal_bytes"))))
    .select(
        "s_suppkey", "s_name", "s_address", "s_nationkey", "s_phone", "s_acctbal",
        # tracking columns
        "op", "source__ts_ms", "lsn", "create_ts", "batch_id"
    )
    .withColumnRenamed("source__ts_ms", "ts_ms")
)
transform_df.show()

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
                'target.delete_ts_ms': 'source.ts_ms',
                'target.delete_ts': 'source.create_ts'
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

apply_changes(
    dt, 
    transform_df, 
    record_key='s_suppkey',
    sequence_by='lsn',
    except_columns=[
        "op"
    ]
)

spark.stop()
sc._gateway.jvm.System.exit(0)