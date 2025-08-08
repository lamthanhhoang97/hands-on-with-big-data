import os
import pyspark

from delta import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf, unbase64, from_unixtime, lit
from pyspark.sql.types import *


# Define the schema based on the provided JSON
schema = StructType([
    StructField("schema", StructType([
        StructField("type", StringType(), False),
        StructField("fields", StructType([
            StructField("type", StringType(), True),
            StructField("optional", BooleanType(), True),
            StructField("name", StringType(), True),
            StructField("version", IntegerType(), True),
            StructField("parameters", MapType(StringType(), StringType()), True),
            StructField("fields", ArrayType(StructType([
                StructField("type", StringType(), False),
                StructField("optional", BooleanType(), True),
                StructField("name", StringType(), True),
                StructField("version", IntegerType(), True),
                StructField("parameters", MapType(StringType(), StringType()), True),
                StructField("default", StringType(), True),
                StructField("fields", ArrayType(StructType([
                    StructField("type", StringType(), False),
                    StructField("optional", BooleanType(), True),
                    StructField("name", StringType(), True),
                    StructField("version", IntegerType(), True),
                    StructField("parameters", MapType(StringType(), StringType()), True),
                    StructField("default", StringType(), True),
                    StructField("field", StringType(), False)
                ])), True),
                StructField("field", StringType(), False)
            ])), True),
            StructField("field", StringType(), False)
        ]), True),
        StructField("optional", BooleanType(), False),
        StructField("name", StringType(), True),
        StructField("version", IntegerType(), True)
    ]), False),
    StructField("payload", StructType([
        StructField("before", StructType([
            StructField("s_suppkey", IntegerType(), False),
            StructField("s_name", StringType(), True),
            StructField("s_address", StringType(), True),
            StructField("s_nationkey", IntegerType(), True),
            StructField("s_phone", StringType(), True),
            StructField("s_acctbal", StringType(), True),
            StructField("s_comment", StringType(), True),
            StructField("s_dummy", StringType(), True)
        ]), True),
        StructField("after", StructType([
            StructField("s_suppkey", IntegerType(), False),
            StructField("s_name", StringType(), True),
            StructField("s_address", StringType(), True),
            StructField("s_nationkey", IntegerType(), True),
            StructField("s_phone", StringType(), True),
            StructField("s_acctbal", StringType(), True),
            StructField("s_comment", StringType(), True),
            StructField("s_dummy", StringType(), True)
        ]), True),
        StructField("source", StructType([
            StructField("version", StringType(), False),
            StructField("connector", StringType(), False),
            StructField("name", StringType(), False),
            StructField("ts_ms", LongType(), False),
            StructField("snapshot", StringType(), True),
            StructField("db", StringType(), False),
            StructField("sequence", StringType(), True),
            StructField("ts_us", LongType(), True),
            StructField("ts_ns", LongType(), True),
            StructField("schema", StringType(), False),
            StructField("table", StringType(), False),
            StructField("txId", LongType(), True),
            StructField("lsn", LongType(), True),
            StructField("xmin", LongType(), True)
        ]), False),
        StructField("transaction", StructType([
            StructField("id", StringType(), False),
            StructField("total_order", LongType(), False),
            StructField("data_collection_order", LongType(), False)
        ]), True),
        StructField("op", StringType(), False),
        StructField("ts_ms", LongType(), True),
        StructField("ts_us", LongType(), True),
        StructField("ts_ns", LongType(), True)
    ]), False)
])


builder = (
    pyspark
    .sql.SparkSession.builder
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
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
sc = spark.sparkContext

# read argument
kafka_topic = spark.conf.get("spark.app.kafka_topic")

# Read data from Kafka
kafka_properties = {
    'kafka.bootstrap.servers': 'kafka:9092',
    'subscribe': kafka_topic,
    'startingOffsets': 'earliest', # "latest" for streaming, "earliest" for batch
    'maxOffsetsPerTrigger': 5000,
}

kafka_df = (
    spark.readStream
    .format("kafka")
    .options(**kafka_properties)
    .load()
)

# Extract the value (payload) from the Kafka message
events_df = (
    kafka_df
    .selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), schema).alias("data"))    
    .select("data.payload.*")
)

# You can now work with the 'events_df' DataFrame, which contains the extracted event data.
# For example, you can select specific fields:
extracted_data = events_df.select(
    # before
    col("before.s_suppkey").alias("before__s_suppkey"),
    col("before.s_name").alias("before__s_name"),
    col("before.s_address").alias("before__s_address"),
    col("before.s_nationkey").alias("before__s_nationkey"),
    col("before.s_phone").alias("before__s_phone"),
    col("before.s_acctbal").alias("before__s_acctbal"),    
    col("before.s_comment").alias("before__s_comment"),

    # after
    col("after.s_suppkey").alias("after__s_suppkey"),
    col("after.s_name").alias("after__s_name"),
    col("after.s_address").alias("after__s_address"),
    col("after.s_nationkey").alias("after__s_nationkey"),
    col("after.s_phone").alias("after__s_phone"),
    col("after.s_acctbal").alias("after__s_acctbal"),    
    col("after.s_comment").alias("after__s_comment"),

    # source
    col("source.ts_ms").alias("source__ts_ms"),
    col("source.ts_us").alias("source__ts_us"),
    col("source.ts_ns").alias("source__ts_ns"),
    col("source.txId").alias("source__txId"),
    col("source.lsn").alias("source__lsn"),
    col("source.table").alias("source__table"),

    # other
    "op",
    "ts_ms",
    "ts_us",
    "ts_ns",
)

def write_to_multiple_destinations(
    batch_df, 
    batch_id
):
    """
    Idempotent table writes with foreachBatch (txnVersion, txnAppId)
    """

    # write to Delta table
    (
        batch_df
        .withColumn("batch_id", lit(batch_id))
        .write
        .format("delta")
        .mode("append")
        .option("txnAppId", 'raw_supplier_cdc')
        .option("txnVersion", batch_id)
        .save("s3a://cdc-events/raw-supplier-cdc/")
    )

DELTA_TABLE_PATH = "s3a://cdc-events/raw-supplier-cdc/"
STREAMING_CHECKPOINT_PATH = F"{DELTA_TABLE_PATH}_checkpoints/"
query = (
    extracted_data
    .writeStream
    .foreachBatch(write_to_multiple_destinations)
    .option("checkpointLocation", STREAMING_CHECKPOINT_PATH)
    .start()
)

# write streaming query to console
# query = (
#     extracted_data
#     .writeStream
#     .outputMode("append")
#     .format("console")
#     .start()
# )

query.awaitTermination()

spark.stop()
sc._gateway.jvm.System.exit(0)