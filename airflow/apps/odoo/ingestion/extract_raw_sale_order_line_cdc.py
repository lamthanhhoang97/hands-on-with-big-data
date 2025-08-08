import os
import pyspark

from delta import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, lit
from pyspark.sql.types import *


# Define the schema based on the provided JSON
variable_scale_decimal_schema = StructType([
    StructField("scale", IntegerType(), False),
    StructField("value", StringType(), False) # Debezium's DECIMAL is often encoded as bytes
])

# Define the schema for the 'before' and 'after' fields (representing the actual data)
# Schema for the 'before' and 'after' data (odoo.public.sale_order_line.Value)
value_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("order_id", IntegerType(), False),
    StructField("sequence", IntegerType(), True),
    StructField("company_id", IntegerType(), True),
    StructField("currency_id", IntegerType(), True),
    StructField("order_partner_id", IntegerType(), True),
    StructField("salesman_id", IntegerType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("product_uom", IntegerType(), True),
    StructField("product_packaging_id", IntegerType(), True),
    StructField("create_uid", IntegerType(), True),
    StructField("write_uid", IntegerType(), True),
    StructField("state", StringType(), True),
    StructField("display_type", StringType(), True),
    StructField("qty_delivered_method", StringType(), True),
    StructField("invoice_status", StringType(), True),
    StructField("analytic_distribution", StringType(), True), # Debezium JSON -> String
    StructField("name", StringType(), False),
    StructField("product_uom_qty", variable_scale_decimal_schema, False),
    StructField("price_unit", variable_scale_decimal_schema, False),
    StructField("discount", variable_scale_decimal_schema, True),
    StructField("price_subtotal", variable_scale_decimal_schema, True),
    StructField("price_total", variable_scale_decimal_schema, True),
    StructField("price_reduce_taxexcl", variable_scale_decimal_schema, True),
    StructField("price_reduce_taxinc", variable_scale_decimal_schema, True),
    StructField("qty_delivered", variable_scale_decimal_schema, True),
    StructField("qty_invoiced", variable_scale_decimal_schema, True),
    StructField("qty_to_invoice", variable_scale_decimal_schema, True),
    StructField("untaxed_amount_invoiced", variable_scale_decimal_schema, True),
    StructField("untaxed_amount_to_invoice", variable_scale_decimal_schema, True),
    StructField("is_downpayment", BooleanType(), True),
    StructField("is_expense", BooleanType(), True),
    StructField("create_date", LongType(), True), # MicroTimestamp -> Long
    StructField("write_date", LongType(), True), # MicroTimestamp -> Long
    StructField("price_tax", DoubleType(), True),
    StructField("product_packaging_qty", DoubleType(), True),
    StructField("customer_lead", DoubleType(), False),
    StructField("route_id", IntegerType(), True)
])

# Define the schema for the 'source' field
source_schema = StructType([
    StructField("version", StringType(), False),
    StructField("connector", StringType(), False),
    StructField("name", StringType(), False),
    StructField("ts_ms", LongType(), False),
    StructField("snapshot", StringType(), True), # It's an Enum, but String is safe
    StructField("db", StringType(), False),
    StructField("sequence", StringType(), True),
    StructField("ts_us", LongType(), True),
    StructField("ts_ns", LongType(), True),
    StructField("schema", StringType(), False),
    StructField("table", StringType(), False),
    StructField("txId", LongType(), True),
    StructField("lsn", LongType(), True),
    StructField("xmin", LongType(), True)
])

# Define the schema for the 'transaction' field
transaction_schema = StructType([
    StructField("id", StringType(), False),
    StructField("total_order", LongType(), False),
    StructField("data_collection_order", LongType(), False)
])

# Define schema
schema_schema = StructType([
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
])

# Define the overall payload schema
payload_schema = StructType([
    StructField("before", value_schema, True),
    StructField("after", value_schema, True),
    StructField("source", source_schema, False),
    StructField("transaction", transaction_schema, True),
    StructField("op", StringType(), False),
    StructField("ts_ms", LongType(), True),
    StructField("ts_us", LongType(), True),
    StructField("ts_ns", LongType(), True)
])

kafka_event_schema = StructType([
    StructField("schema", schema_schema, True),
    StructField("payload", payload_schema, True)
])

APP_NAME = 'raw_sale_order_line_cdc'
DELTA_TABLE_PATH = "s3a://incremental-etl/raw-sale-order-line-cdc/"
STREAMING_CHECKPOINT_PATH = F"{DELTA_TABLE_PATH}_checkpoints/"

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
        .option("txnAppId", APP_NAME)
        .option("txnVersion", batch_id)
        .save(DELTA_TABLE_PATH)
    )


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
parsed_df = (
    kafka_df
    .selectExpr("CAST(value AS STRING)")
    .select(from_json(col("value"), kafka_event_schema).alias("data"))    
    .select("data.payload.*")
)

field_names = [field.name for field in value_schema.fields]
print(f"Table field names: {field_names}")

selected_cols = (
    [
        col(f"before.{field_name}").alias(f"before.{field_name}") for field_name in field_names
    ]
    +
    [
        col(f"after.{field_name}").alias(f"after.{field_name}") for field_name in field_names
    ]
    +
    [
        col(f"source.{field_name}").alias(f"source.{field_name}") for field_name in ["ts_ms", "ts_us", "ts_ns", "txId", "lsn", "table"]
    ]
    +
    [
        "op",
        "ts_ms",
        "ts_us",
        "ts_ns"
    ]
)

parsed_df = (
    parsed_df
    .select(*selected_cols)
)

query = (
    parsed_df
    .writeStream
    .foreachBatch(write_to_multiple_destinations)
    .option("checkpointLocation", STREAMING_CHECKPOINT_PATH)
    .trigger(processingTime='15 seconds')
    .start()
)

# write streaming query to console
# query = (
#     parsed_df
#     .writeStream
#     .outputMode("append")
#     .format("console")
#     # .trigger(availableNow=True)
#     .trigger(processingTime='15 seconds')
#     .start()
# )

query.awaitTermination()

spark.stop()
sc._gateway.jvm.System.exit(0)