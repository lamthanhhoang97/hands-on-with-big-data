import os
import pyspark
import pendulum
import pydeequ

# Python
from datetime import datetime
# Delta 
from delta import *
from delta.tables import DeltaTable, IdentityGenerator
# PySpark
from pyspark.sql.functions import *
from pyspark.sql.types import *
# Deequ
from pydeequ.profiles import *
from pydeequ.checks import *
from pydeequ.verification import *


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
    # Deequ
    .config("spark.jars.packages", pydeequ.deequ_maven_coord)
    .config("spark.jars.excludes", pydeequ.f2j_maven_coord)
    # Logging
    # .config("spark.log.level", "DEBUG")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
sc = spark.sparkContext

# define table
DELTA_TABLE_PATH = 's3a://incremental-etl/daily_product_sales'
dt = (
    DeltaTable
    .createIfNotExists(spark)
    .addColumn("daily_sales_sk", dataType=LongType(), generatedAlwaysAs=IdentityGenerator())
    .addColumn("date_sk", dataType=DateType())
    .addColumn("product_id", dataType=IntegerType())
    .addColumn("sold_qty", dataType=DoubleType())
    .addColumn("record_track_hash", dataType=IntegerType())
    .addColumn("record_commit_timestamp", dataType=TimestampType()) 
    .property('delta.enableChangeDataFeed', 'true')
    .location(DELTA_TABLE_PATH)
    .execute()
)

# read data
read_options = {
    'readChangeFeed': 'true',
}

last_commit_timestamp = spark.sql(
    f"select max(_commit_timestamp) from table_changes_by_path('{DELTA_TABLE_PATH}', 0)"
).collect()[0][0]
if last_commit_timestamp:
    read_options['startingTimestamp'] = str(last_commit_timestamp)
else:
    # for first time
    read_options['startingVersion'] = 0
print(f"Incremental query parameters: {read_options}")   

FACT_SALE_ORDER_LINE_DELTA_PATH = 's3a://incremental-etl/fact_order_line'
fact_change_df = (
    spark
    .read.format("delta")
    .options(**read_options)
    .load(FACT_SALE_ORDER_LINE_DELTA_PATH)
)

fact_change_df = (
    fact_change_df
    .filter(col("_change_type").isin(["insert", "update_postimage", "delete"]))
)

DIM_ORDER_DELTA_PATH = "s3a://incremental-etl/dim_order"
dim_order_df = (
    spark
    .read.format("delta")
    .load(DIM_ORDER_DELTA_PATH)
)

# find affected dates from changed data
affected_date_df = (
    fact_change_df
    .join(
        dim_order_df, dim_order_df.order_sk == fact_change_df.order_sk
    )
    .select(
        fact_change_df.line_product_id,
        to_date(dim_order_df.order_date).alias("order_date"),
        fact_change_df._commit_timestamp
    )
)
affected_date_df.show()

# re-run for affected date range
fact_order_line_df = (
    spark
    .read.format("delta")
    .load(FACT_SALE_ORDER_LINE_DELTA_PATH)
)

transform_df = (
    fact_order_line_df
    .filter(
        fact_order_line_df.record_active_flag == True
    )
    .join(
        dim_order_df,
        fact_order_line_df.order_sk == dim_order_df.order_sk
    )    
    .join(
        affected_date_df,
        [
            fact_order_line_df.line_product_id == affected_date_df.line_product_id,
            to_date(dim_order_df.order_date) == affected_date_df.order_date
        ]
    )
    .select(
        fact_order_line_df.line_product_id,
        to_date(dim_order_df.order_date).alias("order_date"),
        fact_order_line_df.line_product_uom_qty,
        affected_date_df._commit_timestamp
    )
)
transform_df.show()

agg_df = (
    transform_df
    .groupBy(["line_product_id", "order_date"])
    .agg(
        sum("line_product_uom_qty").alias("sold_qty"),
        max("_commit_timestamp").alias("record_commit_timestamp")
    )
    .withColumn("record_track_hash", hash(
        "order_date", "line_product_id", "sold_qty"
    ))
)
agg_df.show()

# merge updated result into current table
(
    dt.alias("target")
    .merge(
        agg_df.alias("source"),
        "target.product_id = source.line_product_id and target.date_sk = source.order_date"
    )
    .whenMatchedUpdate(
        condition="target.record_track_hash != source.record_track_hash",
        set={
            "target.sold_qty": "source.sold_qty",
            "target.record_track_hash": "source.record_track_hash",
            "target.record_commit_timestamp": "source.record_commit_timestamp"
        }
    )
    .whenNotMatchedInsert(
        values={
            "target.date_sk": "source.order_date",
            "target.product_id": "source.line_product_id",
            "target.sold_qty": "source.sold_qty",
            "target.record_track_hash": "source.record_track_hash",
            "target.record_commit_timestamp": "source.record_commit_timestamp"
        }
    )
    .execute()
)