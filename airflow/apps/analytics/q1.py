
import pyspark

from delta import *

from delta.tables import DeltaTable, IdentityGenerator
from pyspark.sql.types import *
from pyspark.sql import functions as F


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


# read data
lineitem_df = (
    spark.read
    .format("delta")
    .load("hdfs://hadoop:9000/user/hadoopuser/raw/300GB/raw_lineitem")
)

result_df = (
    lineitem_df
    .filter(F.col("l_shipdate") <= F.date_sub(F.lit("1998-12-01"), days=90))
    .groupBy(["l_returnflag", "l_linestatus"])
    .agg(
        F.sum("l_quantity").alias("sum_qty"),
        F.sum("l_extendedprice").alias("sum_base_price"),
        F.sum(F.col("l_extendedprice") * (1 - F.col("l_discount"))).alias("sum_disc_price"),
        F.sum(F.col("l_extendedprice") * (1 - F.col("l_discount")) * (1 + F.col("l_tax"))).alias("sum_charge"),
        F.avg("l_quantity").alias("avg_qty"),
        F.avg("l_extendedprice").alias("avg_price"),
        F.avg("l_discount").alias("avg_disc"),
        F.count("*").alias("count_order")
    )
    .sort(
        F.asc("l_returnflag"), 
        F.asc("l_linestatus")
    )
)

result_df.show(10)

spark.stop()
sc._gateway.jvm.System.exit(0)

