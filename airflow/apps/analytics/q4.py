
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

order_df = (
    spark.read
    .format("delta")
    .load("hdfs://hadoop:9000/user/hadoopuser/raw/300GB/raw_orders")
)

result_df = (
    order_df
    .filter(F.col("o_orderdate") >= F.lit("1993-07-01"))
    .filter(F.col("o_orderdate") < F.date_add(F.lit("1993-07-01"), days=90)) # 3 months
    .join(
        lineitem_df
        .filter(
            F.col("l_commitdate") < F.col("l_receiptdate")
        ),
        lineitem_df.l_orderkey == order_df.o_orderkey
    )
    .groupBy("o_orderpriority")
    .agg(
        F.count_distinct("o_orderkey").alias("order_count")
    )
    .sort(F.asc("o_orderpriority"))
)

result_df.show(10, truncate=False)

sql_query = """
    select
        o_orderpriority,
        count(*) as order_count
    from
        orders
    where
        o_orderdate >= date ':1'
        and o_orderdate < date ':1' + interval '3' month
        and exists (
            select
                *
            from
                lineitem
            where
                l_orderkey = o_orderkey
                and l_commitdate < l_receiptdate
        )
    group by
        o_orderpriority
    order by
        o_orderpriority;
"""

# 1.DATE = 1993-07-01

spark.stop()
sc._gateway.jvm.System.exit(0)

