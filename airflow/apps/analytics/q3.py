
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

customer_df = (
    spark.read
    .format("delta")
    .load("hdfs://hadoop:9000/user/hadoopuser/raw/300GB/raw_customer")
)

order_df = (
    spark.read
    .format("delta")
    .load("hdfs://hadoop:9000/user/hadoopuser/raw/300GB/raw_orders")
)

result_df = (
    lineitem_df
    .filter(F.col("l_shipdate") > F.lit("1995-03-15"))
    .join(
        order_df
        .filter(F.col("o_orderdate") < F.lit("1995-03-15")),
        lineitem_df.l_orderkey == order_df.o_orderkey
    )
    .join(
        customer_df
        .filter(F.col("c_mktsegment") == "BUILDING"),
        order_df.o_custkey == customer_df.c_custkey
    )
    .groupBy([
        "l_orderkey",
        "o_orderdate",
        "o_shippriority"
    ])
    .agg(
        F.sum(F.col("l_extendedprice") * (1 - F.col("l_discount"))).alias("revenue")
    )
    .select(
        "l_orderkey",
        "revenue",
        "o_orderdate",
        "o_shippriority"
    )
    .sort(
        F.desc("revenue"),
        F.asc("o_orderdate")
    )
)

result_df.show(10)

sql_query = """
    select
        l_orderkey,
        sum(l_extendedprice * (1 - l_discount)) as revenue,
        o_orderdate,
        o_shippriority
    from
        customer,
        orders,
        lineitem
    where
        c_mktsegment = ':1'
        and c_custkey = o_custkey
        and l_orderkey = o_orderkey
        and o_orderdate < date ':2'
        and l_shipdate > date ':2'
    group by
        l_orderkey,
        o_orderdate,
        o_shippriority
    order by
        revenue desc,
        o_orderdate;
"""

# 1.SEGMENT = BUILDING;
# 2.DATE = 1995-03-15.

spark.stop()
sc._gateway.jvm.System.exit(0)

