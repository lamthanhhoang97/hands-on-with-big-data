
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
    lineitem_df
    .filter(F.col("l_shipmode").isin(["MAIL", "SHIP"]))
    .filter(F.col("l_commitdate") < F.col("l_receiptdate"))
    .filter(F.col("l_shipdate") < F.col("l_commitdate"))
    .filter(F.col("l_receiptdate") >= F.lit("1994-01-01"))
    .filter(F.col("l_receiptdate") < F.date_add(F.lit("1994-01-01"), days=365)) # 1 year
    .join(
        order_df,
        lineitem_df.l_orderkey == order_df.o_orderkey
    )
    .groupBy("l_shipmode")
    .agg(
        F.sum(
            F.when(
                (F.col("o_orderpriority") == "1-URGENT") | (F.col("o_orderpriority") == "2-HIGH"), 1).otherwise(0)
        ).alias("high_line_count"),
        F.sum(
            F.when(
                (F.col("o_orderpriority") != "1-URGENT") & (F.col("o_orderpriority") != "2-HIGH"), 1).otherwise(0)
        ).alias("low_line_count")
    )
    .sort(
        F.asc("l_shipmode")
    )
)

result_df.show(10, truncate=False)

sql_query = """
    SELECT
        l_shipmode,
        sum(CASE
                WHEN o_orderpriority = '1-URGENT'
                    OR o_orderpriority = '2-HIGH'
                THEN 1
                ELSE 0
            END) AS high_line_count,
        sum(CASE
            WHEN o_orderpriority <> '1-URGENT'
                    AND o_orderpriority <> '2-HIGH'
                THEN 1
            ELSE 0
            END) AS low_line_count
    FROM
        orders,
        lineitem
    WHERE
        o_orderkey = l_orderkey
        AND l_shipmode in ('MAIL', 'SHIP')
        AND l_commitdate < l_receiptdate
        AND l_shipdate < l_commitdate
        AND l_receiptdate >= DATE '1994-01-01'
        AND l_receiptdate < DATE '1994-01-01' + INTERVAL '1' year
    GROUP BY
        l_shipmode
    ORDER BY
        l_shipmode;
"""

# 1.DATE = 1994-01-01;
# 2.DISCOUNT = 0.06;
# 3.QUANTITY = 24.

spark.stop()
sc._gateway.jvm.System.exit(0)

