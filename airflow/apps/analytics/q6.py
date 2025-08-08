
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
    .filter(
        F.col("l_shipdate") >= F.lit("1994-01-01")
    )
    .filter(
        F.col("l_shipdate") < F.date_add(F.lit("1994-01-01"), days=365) # 1 year
    )
    .filter(
        F.col("l_discount") >= (0.06 - 0.01)
    )
    .filter(
        F.col("l_discount") <= (0.06 + 0.01)
    )
    .filter(
        F.col("l_quantity") < 24
    )
    .agg(
        F.sum(F.col("l_extendedprice") * F.col("l_discount")).alias("revenue")
    )
)

result_df.show(10, truncate=False)

sql_query = """
    select
        sum(l_extendedprice*l_discount) as revenue
    from
        lineitem
    where
        l_shipdate >= date '[DATE]'
        and l_shipdate < date '[DATE]' + interval '1' year
        and l_discount between [DISCOUNT] - 0.01 and [DISCOUNT] + 0.01
        and l_quantity < [QUANTITY];
"""

# 1.DATE = 1994-01-01;
# 2.DISCOUNT = 0.06;
# 3.QUANTITY = 24.

spark.stop()
sc._gateway.jvm.System.exit(0)

