import os

from pyspark.sql import SparkSession
from pyspark.sql.types import *

if __name__ == "__main__":
    spark = (
        SparkSession
        .builder
        .appName("TPCH_to_Parquet")
        .config("spark.default.parallelism", 2)
        .config("spark.driver.memory", "8g")
        .config("spark.executor.memory", "8g")
        .getOrCreate()
    )

    # Define schema (Crucial!)
    order_schema = StructType([
        StructField("o_orderkey", IntegerType(), False),      # Order Key (Primary Key)
        StructField("o_custkey", IntegerType(), False),       # Customer Key (Foreign Key referencing CUSTOMER)
        StructField("o_orderstatus", StringType(), False),    # Order Status (e.g., 'O' for Open, 'C' for Closed)
        StructField("o_totalprice", DoubleType(), False),     # Total Order Price
        StructField("o_orderdate", DateType(), False),       # Order Date
        StructField("o_orderpriority", StringType(), False),  # Order Priority (e.g., '1-URGENT', '2-HIGH')
        StructField("o_clerk", StringType(), False),          # Clerk Name
        StructField("o_shippriority", IntegerType(), False),   # Shipping Priority
        StructField("o_comment", StringType(), True),           # Order Comment (Can be NULL)
        StructField("o_dummy", StringType(), True) # Dummy column
    ])

    lineitem_schema = StructType([
        StructField("l_orderkey", IntegerType(), nullable=False),
        StructField("l_partkey", IntegerType(), nullable=False),
        StructField("l_suppkey", IntegerType(), nullable=False),
        StructField("l_linenumber", IntegerType(), nullable=False),
        StructField("l_quantity", DecimalType(12, 2), nullable=False),  # Use DecimalType for precision
        StructField("l_extendedprice", DecimalType(12, 2), nullable=False), # Use DecimalType for precision
        StructField("l_discount", DecimalType(12, 2), nullable=False), # Use DecimalType for precision
        StructField("l_tax", DecimalType(12, 2), nullable=False), # Use DecimalType for precision
        StructField("l_returnflag", StringType(), nullable=False),
        StructField("l_linestatus", StringType(), nullable=False),
        StructField("l_shipdate", DateType(), nullable=False),
        StructField("l_commitdate", DateType(), nullable=False),
        StructField("l_receiptdate", DateType(), nullable=False),
        StructField("l_shipinstruct", StringType(), nullable=False),
        StructField("l_shipmode", StringType(), nullable=False),
        StructField("l_comment", StringType(), nullable=False),
        StructField("o_dummy", StringType(), True) # Dummy column
    ])

    # Read the data (adjust path if needed.  /data is mounted from your host)
    df = spark.read.csv(
        "work/tpch/lineitem.tbl", 
        sep="|", 
        header=False, 
        schema=lineitem_schema
    ) # Or whatever your data file is called

    # Repartition (Optional, but often beneficial)
    df = df.repartition(50) # Example: 100 partitions

    # Write to Parquet (adjust path as needed. /data is mounted from your host)
    df.write.mode("overwrite").parquet(
        "/data/tpch-parquet/lineitem/"
    )

    spark.stop()