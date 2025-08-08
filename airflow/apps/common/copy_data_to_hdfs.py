# import os
# import logging
import sys

from pyspark.sql import SparkSession

# Initialize SparkSession
spark = (
    SparkSession
    .builder
    .appName("TPC-H dataset to HDFS")
    # .config("spark.log.level", "DEBUG")
    .getOrCreate()
)

# get arguments
table_name = spark.conf.get("spark.app.table_name")

# read data
hdfs_path = f'hdfs://hadoop:9000/user/hadoopuser/raw/100GB/{table_name}.tbl'
df = spark.read.csv(
    hdfs_path,
    sep="|",
    header=False,
    inferSchema=True
)

# rename the columns
table_name_2_cols = {
    'nation': ['n_nationkey', 'n_name', 'n_regionkey', 'n_comment', 'n_dummy'],
    'region': ['r_regionkey', 'r_name', 'r_comment', 'r_dummy'],
    'supplier': ['s_suppkey', 's_name', 's_address', 's_nationkey', 's_phone', 's_acctbal', 's_comment', 's_dummy'],
    'customer': ['c_custkey', 'c_name', 'c_address', 'c_nationkey', 'c_phone', 'c_acctbal', 'c_mktsegment', 'c_comment', 'c_dummy'],
    'part': ['p_partkey', 'p_name', 'p_mfgr', 'p_brand', 'p_type', 'p_size', 'p_container', 'p_retailprice', 'p_comment', 'p_dummy'],
    'partsupp': ['ps_partkey', 'ps_suppkey', 'ps_availqty', 'ps_supplycost', 'ps_comment', 'ps_dummy'],
    'orders': ['o_orderkey', 'o_custkey', 'o_orderstatus', 'o_totalprice', 'o_orderdate', 'o_orderpriority', 'o_clerk', 'o_shippriority', 'o_comment', 'o_dummy'],
    'lineitem': ['l_orderkey', 'l_partkey', 'l_suppkey', 'l_linenumber', 'l_quantity', 'l_extendedprice', 'l_discount', 'l_tax', 'l_returnflag', 'l_linestatus', 'l_shipdate', 'l_commitdate', 'l_receiptdate', 'l_shipinstruct', 'l_shipmode', 'l_comment', 'l_dummy']
}
new_columns = table_name_2_cols.get(table_name, [])
df = df.toDF(*new_columns)

# drop the dummy column
df = df.drop(new_columns[-1])

df.printSchema()

df.show(10)

# write data to HDFS
output_path = f"hdfs://hadoop:9000/user/hadoopuser/landingzone/tb_{table_name}"
(
    df
    .repartition(20)
    .write
    .mode("overwrite")
    .parquet(
        output_path, 
        compression="snappy"
    )
)