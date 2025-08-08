import pyspark
import os

from delta import *



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
    # Logging
    # .config("spark.log.level", "DEBUG")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

sc = spark.sparkContext

# get arguments
table_name = spark.conf.get("spark.app.table_name")

print(f"Current director: {os.getcwd()}")

# read data
# /opt/spark/work-dir/tpch/customer/...
file_path_pattern = f"/opt/spark/work-dir/tpch/{table_name}/{table_name}.tbl.*"
print(file_path_pattern)
df = spark.read.csv(
    file_path_pattern,
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
print(f"Number of partitions: {df.rdd.getNumPartitions()}")
    
# load data
output_path = f"hdfs://hadoop:9000//user/hadoopuser/raw/300GB/raw_{table_name}"

# write as Delta format
(
    df
    .write
    .format("delta")
    .mode("overwrite")
    .option("optimizeWrite", "true")
    .save(output_path)
)

spark.stop()
#NOTE: some threads are created by user code (Python) block jvm to exit
# https://kb.databricks.com/clusters/databricks-spark-submit-jobs-appear-to-%E2%80%9Chang%E2%80%9D-and-clusters-do-not-auto-terminate-
# long-term fix: https://issues.apache.org/jira/browse/SPARK-48547
sc._gateway.jvm.System.exit(0)