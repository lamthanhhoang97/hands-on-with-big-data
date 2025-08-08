import pyspark
import os

from delta import *
from delta.tables import DeltaTable
from pyspark.sql import SparkSession

builder = (
    pyspark
    .sql.SparkSession.builder
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    # .config("spark.jars.packages", ",".join(packages))
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

# raw_lineitem

# # enable liquid clustering
# spark.sql(f"""
#     ALTER TABLE delta.`hdfs://hadoop:9000/user/hadoopuser/raw/raw_lineitem`
#     CLUSTER BY (l_linenumber, l_orderkey, l_partkey, l_suppkey);
# """)

# # recluster entire table
# spark.sql(f"""
#     OPTIMIZE delta.`hdfs://hadoop:9000/user/hadoopuser/raw/raw_lineitem` FULL;
# """)

# dim_order

# # enable liquid clustering
# spark.sql(f"""
#     ALTER TABLE delta.`s3a://data-warehouse/dim_order`
#     CLUSTER BY (sk_orderkey, o_orderkey);
# """)

# # recluster entire table
# spark.sql(f"""
#     OPTIMIZE delta.`s3a://data-warehouse/dim_order` FULL;
# """)

# dim_part

# # enable liquid clustering
# spark.sql(f"""
#     ALTER TABLE delta.`s3a://data-warehouse/dim_part`
#     CLUSTER BY (sk_partkey, p_partkey);
# """)

# # recluster entire table
# spark.sql(f"""
#     OPTIMIZE delta.`s3a://data-warehouse/dim_part` FULL;
# """)

# bridge_order_customer
# # enable liquid clustering
# spark.sql(f"""
#     ALTER TABLE delta.`s3a://data-warehouse/bridge_order_customer`
#     CLUSTER BY (sk_orderkey, o_orderkey, sk_custkey, c_custkey);
# """)

# # recluster entire table
# spark.sql(f"""
#     OPTIMIZE delta.`s3a://data-warehouse/bridge_order_customer` FULL;
# """)

# # bridge_part_supp
# # enable liquid clustering
# spark.sql(f"""
#     ALTER TABLE delta.`s3a://data-warehouse/bridge_part_supp`
#     CLUSTER BY (sk_partkey, p_partkey, sk_suppkey, s_suppkey);
# """)

# # recluster entire table
# spark.sql(f"""
#     OPTIMIZE delta.`s3a://data-warehouse/bridge_part_supp` FULL;
# """)

# fact_line_item
# enable liquid clustering
# spark.sql(f"""
#     ALTER TABLE delta.`s3a://data-warehouse/fact_line_item`
#     CLUSTER BY (l_orderkey, l_linenumber);
# """)

# # recluster entire table
# spark.sql(f"""
#     OPTIMIZE delta.`s3a://data-warehouse/fact_line_item` FULL;
# """)

spark.stop()
sc._gateway.jvm.System.exit(0)
