import pyspark

from delta import *

builder = (
    pyspark
    .sql.SparkSession.builder
    .appName("Landing zone to stage layer")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()

# get arguments
table_name = spark.conf.get("spark.app.table_name")

# read data from HDFS
hdfs_path = f"hdfs://hadoop:9000/user/hadoopuser/landingzone/tb_{table_name}"
df = spark.read.parquet(hdfs_path)
df.show(10)

# create a table
output_path = f"hdfs://hadoop:9000/user/hadoopuser/stage/delta_{table_name}_raw"
(
    df
    .write
    .format("delta")
    .mode("overwrite")
    .save(output_path)
)